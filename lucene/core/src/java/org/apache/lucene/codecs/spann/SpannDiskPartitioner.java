/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.spann;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * Helper class to buffer and sort assignments for SPANN.
 *
 * <p>Uses "Modulo Bucketing" to minimize temporary disk usage. Instead of sorting the full vector
 * payload, it buckets (PartitionID, DocID) pointers into temporary files. During flush, it sorts
 * the pointers in RAM and retrieves the vector data from the original temp file via random access.
 */
final class SpannDiskPartitioner implements Closeable {

  private static final long TARGET_BUCKET_BYTES = 256 * 1024 * 1024; // 256MB RAM per bucket
  private static final int RECORD_BYTES = Integer.BYTES * 2; // PartitionID + DocID

  private final Directory directory;
  private final String segmentName;
  private final String tempFileNamePrefix;
  private final int numBuckets;
  private final IndexOutput[] bucketOutputs;
  private final List<String> tempFiles = new ArrayList<>();

  SpannDiskPartitioner(
      Directory directory,
      SegmentInfo segmentInfo,
      String fieldName,
      IOContext context,
      long totalAssignments)
      throws IOException {
    this.directory = directory;
    this.segmentName = segmentInfo.name;
    this.tempFileNamePrefix = segmentName + "_" + fieldName + "_bucket_";

    // Calculate number of buckets needed to keep each bucket within RAM limit
    long totalBytes = totalAssignments * RECORD_BYTES;
    int needed = (int) Math.ceil((double) totalBytes / TARGET_BUCKET_BYTES);
    this.numBuckets = Math.max(1, needed);

    this.bucketOutputs = new IndexOutput[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      IndexOutput out = directory.createTempOutput(tempFileNamePrefix + i, "spann_bucket", context);
      tempFiles.add(out.getName());
      bucketOutputs[i] = out;
    }
  }

  /**
   * Adds an assignment to the appropriate bucket.
   *
   * @param partitionId the partition ID to assign to
   * @param docId the document ID
   */
  void addAssignment(int partitionId, int docId) throws IOException {
    int bucketId = partitionId % numBuckets;
    bucketOutputs[bucketId].writeInt(partitionId);
    bucketOutputs[bucketId].writeInt(docId);
  }

  /**
   * Finishes processing, sorts buckets, and writes final .spad and .spam files.
   *
   * @param vectorData input stream to read vectors from. MUST be able to seek.
   * @param spadOut output for vector data
   * @param spamOut output for metadata
   * @param vectorEncoding the encoding of the vectors
   * @param dim the dimension of the vectors
   */
  void finish(
      IndexInput vectorData,
      IndexOutput spadOut,
      IndexOutput spamOut,
      VectorEncoding vectorEncoding,
      int dim)
      throws IOException {
    // Close all buckets first to ensure data is flushed to disk
    for (IndexOutput out : bucketOutputs) {
      out.close();
    }

    boolean isByte = vectorEncoding == VectorEncoding.BYTE;
    int vectorByteWidth = isByte ? dim : dim * Float.BYTES;
    int vectorDataSize = Integer.BYTES + vectorByteWidth; // DocID + Vector

    ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
    int totalAssignments = 0;

    ByteBuffersDataOutput docIdBuffer = new ByteBuffersDataOutput();
    ByteBuffersDataOutput vectorBuffer = new ByteBuffersDataOutput();
    byte[] vectorScratch = new byte[vectorByteWidth];

    // Process each bucket
    for (int i = 0; i < numBuckets; i++) {
      String fileName = tempFiles.get(i);
      try (IndexInput bucketIn = directory.openInput(fileName, IOContext.READONCE)) {
        long length = bucketIn.length();
        int numRecords = (int) (length / RECORD_BYTES);
        if (numRecords == 0) continue;

        int[] partitionIds = new int[numRecords];
        int[] docIds = new int[numRecords];

        for (int j = 0; j < numRecords; j++) {
          partitionIds[j] = bucketIn.readInt();
          docIds[j] = bucketIn.readInt();
        }

        // Sort this bucket by partitionId
        new InPlaceMergeSorter() {
          @Override
          protected void swap(int i, int j) {
            int tmpP = partitionIds[i];
            partitionIds[i] = partitionIds[j];
            partitionIds[j] = tmpP;

            int tmpD = docIds[i];
            docIds[i] = docIds[j];
            docIds[j] = tmpD;
          }

          @Override
          protected int compare(int i, int j) {
            int cmp = Integer.compare(partitionIds[i], partitionIds[j]);
            if (cmp != 0) return cmp;
            return Integer.compare(docIds[i], docIds[j]);
          }
        }.sort(0, numRecords);

        // Iterate sorted records and write to final output
        int currentPartition = -1;
        for (int j = 0; j < numRecords; j++) {
          int pId = partitionIds[j];
          int sourceOrdinal = docIds[j]; // We stored the ordinal in the bucket's "docId" slot

          if (pId != currentPartition) {
            if (currentPartition != -1) {
              writePartition(
                  spadOut, metaBuffer, currentPartition, docIdBuffer, vectorBuffer, vectorByteWidth);
            }
            currentPartition = pId;
          }

          // Random Access Read
          long offset = (long) sourceOrdinal * vectorDataSize;
          vectorData.seek(offset);
          int realDocId = vectorData.readInt();
          vectorData.readBytes(vectorScratch, 0, vectorByteWidth);

          docIdBuffer.writeInt(realDocId);
          vectorBuffer.writeBytes(vectorScratch, 0, vectorByteWidth);
          totalAssignments++;
        }

        if (currentPartition != -1) {
          writePartition(
              spadOut, metaBuffer, currentPartition, docIdBuffer, vectorBuffer, vectorByteWidth);
        }
      } finally {
        // Delete bucket immediately to free space
        directory.deleteFile(fileName);
      }
    }

    CodecUtil.writeFooter(spadOut);
    
    // Write final metadata
    spamOut.writeVInt(totalAssignments);
    metaBuffer.copyTo(spamOut);
    CodecUtil.writeFooter(spamOut);
  }

  private void writePartition(
      IndexOutput dataOut,
      ByteBuffersDataOutput metaBuffer,
      int partitionId,
      ByteBuffersDataOutput docIdBuffer,
      ByteBuffersDataOutput vectorBuffer,
      int vectorByteWidth)
      throws IOException {

    long partitionStart = dataOut.getFilePointer();
    docIdBuffer.copyTo(dataOut);
    vectorBuffer.copyTo(dataOut);

    metaBuffer.writeVInt(partitionId);
    metaBuffer.writeVLong(partitionStart);
    metaBuffer.writeVLong(dataOut.getFilePointer() - partitionStart);

    docIdBuffer.reset();
    vectorBuffer.reset();
  }

  @Override
  public void close() throws IOException {
    // Safety close in case finish wasn't called or failed
    // Safety close in case finish wasn't called or failed
    IOUtils.close(bucketOutputs);
    IOUtils.deleteFilesIgnoringExceptions(directory, tempFiles);
  }
}
