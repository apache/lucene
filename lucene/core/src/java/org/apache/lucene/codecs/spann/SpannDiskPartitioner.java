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
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
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

  private static final long TARGET_BUCKET_BYTES = 32 * 1024 * 1024; // 32MB per bucket
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
    long vectorDataSize = (long) Integer.BYTES + vectorByteWidth; // DocID + Vector
    if (vectorData.length() % vectorDataSize != 0) {
      throw new IllegalStateException(
          "Vector data file "
              + vectorData
              + " has length "
              + vectorData.length()
              + " which is not divisible by record size "
              + vectorDataSize);
    }

    ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
    int totalAssignments = 0;

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
        int start = 0;
        while (start < numRecords) {
          int end = start + 1;
          int partitionId = partitionIds[start];
          while (end < numRecords && partitionIds[end] == partitionId) {
            end++;
          }

          // Found a run for partitionId: [start, end)
          int count = end - start;
          long partitionStart = spadOut.getFilePointer();

          // Hybrid Buffering: Use RAM for small runs to avoid FS churn, Disk for large runs.
          long totalVectorBytes = (long) count * vectorByteWidth;
          boolean useHeap = totalVectorBytes <= 1024 * 1024; // 1MB threshold

          if (useHeap) {
            byte[] heapBuffer = new byte[(int) totalVectorBytes];
            int bufferOffset = 0;
            for (int k = start; k < end; k++) {
              int sourceOrdinal = docIds[k];
              long offset = (long) sourceOrdinal * vectorDataSize;
              vectorData.seek(offset);
              int realDocId = vectorData.readInt();
              spadOut.writeInt(realDocId);

              vectorData.readBytes(vectorScratch, 0, vectorByteWidth);
              System.arraycopy(vectorScratch, 0, heapBuffer, bufferOffset, vectorByteWidth);
              bufferOffset += vectorByteWidth;
            }
            spadOut.writeBytes(heapBuffer, heapBuffer.length);
          } else {
            // Large run: Use Temp File (Fixes Double-Seek and OOM, accepts some churn)
            String vectorTempName = null;
            try {
              try (IndexOutput vecTempOut =
                  directory.createTempOutput(
                      tempFileNamePrefix + "_vectors", "spann_vec_temp", IOContext.DEFAULT)) {
                vectorTempName = vecTempOut.getName();
                for (int k = start; k < end; k++) {
                  int sourceOrdinal = docIds[k];
                  long offset = (long) sourceOrdinal * vectorDataSize;
                  vectorData.seek(offset);
                  int realDocId = vectorData.readInt();
                  spadOut.writeInt(realDocId);

                  vectorData.readBytes(vectorScratch, 0, vectorByteWidth);
                  vecTempOut.writeBytes(vectorScratch, 0, vectorByteWidth);
                }
                CodecUtil.writeFooter(vecTempOut);
              }

              // vecTempOut is now closed, safe to read
              try (IndexInput vecTempIn = directory.openInput(vectorTempName, IOContext.READONCE)) {
                long vecLength = vecTempIn.length() - CodecUtil.footerLength();
                spadOut.copyBytes(vecTempIn, vecLength);
              }
            } finally {
              if (vectorTempName != null) {
                IOUtils.deleteFilesIgnoringExceptions(directory, vectorTempName);
              }
            }
          }

          long partitionLen = spadOut.getFilePointer() - partitionStart;

          // Write Metadata
          metaBuffer.writeVInt(partitionId);
          metaBuffer.writeVLong(partitionStart);
          metaBuffer.writeVLong(partitionLen);

          totalAssignments += count;
          start = end;
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

  @Override
  public void close() throws IOException {
    // Safety close in case finish wasn't called or failed
    IOUtils.close(bucketOutputs);
    IOUtils.deleteFilesIgnoringExceptions(directory, tempFiles);
  }
}
