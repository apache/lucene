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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;

/**
 * Writes vectors in the SPANN (HNSW-IVF) format.
 *
 * <p>Centroids are computed via K-Means and indexed into an HNSW-based coarse quantizer. Vector
 * data is assigned to the nearest centroid and written sequentially in a clustered format.
 */
public class Lucene99SpannVectorsWriter extends KnnVectorsWriter {

  private static final int KMEANS_MAX_ITERS = 20;

  private final SegmentWriteState state;
  private final KnnVectorsWriter centroidDelegate;
  private final Map<String, SpannFieldVectorsWriter> fieldWriters = new HashMap<>();
  private final int maxPartitions;
  private final int clusteringSampleSize;

  public Lucene99SpannVectorsWriter(
      SegmentWriteState state,
      KnnVectorsFormat centroidFormat,
      int maxPartitions,
      int clusteringSampleSize)
      throws IOException {
    this(state, centroidFormat, maxPartitions, clusteringSampleSize, 1);
  }

  public Lucene99SpannVectorsWriter(
      SegmentWriteState state,
      KnnVectorsFormat centroidFormat,
      int maxPartitions,
      int clusteringSampleSize,
      int replicationFactor)
      throws IOException {
    this.state = state;
    this.centroidDelegate = centroidFormat.fieldsWriter(state);
    this.maxPartitions = maxPartitions;
    this.clusteringSampleSize = clusteringSampleSize;
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    SpannFieldVectorsWriter writer = new SpannFieldVectorsWriter(fieldInfo, state);
    fieldWriters.put(fieldInfo.name, writer);

    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      return new KnnFieldVectorsWriter<byte[]>() {
        @Override
        public void addValue(int docID, byte[] vectorValue) throws IOException {
          float[] floats = new float[vectorValue.length];
          for (int i = 0; i < vectorValue.length; i++) {
            floats[i] = (float) vectorValue[i];
          }
          writer.addValue(docID, floats);
        }

        @Override
        public byte[] copyValue(byte[] vectorValue) {
          return vectorValue.clone();
        }

        @Override
        public long ramBytesUsed() {
          return 0; // The delegate writer tracks usage
        }
      };
    }

    return writer;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    // Data is buffered in addValue and persisted during finish()
  }

  @Override
  public void finish() throws IOException {
    int maxCentroidDoc = 0;
    for (Map.Entry<String, SpannFieldVectorsWriter> entry : fieldWriters.entrySet()) {
      String fieldName = entry.getKey();
      SpannFieldVectorsWriter writer = entry.getValue();
      FieldInfo fieldInfo = writer.getFieldInfo();

      if (writer.getCount() == 0) {
        continue;
      }

      // Optimized path for small segments to avoid clustering overhead.
      if (writer.getCount() < 4096) {
        float[][] vectorArray = new float[writer.getCount()][fieldInfo.getVectorDimension()];
        int[] docIds = new int[writer.getCount()];
        try (IndexInput input = writer.openInput()) {
          for (int i = 0; i < writer.getCount(); i++) {
            docIds[i] = input.readInt();
            if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
              for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
                vectorArray[i][d] = (float) input.readByte();
              }
            } else {
              for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
                vectorArray[i][d] = Float.intBitsToFloat(input.readInt());
              }
            }
          }
        }
        writer.close(); // Delete temp file

        // Compute mean for the single representative centroid
        float[] mean = new float[fieldInfo.getVectorDimension()];
        for (float[] v : vectorArray) {
          for (int d = 0; d < mean.length; d++) {
            mean[d] += v[d];
          }
        }
        float scale = 1.0f / vectorArray.length;
        for (int d = 0; d < mean.length; d++) {
          mean[d] *= scale;
        }

        // Write the single centroid
        if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
          @SuppressWarnings("unchecked")
          KnnFieldVectorsWriter<byte[]> byteCentroidWriter =
              (KnnFieldVectorsWriter<byte[]>) centroidDelegate.addField(fieldInfo);
          byte[] byteCentroid = new byte[mean.length];
          for (int k = 0; k < mean.length; k++) {
            byteCentroid[k] = (byte) mean[k];
          }
          byteCentroidWriter.addValue(0, byteCentroid);
        } else {
          @SuppressWarnings("unchecked")
          KnnFieldVectorsWriter<float[]> floatCentroidWriter =
              (KnnFieldVectorsWriter<float[]>) centroidDelegate.addField(fieldInfo);
          floatCentroidWriter.addValue(0, mean);
        }
        maxCentroidDoc = Math.max(maxCentroidDoc, 1);

        // Write directly to .spad (no need for list of lists)
        writeSinglePartition(fieldName, fieldInfo, vectorArray, docIds);
        continue;
      }

      // Sample training vectors via reservoir sampling to mitigate bias in sorted
      // segments.
      int numPartitions = maxPartitions;
      if (numPartitions == -1) {
        numPartitions = (int) Math.sqrt(writer.getCount());
      }
      numPartitions = Math.min(writer.getCount(), Math.max(1, numPartitions));

      int sampleSize = Math.min(clusteringSampleSize, writer.getCount());
      float[][] trainingVectors = new float[sampleSize][fieldInfo.getVectorDimension()];
      java.util.Random random = new java.util.Random(42);
      try (IndexInput input = writer.openInput()) {
        boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
        for (int i = 0; i < writer.getCount(); i++) {
          input.readInt(); // Consume docID
          float[] currentVector = new float[fieldInfo.getVectorDimension()];
          if (isByte) {
            for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
              currentVector[d] = (float) input.readByte();
            }
          } else {
            for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
              currentVector[d] = Float.intBitsToFloat(input.readInt());
            }
          }

          if (i < sampleSize) {
            trainingVectors[i] = currentVector;
          } else {
            int j = random.nextInt(i + 1);
            if (j < sampleSize) {
              trainingVectors[j] = currentVector;
            }
          }
        }
      }

      float[][] centroids =
          SpannKMeans.cluster(
              trainingVectors,
              numPartitions,
              fieldInfo.getVectorSimilarityFunction(),
              KMEANS_MAX_ITERS);
      maxCentroidDoc = Math.max(maxCentroidDoc, centroids.length);

      // Write centroids to the delegate format (HNSW coarse quantizer)
      if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
        @SuppressWarnings("unchecked")
        KnnFieldVectorsWriter<byte[]> byteCentroidWriter =
            (KnnFieldVectorsWriter<byte[]>) centroidDelegate.addField(fieldInfo);
        for (int partitionId = 0; partitionId < centroids.length; partitionId++) {
          byte[] byteCentroid = new byte[centroids[partitionId].length];
          for (int k = 0; k < centroids[partitionId].length; k++) {
            byteCentroid[k] = (byte) centroids[partitionId][k];
          }
          byteCentroidWriter.addValue(partitionId, byteCentroid);
        }
      } else {
        @SuppressWarnings("unchecked")
        KnnFieldVectorsWriter<float[]> floatCentroidWriter =
            (KnnFieldVectorsWriter<float[]>) centroidDelegate.addField(fieldInfo);
        for (int partitionId = 0; partitionId < centroids.length; partitionId++) {
          floatCentroidWriter.addValue(partitionId, centroids[partitionId]);
        }
      }

      // Partition assignment and offline sort.
      OfflineSorter sorter = new OfflineSorter(state.directory, "spann_assign_" + fieldName);

      IndexOutput unsortedOut =
          state.directory.createTempOutput(state.segmentInfo.name, "assign", state.context);
      String unsortedAssignName = unsortedOut.getName();
      try (OfflineSorter.ByteSequencesWriter assignWriter =
          new OfflineSorter.ByteSequencesWriter(unsortedOut)) {
        try (IndexInput input = writer.openInput()) {
          boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
          int vectorDataSize =
              isByte
                  ? fieldInfo.getVectorDimension()
                  : fieldInfo.getVectorDimension() * Float.BYTES;
          byte[] scratch = new byte[Integer.BYTES * 2 + vectorDataSize];
          ByteArrayDataOutput scratchOut = new ByteArrayDataOutput(scratch);
          float[] currentVector = new float[fieldInfo.getVectorDimension()];

          for (int i = 0; i < writer.getCount(); i++) {
            int docId = input.readInt();
            if (isByte) {
              byte[] vectorBytes = new byte[fieldInfo.getVectorDimension()];
              for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
                byte b = input.readByte();
                vectorBytes[d] = b;
                currentVector[d] = (float) b;
              }
              int bestCentroid = 0;
              float maxSim = Float.NEGATIVE_INFINITY;
              for (int j = 0; j < centroids.length; j++) {
                float sim =
                    fieldInfo.getVectorSimilarityFunction().compare(currentVector, centroids[j]);
                if (sim > maxSim) {
                  maxSim = sim;
                  bestCentroid = j;
                }
              }
              scratchOut.reset(scratch);
              scratchOut.writeInt(bestCentroid);
              scratchOut.writeInt(docId);
              scratchOut.writeBytes(vectorBytes, 0, vectorBytes.length);
            } else {
              for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
                currentVector[d] = Float.intBitsToFloat(input.readInt());
              }
              int bestCentroid = 0;
              float maxSim = Float.NEGATIVE_INFINITY;
              for (int j = 0; j < centroids.length; j++) {
                float sim =
                    fieldInfo.getVectorSimilarityFunction().compare(currentVector, centroids[j]);
                if (sim > maxSim) {
                  maxSim = sim;
                  bestCentroid = j;
                }
              }
              scratchOut.reset(scratch);
              scratchOut.writeInt(bestCentroid);
              scratchOut.writeInt(docId);
              for (float v : currentVector) {
                scratchOut.writeInt(Float.floatToIntBits(v));
              }
            }
            assignWriter.write(scratch, 0, scratch.length);
          }
        }
        CodecUtil.writeFooter(unsortedOut);
      }
      writer.close(); // Cleanup temp file

      String sortedAssignName = sorter.sort(unsortedAssignName);
      state.directory.deleteFile(unsortedAssignName);

      writeSortedPartitions(fieldName, fieldInfo, sortedAssignName, numPartitions);
      state.directory.deleteFile(sortedAssignName);
    }
    if (maxCentroidDoc > 0) {
      centroidDelegate.flush(maxCentroidDoc, null);
    }
    centroidDelegate.finish();
  }

  private void writeSinglePartition(
      String fieldName, FieldInfo fieldInfo, float[][] vectors, int[] docIds) throws IOException {

    String dataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldName + ".spad");
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldName + ".spam");

    try (IndexOutput dataOut = state.directory.createOutput(dataFileName, state.context);
        IndexOutput metaOut = state.directory.createOutput(metaFileName, state.context)) {
      CodecUtil.writeIndexHeader(
          dataOut, "Lucene99SpannData", 0, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          metaOut, "Lucene99SpannMeta", 0, state.segmentInfo.getId(), state.segmentSuffix);

      metaOut.writeVInt(vectors.length);

      long startOffset = dataOut.getFilePointer();
      for (int docId : docIds) dataOut.writeInt(docId);
      for (float[] v : vectors) {
        if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
          for (float f : v) dataOut.writeByte((byte) f);
        } else {
          for (float f : v) dataOut.writeInt(Float.floatToIntBits(f));
        }
      }
      long lengthBytes = dataOut.getFilePointer() - startOffset;

      metaOut.writeVInt(0); // Single partition ID
      metaOut.writeVLong(startOffset);
      metaOut.writeVLong(lengthBytes);

      CodecUtil.writeFooter(dataOut);
      CodecUtil.writeFooter(metaOut);
    }
  }

  private void writeSortedPartitions(
      String fieldName, FieldInfo fieldInfo, String sortedFileName, int numPartitions)
      throws IOException {

    String spadFile =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldName + ".spad");
    String spamFile =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldName + ".spam");

    try (org.apache.lucene.util.OfflineSorter.ByteSequencesReader reader =
            new org.apache.lucene.util.OfflineSorter.ByteSequencesReader(
                state.directory.openChecksumInput(sortedFileName), sortedFileName);
        IndexOutput dataOut = state.directory.createOutput(spadFile, state.context);
        IndexOutput metaOut = state.directory.createOutput(spamFile, state.context)) {

      CodecUtil.writeIndexHeader(
          dataOut, "Lucene99SpannData", 0, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          metaOut, "Lucene99SpannMeta", 0, state.segmentInfo.getId(), state.segmentSuffix);

      org.apache.lucene.store.ByteBuffersDataOutput metaBuffer =
          new org.apache.lucene.store.ByteBuffersDataOutput();
      int totalAssignments = 0;

      org.apache.lucene.util.BytesRef scratch;
      org.apache.lucene.store.ByteArrayDataInput readerInput =
          new org.apache.lucene.store.ByteArrayDataInput();

      int currentPartition = -1;
      org.apache.lucene.store.ByteBuffersDataOutput docIdBuffer =
          new org.apache.lucene.store.ByteBuffersDataOutput();
      org.apache.lucene.store.ByteBuffersDataOutput vectorBuffer =
          new org.apache.lucene.store.ByteBuffersDataOutput();

      while ((scratch = reader.next()) != null) {
        readerInput.reset(scratch.bytes, scratch.offset, scratch.length);
        int partitionId = readerInput.readInt();
        int docId = readerInput.readInt();

        if (partitionId != currentPartition) {
          if (currentPartition != -1) {
            writePartition(dataOut, metaBuffer, currentPartition, docIdBuffer, vectorBuffer);
          }
          currentPartition = partitionId;
        }

        totalAssignments++;
        docIdBuffer.writeInt(docId);
        vectorBuffer.writeBytes(
            scratch.bytes, readerInput.getPosition(), scratch.length - readerInput.getPosition());
      }

      if (currentPartition != -1) {
        writePartition(dataOut, metaBuffer, currentPartition, docIdBuffer, vectorBuffer);
      }

      metaOut.writeVInt(totalAssignments);
      metaBuffer.copyTo(metaOut);

      CodecUtil.writeFooter(dataOut);
      CodecUtil.writeFooter(metaOut);
    }
  }

  private void writePartition(
      IndexOutput dataOut,
      org.apache.lucene.store.ByteBuffersDataOutput metaBuffer,
      int partitionId,
      org.apache.lucene.store.ByteBuffersDataOutput docIdBuffer,
      org.apache.lucene.store.ByteBuffersDataOutput vectorBuffer)
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
  public long ramBytesUsed() {
    long total = centroidDelegate.ramBytesUsed();
    for (SpannFieldVectorsWriter writer : fieldWriters.values()) {
      total += writer.ramBytesUsed();
    }
    return total;
  }

  @Override
  public void close() throws IOException {
    List<java.io.Closeable> toClose = new ArrayList<>();
    toClose.add(centroidDelegate);
    toClose.addAll(fieldWriters.values());
    IOUtils.close(toClose);
  }
}
