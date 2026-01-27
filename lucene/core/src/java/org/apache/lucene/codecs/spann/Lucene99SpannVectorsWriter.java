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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

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
  private final int replicationFactor;

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
    this.replicationFactor = replicationFactor;
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    SpannFieldVectorsWriter writer = new SpannFieldVectorsWriter(fieldInfo);
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
    // We already collected data in addValue; we wait for finish() to write SPANN
    // files
  }

  @Override
  public void finish() throws IOException {
    for (Map.Entry<String, SpannFieldVectorsWriter> entry : fieldWriters.entrySet()) {
      String fieldName = entry.getKey();
      SpannFieldVectorsWriter writer = entry.getValue();
      FieldInfo fieldInfo = writer.getFieldInfo();

      if (writer.getVectors().isEmpty()) {
        continue;
      }

      float[][] vectorArray = writer.getVectors().toArray(new float[0][]);

      // Cap the number of partitions at configured limit (default 100)
      int numPartitions = Math.min(vectorArray.length, maxPartitions);

      // Downsample to keep flush time constant
      float[][] trainingVectors = SpannKMeans.downsample(vectorArray, clusteringSampleSize);

      float[][] centroids =
          SpannKMeans.cluster(
              trainingVectors,
              numPartitions,
              fieldInfo.getVectorSimilarityFunction(),
              KMEANS_MAX_ITERS);

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

      List<List<Integer>> partitionDocIds = new ArrayList<>(centroids.length);
      List<List<float[]>> partitions = new ArrayList<>(centroids.length);
      for (int i = 0; i < centroids.length; i++) {
        partitions.add(new ArrayList<>());
        partitionDocIds.add(new ArrayList<>());
      }

      VectorSimilarityFunction simFunc = fieldInfo.getVectorSimilarityFunction();
      List<float[]> vectors = writer.getVectors();
      List<Integer> docIds = writer.getDocIds();
      int totalAssignments = 0;

      for (int i = 0; i < vectors.size(); i++) {
        float[] vector = vectors.get(i);
        int docId = docIds.get(i);

        // Find top K centroids
        int[] bestCentroids = new int[replicationFactor];
        float[] bestScores = new float[replicationFactor];
        java.util.Arrays.fill(bestScores, Float.NEGATIVE_INFINITY);

        for (int c = 0; c < centroids.length; c++) {
          float score = simFunc.compare(vector, centroids[c]);
          // Insert into sorted list/array if better
          // Linear scan for small replication factor is fastest
          for (int r = 0; r < replicationFactor; r++) {
            if (score > bestScores[r]) {
              // Shift right
              for (int k = replicationFactor - 1; k > r; k--) {
                bestScores[k] = bestScores[k - 1];
                bestCentroids[k] = bestCentroids[k - 1];
              }
              bestScores[r] = score;
              bestCentroids[r] = c;
              break;
            }
          }
        }

        for (int r = 0; r < replicationFactor; r++) {
          if (bestScores[r] > Float.NEGATIVE_INFINITY) {
            partitions.get(bestCentroids[r]).add(vector);
            partitionDocIds.get(bestCentroids[r]).add(docId);
            totalAssignments++;
          }
        }
      }

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

        metaOut.writeVInt(totalAssignments); // Write total stored vectors, not unique docs
        for (int partitionId = 0; partitionId < partitions.size(); partitionId++) {
          List<float[]> clusterVectors = partitions.get(partitionId);
          long startOffset = dataOut.getFilePointer();

          for (int i = 0; i < clusterVectors.size(); i++) {
            float[] v = clusterVectors.get(i);
            int docId = partitionDocIds.get(partitionId).get(i);
            dataOut.writeInt(docId);
            if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
              for (float f : v) {
                dataOut.writeByte((byte) f);
              }
            } else {
              for (float f : v) {
                dataOut.writeInt(Float.floatToIntBits(f));
              }
            }
          }

          long lengthBytes = dataOut.getFilePointer() - startOffset;

          metaOut.writeVInt(partitionId);
          metaOut.writeVLong(startOffset);
          metaOut.writeVLong(lengthBytes);
        }

        CodecUtil.writeFooter(dataOut);
        CodecUtil.writeFooter(metaOut);
      }
    }

    centroidDelegate.flush(1, null); // maxDoc actually doesn't matter much for our use case here
    centroidDelegate.finish();
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
    IOUtils.close(centroidDelegate);
  }
}
