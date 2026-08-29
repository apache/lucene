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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

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

  private final Map<String, float[][]> preComputedCentroids = new HashMap<>();

  @Override
  public void mergeOneField(FieldInfo fieldInfo, org.apache.lucene.index.MergeState mergeState)
      throws IOException {
    if (fieldInfo.hasVectorValues() == false
        || fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      super.mergeOneField(fieldInfo, mergeState);
      return;
    }

    List<float[]> allCentroids = new ArrayList<>();
    List<Long> allWeights = new ArrayList<>();
    boolean canOptimize = true;

    for (KnnVectorsReader reader : mergeState.knnVectorsReaders) {
      if (reader instanceof Lucene99SpannVectorsReader == false) {
        canOptimize = false;
        break;
      }

      Lucene99SpannVectorsReader spannReader = (Lucene99SpannVectorsReader) reader;
      float[][] segCentroids = spannReader.getCentroids(fieldInfo.name);
      long[] segWeights = spannReader.getCentroidWeights(fieldInfo.name);

      if (segCentroids == null
          || segWeights == null
          || segCentroids.length != segWeights.length
          || segCentroids.length == 0) {
        canOptimize = false;
        break;
      }

      for (int k = 0; k < segCentroids.length; k++) {
        allCentroids.add(segCentroids[k]);
        allWeights.add(segWeights[k]);
      }
    }

    long totalVectors = 0;
    if (canOptimize && allCentroids.isEmpty() == false) {
      for (long w : allWeights) {
        totalVectors += w;
      }
      if (totalVectors == 0) {
        canOptimize = false;
      }
    }

    if (canOptimize && allCentroids.isEmpty() == false) {
      int targetPartitions = maxPartitions;
      if (targetPartitions == -1) {
        targetPartitions = (int) (2.0 * Math.sqrt((double) totalVectors));
      }
      targetPartitions = Math.min((int) totalVectors, Math.max(1, targetPartitions));

      float[][] centroidArray = allCentroids.toArray(new float[0][]);
      long[] weightArray = allWeights.stream().mapToLong(l -> l).toArray();

      float[][] mergedCentroids =
          SpannKMeans.clusterWeighted(
              centroidArray,
              weightArray,
              targetPartitions,
              fieldInfo.getVectorSimilarityFunction(),
              KMEANS_MAX_ITERS);
      preComputedCentroids.put(fieldInfo.name, mergedCentroids);
    }

    super.mergeOneField(fieldInfo, mergeState);
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    SpannFieldVectorsWriter writer = new SpannFieldVectorsWriter(fieldInfo, state);
    if (preComputedCentroids.containsKey(fieldInfo.name)) {
      writer.setCentroids(preComputedCentroids.get(fieldInfo.name));
    }
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
          return 0;
        }
      };
    }

    return writer;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {}

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
        writer.close();

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

        writeSinglePartition(fieldName, fieldInfo, vectorArray, docIds);
        continue;
      }

      float[][] centroids = writer.getPreDefinedCentroids();
      int numPartitions;
      if (centroids == null) {
        numPartitions = maxPartitions;
        if (numPartitions == -1) {
          numPartitions = (int) (2.0 * Math.sqrt(writer.getCount()));
        }
        numPartitions = Math.min(writer.getCount(), Math.max(1, numPartitions));

        int sampleSize = Math.min(clusteringSampleSize, writer.getCount());
        float[][] trainingVectors = new float[sampleSize][fieldInfo.getVectorDimension()];
        java.util.Random random = new java.util.Random(42);
        try (IndexInput input = writer.openInput()) {
          boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
          for (int i = 0; i < writer.getCount(); i++) {
            input.readInt();
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

        centroids =
            SpannKMeans.cluster(
                trainingVectors,
                numPartitions,
                fieldInfo.getVectorSimilarityFunction(),
                KMEANS_MAX_ITERS);
      } else {
        numPartitions = centroids.length;
      }
      maxCentroidDoc = Math.max(maxCentroidDoc, centroids.length);

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

      String spadFile =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, fieldName + ".spad");
      String spamFile =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, fieldName + ".spam");

      long totalAssignments = (long) writer.getCount() * replicationFactor;
      try (SpannDiskPartitioner partitioner =
              new SpannDiskPartitioner(
                  state.directory, state.segmentInfo, fieldName, state.context, totalAssignments);
          IndexInput input = writer.openInput();
          IndexOutput dataOut = state.directory.createOutput(spadFile, state.context);
          IndexOutput metaOut = state.directory.createOutput(spamFile, state.context)) {

        CodecUtil.writeIndexHeader(
            dataOut, "Lucene99SpannData", 0, state.segmentInfo.getId(), state.segmentSuffix);
        CodecUtil.writeIndexHeader(
            metaOut, "Lucene99SpannMeta", 0, state.segmentInfo.getId(), state.segmentSuffix);

        boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
        float[] currentVector = new float[fieldInfo.getVectorDimension()];

        class CentroidScorer
            extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
          private final float[][] centroids;
          private float[] query;

          CentroidScorer(float[][] centroids) {
            super(null);
            this.centroids = centroids;
          }

          @Override
          public void setScoringOrdinal(int node) {
            query = centroids[node];
          }

          public void setQuery(float[] query) {
            this.query = query;
          }

          @Override
          public float score(int node) throws IOException {
            return fieldInfo.getVectorSimilarityFunction().compare(query, centroids[node]);
          }

          @Override
          public int maxOrd() {
            return centroids.length;
          }

          @Override
          public int ordToDoc(int ord) {
            return ord;
          }

          @Override
          public Bits getAcceptOrds(Bits acceptDocs) {
            return null;
          }
        }

        final float[][] finalCentroids = centroids;
        CentroidScorer scorer = new CentroidScorer(finalCentroids);

        RandomVectorScorerSupplier scorerSupplier =
            new RandomVectorScorerSupplier() {
              @Override
              public UpdateableRandomVectorScorer scorer() {
                return new CentroidScorer(finalCentroids);
              }

              @Override
              public RandomVectorScorerSupplier copy() {
                return this;
              }
            };

        HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
        OnHeapHnswGraph centroidGraph = builder.build(centroids.length);

        for (int i = 0; i < writer.getCount(); i++) {
          input.readInt(); // Skip DocID
          if (isByte) {
            byte[] vectorBytes = new byte[fieldInfo.getVectorDimension()];
            for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
              byte b = input.readByte();
              vectorBytes[d] = b;
              currentVector[d] = (float) b;
            }
          } else {
            for (int d = 0; d < fieldInfo.getVectorDimension(); d++) {
              currentVector[d] = Float.intBitsToFloat(input.readInt());
            }
          }

          scorer.setQuery(currentVector);
          KnnCollector collector =
              HnswGraphSearcher.search(
                  scorer, replicationFactor, centroidGraph, null, Integer.MAX_VALUE);
          org.apache.lucene.search.TopDocs topDocs = collector.topDocs();
          for (ScoreDoc sd : topDocs.scoreDocs) {
            partitioner.addAssignment(sd.doc, i);
          }
        }

        partitioner.finish(
            input.clone(),
            dataOut,
            metaOut,
            fieldInfo.getVectorEncoding(),
            fieldInfo.getVectorDimension());
      }
      writer.close();
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

      metaOut.writeVInt(0);
      metaOut.writeVLong(startOffset);
      metaOut.writeVLong(lengthBytes);

      CodecUtil.writeFooter(dataOut);
      CodecUtil.writeFooter(metaOut);
    }
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
