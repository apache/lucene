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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Reads vectors from an index in the SPANN (HNSW-IVF) format.
 *
 * @lucene.experimental
 */
public class Lucene99SpannVectorsReader extends KnnVectorsReader {

  private final KnnVectorsReader centroidDelegate;
  private final Map<String, SpannFieldEntry> fields = new HashMap<>();
  private final int nprobe;

  public Lucene99SpannVectorsReader(
      SegmentReadState state, KnnVectorsFormat centroidFormat, int nprobe) throws IOException {
    if (nprobe < 1) {
      throw new IllegalArgumentException("nprobe must be >= 1; got " + nprobe);
    }
    this.nprobe = nprobe;
    this.centroidDelegate = centroidFormat.fieldsReader(state);

    for (FieldInfo fieldInfo : state.fieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        String metaFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, fieldInfo.name + ".spam");
        String dataFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, fieldInfo.name + ".spad");

        IndexInput metaIn = null;
        IndexInput dataIn = null;
        boolean success = false;

        try {
          if (Arrays.asList(state.directory.listAll()).contains(metaFileName)) {
            metaIn = state.directory.openInput(metaFileName, state.context);
            CodecUtil.checkIndexHeader(
                metaIn, "Lucene99SpannMeta", 0, 0, state.segmentInfo.getId(), state.segmentSuffix);

            int totalSize = metaIn.readVInt();
            Map<Integer, Long> offsets = new HashMap<>();
            Map<Integer, Long> lengths = new HashMap<>();

            while (metaIn.getFilePointer() < metaIn.length() - CodecUtil.footerLength()) {
              int partitionId = metaIn.readVInt();
              long offset = metaIn.readVLong();
              long length = metaIn.readVLong();
              offsets.put(partitionId, offset);
              lengths.put(partitionId, length);
            }

            dataIn = state.directory.openInput(dataFileName, state.context);
            CodecUtil.checkIndexHeader(
                dataIn, "Lucene99SpannData", 0, 0, state.segmentInfo.getId(), state.segmentSuffix);

            fields.put(
                fieldInfo.name,
                new SpannFieldEntry(
                    offsets, lengths, dataIn, fieldInfo, totalSize, state.segmentInfo.maxDoc()));
            success = true;
          }
        } finally {
          if (!success) {
            IOUtils.closeWhileHandlingException(metaIn, dataIn);
          } else {
            IOUtils.closeWhileHandlingException(metaIn);
          }
        }
      }
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    centroidDelegate.checkIntegrity();
    for (SpannFieldEntry entry : fields.values()) {
      CodecUtil.checksumEntireFile(entry.dataIn);
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return centroidDelegate.getFloatVectorValues(field);
    }
    return new SpannFloatVectorValues(entry);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return centroidDelegate.getByteVectorValues(field);
    }
    return new SpannByteVectorValues(entry);
  }

  /** Returns the centroids for the given field. */
  float[][] getCentroids(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return null;
    }

    if (entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      return null;
    }

    FloatVectorValues centroidValues = centroidDelegate.getFloatVectorValues(field);
    if (centroidValues == null) {
      return null;
    }

    int numCentroids = centroidValues.size();
    int dim = centroidValues.dimension();
    float[][] centroids = new float[numCentroids][dim];

    org.apache.lucene.index.KnnVectorValues.DocIndexIterator iterator = centroidValues.iterator();
    while (iterator.nextDoc() != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
      int id = iterator.index();
      if (id < numCentroids) {
        float[] v = centroidValues.vectorValue(id);
        System.arraycopy(v, 0, centroids[id], 0, dim);
      }
    }
    return centroids;
  }

  /**
   * Returns the weights (element counts) for each centroid/partition. Based on the on-disk segment
   * data.
   */
  long[] getCentroidWeights(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return null;
    }

    int numCentroids = 0;
    if (entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      ByteVectorValues bv = centroidDelegate.getByteVectorValues(field);
      if (bv != null) numCentroids = bv.size();
    } else {
      FloatVectorValues fv = centroidDelegate.getFloatVectorValues(field);
      if (fv != null) numCentroids = fv.size();
    }

    if (numCentroids == 0) {
      int maxId = -1;
      for (int id : entry.lengths.keySet()) {
        maxId = Math.max(maxId, id);
      }
      numCentroids = maxId + 1;
    }

    if (numCentroids == 0) return new long[0];

    long[] weights = new long[numCentroids];
    boolean isByte = entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
    int dim = entry.fieldInfo.getVectorDimension();
    int vectorByteWidth = isByte ? dim : dim * Float.BYTES;
    int bytesPerDoc = Integer.BYTES + vectorByteWidth;

    for (Map.Entry<Integer, Long> e : entry.lengths.entrySet()) {
      int id = e.getKey();
      long length = e.getValue();
      if (id < weights.length) {
        weights[id] = length / bytesPerDoc;
      }
    }
    return weights;
  }

  private static final AcceptDocs MATCH_ALL_ACCEPT_DOCS =
      new AcceptDocs() {
        @Override
        public int cost() {
          return Integer.MAX_VALUE;
        }

        @Override
        public Bits bits() {
          return new Bits.MatchAllBits(Integer.MAX_VALUE);
        }

        @Override
        public org.apache.lucene.search.DocIdSetIterator iterator() {
          return org.apache.lucene.search.DocIdSetIterator.all(Integer.MAX_VALUE);
        }
      };

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      centroidDelegate.search(field, target, knnCollector, acceptDocs);
      return;
    }

    if (entry.fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("float[] query on BYTE field");
    }

    int candidateProbe = Math.max(nprobe * 4, 16);
    TopKnnCollector coarseCollector = new TopKnnCollector(candidateProbe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, MATCH_ALL_ACCEPT_DOCS);
    TopDocs topCentroids = coarseCollector.topDocs();

    searchFine(entry, target, null, topCentroids, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      centroidDelegate.search(field, target, knnCollector, acceptDocs);
      return;
    }

    if (entry.fieldInfo.getVectorEncoding() != VectorEncoding.BYTE) {
      throw new IllegalArgumentException("byte[] query on FLOAT32 field");
    }

    int candidateProbe = Math.max(nprobe * 4, 16);
    TopKnnCollector coarseCollector = new TopKnnCollector(candidateProbe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, MATCH_ALL_ACCEPT_DOCS);
    TopDocs topCentroids = coarseCollector.topDocs();

    searchFine(entry, null, target, topCentroids, knnCollector, acceptDocs);
  }

  private void searchFine(
      SpannFieldEntry entry,
      float[] floatTarget,
      byte[] byteTarget,
      TopDocs topCentroids,
      KnnCollector knnCollector,
      AcceptDocs acceptDocs)
      throws IOException {
    Bits acceptBits = null;
    if (acceptDocs != null) {
      acceptBits = acceptDocs.bits();
    }
    org.apache.lucene.util.FixedBitSet visitedDocs =
        new org.apache.lucene.util.FixedBitSet(entry.maxDoc);

    try (IndexInput dataIn = entry.dataIn.clone()) {
      boolean isByte = entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
      int dim = entry.fieldInfo.getVectorDimension();
      int vectorByteWidth = isByte ? dim : dim * Float.BYTES;

      byte[] byteScratch = isByte ? new byte[vectorByteWidth] : null;
      float[] floatScratch = isByte ? null : new float[dim];

      int visitedPartitions = 0;

      for (int i = 0; i < topCentroids.scoreDocs.length; i++) {
        if (visitedPartitions >= nprobe) {
          break;
        }

        int partitionId = topCentroids.scoreDocs[i].doc;
        Long offset = entry.offsets.get(partitionId);
        Long length = entry.lengths.get(partitionId);

        if (offset == null) {
          continue;
        }

        dataIn.seek(offset);

        int numDocs = (int) (length / (Integer.BYTES + vectorByteWidth));
        int[] docIds = new int[numDocs];
        for (int j = 0; j < numDocs; j++) {
          docIds[j] = dataIn.readInt();
        }

        boolean anyAccepted = false;
        if (acceptBits == null) {
          for (int docId : docIds) {
            if (!visitedDocs.get(docId)) {
              anyAccepted = true;
              break;
            }
          }
        } else {
          for (int docId : docIds) {
            if (acceptBits.get(docId) && !visitedDocs.get(docId)) {
              anyAccepted = true;
              break;
            }
          }
        }

        if (!anyAccepted) {
          dataIn.skipBytes((long) numDocs * vectorByteWidth);
          continue;
        }

        visitedPartitions++;

        if (isByte) {
          for (int j = 0; j < numDocs; j++) {
            int docId = docIds[j];
            boolean shouldProcess =
                (acceptBits == null || acceptBits.get(docId)) && !visitedDocs.get(docId);

            if (!shouldProcess) {
              dataIn.skipBytes(vectorByteWidth);
              continue;
            }

            dataIn.readBytes(byteScratch, 0, vectorByteWidth);
            float score =
                entry.fieldInfo.getVectorSimilarityFunction().compare(byteTarget, byteScratch);
            knnCollector.incVisitedCount(1);
            knnCollector.collect(docId, score);
            visitedDocs.set(docId);
          }
        } else {
          for (int j = 0; j < numDocs; j++) {
            int docId = docIds[j];
            boolean shouldProcess =
                (acceptBits == null || acceptBits.get(docId)) && !visitedDocs.get(docId);

            if (!shouldProcess) {
              dataIn.skipBytes(vectorByteWidth);
              continue;
            }

            dataIn.readFloats(floatScratch, 0, dim);
            float score =
                entry.fieldInfo.getVectorSimilarityFunction().compare(floatTarget, floatScratch);
            knnCollector.incVisitedCount(1);
            knnCollector.collect(docId, score);
            visitedDocs.set(docId);
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    centroidDelegate.close();
    for (SpannFieldEntry entry : fields.values()) {
      entry.dataIn.close();
    }
  }

  private static class SpannFieldEntry {
    final Map<Integer, Long> offsets;
    final Map<Integer, Long> lengths;
    final IndexInput dataIn;
    final FieldInfo fieldInfo;
    final int maxDoc;
    final int docCount;
    final int[] ordToDocId;
    final long[] ordToOffset;

    SpannFieldEntry(
        Map<Integer, Long> offsets,
        Map<Integer, Long> lengths,
        IndexInput dataIn,
        FieldInfo fieldInfo,
        int totalSize,
        int maxDoc)
        throws IOException {
      this.offsets = offsets;
      this.lengths = lengths;
      this.dataIn = dataIn;
      this.fieldInfo = fieldInfo;
      this.maxDoc = maxDoc;
      long[] docIdToOffset = new long[maxDoc];
      Arrays.fill(docIdToOffset, -1L);

      try (IndexInput input = dataIn.clone()) {
        boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
        int vectorByteWidth =
            isByte ? fieldInfo.getVectorDimension() : fieldInfo.getVectorDimension() * Float.BYTES;

        Integer[] pIds = offsets.keySet().toArray(new Integer[0]);
        Arrays.sort(pIds);

        for (int pId : pIds) {
          long offset = offsets.get(pId);
          long length = lengths.get(pId);
          input.seek(offset);
          int numDocs = (int) (length / (Integer.BYTES + vectorByteWidth));

          int[] tempIds = new int[numDocs];
          for (int i = 0; i < numDocs; i++) {
            tempIds[i] = input.readInt();
          }

          long vectorsStart = offset + (long) numDocs * Integer.BYTES;
          for (int i = 0; i < numDocs; i++) {
            int docId = tempIds[i];
            if (docIdToOffset[docId] == -1L) {
              docIdToOffset[docId] = vectorsStart + (long) i * vectorByteWidth;
            }
          }
        }
      }

      int count = 0;
      for (long offset : docIdToOffset) {
        if (offset != -1L) {
          count++;
        }
      }
      this.docCount = count;
      this.ordToDocId = new int[docCount];
      this.ordToOffset = new long[docCount];
      int ord = 0;
      for (int docId = 0; docId < docIdToOffset.length; docId++) {
        long offset = docIdToOffset[docId];
        if (offset != -1L) {
          ordToDocId[ord] = docId;
          ordToOffset[ord] = offset;
          ord++;
        }
      }
    }
  }

  private static class SpannFloatVectorValues extends FloatVectorValues {
    private final SpannFieldEntry entry;
    private final IndexInput dataIn;

    SpannFloatVectorValues(SpannFieldEntry entry) throws IOException {
      this.entry = entry;
      this.dataIn = entry.dataIn.clone();
    }

    @Override
    public int dimension() {
      return entry.fieldInfo.getVectorDimension();
    }

    @Override
    public int size() {
      return entry.docCount;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      float[] v = new float[dimension()];
      dataIn.seek(entry.ordToOffset[ord]);
      for (int d = 0; d < v.length; d++) {
        v[d] = Float.intBitsToFloat(dataIn.readInt());
      }
      return v;
    }

    @Override
    public int ordToDoc(int ord) {
      return entry.ordToDocId[ord];
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public FloatVectorValues copy() throws IOException {
      return new SpannFloatVectorValues(entry);
    }

    void vectorValue(int ord, float[] reuse) throws IOException {
      dataIn.seek(entry.ordToOffset[ord]);
      for (int d = 0; d < reuse.length; d++) {
        reuse[d] = Float.intBitsToFloat(dataIn.readInt());
      }
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      final SpannFloatVectorValues copy = (SpannFloatVectorValues) this.copy();
      final KnnVectorValues.DocIndexIterator iter = copy.iterator();
      final float[] scratch = new float[dimension()];
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          copy.vectorValue(iter.index(), scratch);
          return entry.fieldInfo.getVectorSimilarityFunction().compare(target, scratch);
        }

        @Override
        public org.apache.lucene.search.DocIdSetIterator iterator() {
          return iter;
        }
      };
    }
  }

  private static class SpannByteVectorValues extends ByteVectorValues {
    private final SpannFieldEntry entry;
    private final IndexInput dataIn;

    SpannByteVectorValues(SpannFieldEntry entry) throws IOException {
      this.entry = entry;
      this.dataIn = entry.dataIn.clone();
    }

    @Override
    public int dimension() {
      return entry.fieldInfo.getVectorDimension();
    }

    @Override
    public int size() {
      return entry.docCount;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      byte[] v = new byte[dimension()];
      dataIn.seek(entry.ordToOffset[ord]);
      dataIn.readBytes(v, 0, v.length);
      return v;
    }

    @Override
    public int ordToDoc(int ord) {
      return entry.ordToDocId[ord];
    }

    @Override
    public ByteVectorValues copy() throws IOException {
      return new SpannByteVectorValues(entry);
    }

    void vectorValue(int ord, byte[] reuse) throws IOException {
      dataIn.seek(entry.ordToOffset[ord]);
      dataIn.readBytes(reuse, 0, reuse.length);
    }

    @Override
    public VectorScorer scorer(byte[] target) throws IOException {
      final SpannByteVectorValues copy = (SpannByteVectorValues) this.copy();
      final KnnVectorValues.DocIndexIterator iter = copy.iterator();
      final byte[] scratch = new byte[dimension()];
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          copy.vectorValue(iter.index(), scratch);
          return entry.fieldInfo.getVectorSimilarityFunction().compare(target, scratch);
        }

        @Override
        public org.apache.lucene.search.DocIdSetIterator iterator() {
          return iter;
        }
      };
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }
  }
}
