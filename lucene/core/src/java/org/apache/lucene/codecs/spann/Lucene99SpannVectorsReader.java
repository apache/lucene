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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.IndexInput;
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
                new SpannFieldEntry(offsets, lengths, dataIn, fieldInfo, totalSize));
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

    TopDocs topCentroids = searchCentroids(field, target, acceptDocs);
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

    TopDocs topCentroids = searchCentroids(field, target, acceptDocs);
    searchFine(entry, null, target, topCentroids, knnCollector, acceptDocs);
  }

  private TopDocs searchCentroids(String field, float[] target, AcceptDocs acceptDocs)
      throws IOException {
    int candidateProbe = Math.max(nprobe * 4, 16);
    TopKnnCollector coarseCollector = new TopKnnCollector(candidateProbe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, acceptDocs);
    return coarseCollector.topDocs();
  }

  private TopDocs searchCentroids(String field, byte[] target, AcceptDocs acceptDocs)
      throws IOException {
    int candidateProbe = Math.max(nprobe * 4, 16);
    TopKnnCollector coarseCollector = new TopKnnCollector(candidateProbe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, acceptDocs);
    return coarseCollector.topDocs();
  }

  private void searchFine(
      SpannFieldEntry entry,
      float[] floatTarget,
      byte[] byteTarget,
      TopDocs topCentroids,
      KnnCollector knnCollector,
      AcceptDocs acceptDocs)
      throws IOException {
    try (IndexInput dataIn = entry.dataIn.clone()) {
      boolean isByte = entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
      int vectorByteWidth =
          isByte
              ? entry.fieldInfo.getVectorDimension()
              : entry.fieldInfo.getVectorDimension() * Float.BYTES;

      for (int i = 0; i < topCentroids.scoreDocs.length; i++) {
        int partitionId = topCentroids.scoreDocs[i].doc;
        Long offset = entry.offsets.get(partitionId);
        Long length = entry.lengths.get(partitionId);
        if (offset == null) continue;

        dataIn.seek(offset);
        int numDocs = (int) (length / (Integer.BYTES + vectorByteWidth));
        int[] docIds = new int[numDocs];
        for (int j = 0; j < numDocs; j++) {
          docIds[j] = dataIn.readInt();
        }

        for (int j = 0; j < numDocs; j++) {
          float score;
          if (isByte) {
            byte[] v = new byte[entry.fieldInfo.getVectorDimension()];
            dataIn.readBytes(v, 0, v.length);
            score = entry.fieldInfo.getVectorSimilarityFunction().compare(byteTarget, v);
          } else {
            float[] v = new float[entry.fieldInfo.getVectorDimension()];
            for (int d = 0; d < v.length; d++) {
              v[d] = Float.intBitsToFloat(dataIn.readInt());
            }
            score = entry.fieldInfo.getVectorSimilarityFunction().compare(floatTarget, v);
          }
          knnCollector.collect(docIds[j], score);
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
    final int totalSize;
    final int[] ordToDocId;
    final long[] ordToOffset;

    SpannFieldEntry(
        Map<Integer, Long> offsets,
        Map<Integer, Long> lengths,
        IndexInput dataIn,
        FieldInfo fieldInfo,
        int totalSize)
        throws IOException {
      this.offsets = offsets;
      this.lengths = lengths;
      this.dataIn = dataIn;
      this.fieldInfo = fieldInfo;
      this.totalSize = totalSize;
      this.ordToDocId = new int[totalSize];
      this.ordToOffset = new long[totalSize];

      // Build in-memory map (Path A style)
      try (IndexInput input = dataIn.clone()) {
        int currentOrd = 0;
        boolean isByte = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
        int vectorByteWidth =
            isByte ? fieldInfo.getVectorDimension() : fieldInfo.getVectorDimension() * Float.BYTES;

        // Sort partition IDs to scan sequentially
        Integer[] pIds = offsets.keySet().toArray(new Integer[0]);
        java.util.Arrays.sort(pIds);

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
            ordToDocId[currentOrd] = tempIds[i];
            ordToOffset[currentOrd] = vectorsStart + (long) i * vectorByteWidth;
            currentOrd++;
          }
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
      return entry.totalSize;
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
      return entry.totalSize;
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

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }
  }
}
