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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;

/**
* Reader for SPANN (HNSW-IVF) vectors.
*
* @lucene.experimental
*/
public class Lucene99SpannVectorsReader extends KnnVectorsReader {

  private final KnnVectorsReader centroidDelegate;
  private final Map<String, SpannFieldEntry> fields = new HashMap<>();

  public Lucene99SpannVectorsReader(SegmentReadState state, KnnVectorsFormat centroidFormat)
      throws IOException {
    this.centroidDelegate = centroidFormat.fieldsReader(state);

    for (FieldInfo fieldInfo : state.fieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldInfo.name + ".spam");
        String dataFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, fieldInfo.name + ".spad");

        IndexInput metaIn = null;
        IndexInput dataIn = null;
        boolean success = false;

        try {
          metaIn = state.directory.openInput(metaFileName, state.context);
        } catch (java.nio.file.NoSuchFileException | java.io.FileNotFoundException e) {
          continue;
        }

        try {
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

          // Build ordinal map for random access vectorValue(ord)
          int[] ordToDocId = new int[totalSize];
          long[] ordToOffset = new long[totalSize];
          int currentOrd = 0;
          IndexInput dataInClone = dataIn.clone();
          int vectorByteWidth = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE
              ? fieldInfo.getVectorDimension()
              : fieldInfo.getVectorDimension() * Float.BYTES;

          for (Integer pId : offsets.keySet()) {
            long start = offsets.get(pId);
            long len = lengths.get(pId);
            dataInClone.seek(start);
            long end = start + len;
            while (dataInClone.getFilePointer() < end) {
              ordToDocId[currentOrd] = dataInClone.readInt();
              ordToOffset[currentOrd] = dataInClone.getFilePointer();
              currentOrd++;
              dataInClone.skipBytes(vectorByteWidth);
            }
          }

          fields.put(fieldInfo.name,
              new SpannFieldEntry(offsets, lengths, dataIn, fieldInfo, totalSize, ordToDocId,
                  ordToOffset));
          success = true;
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
      // Meta check is done on open
      CodecUtil.checksumEntireFile(entry.dataIn);
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return centroidDelegate.getFloatVectorValues(field);
    }
    if (entry.fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
    return new SpannFloatVectorValues(entry);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      return centroidDelegate.getByteVectorValues(field);
    }
    if (entry.fieldInfo.getVectorEncoding() != VectorEncoding.BYTE) {
      return null;
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

    TopDocs topCentroids;
    if (entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      byte[] byteTarget = new byte[target.length];
      for (int i = 0; i < target.length; i++) {
        byteTarget[i] = (byte) target[i];
      }
      topCentroids = searchCentroids(field, byteTarget, acceptDocs);
    } else {
      topCentroids = searchCentroids(field, target, acceptDocs);
    }

    searchFine(entry, target, topCentroids, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    SpannFieldEntry entry = fields.get(field);
    if (entry == null) {
      centroidDelegate.search(field, target, knnCollector, acceptDocs);
      return;
    }

    TopDocs topCentroids;
    float[] floatTarget = new float[target.length];
    for (int i = 0; i < target.length; i++) {
      floatTarget[i] = (float) target[i];
    }

    if (entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      topCentroids = searchCentroids(field, target, acceptDocs);
    } else {
      topCentroids = searchCentroids(field, floatTarget, acceptDocs);
    }

    searchFine(entry, floatTarget, topCentroids, knnCollector, acceptDocs);
  }

  private TopDocs searchCentroids(String field, float[] target, AcceptDocs acceptDocs)
      throws IOException {
    int nprobe = 10;
    TopKnnCollector coarseCollector = new TopKnnCollector(nprobe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, acceptDocs);
    return coarseCollector.topDocs();
  }

  private TopDocs searchCentroids(String field, byte[] target, AcceptDocs acceptDocs)
      throws IOException {
    int nprobe = 10;
    TopKnnCollector coarseCollector = new TopKnnCollector(nprobe, Integer.MAX_VALUE);
    centroidDelegate.search(field, target, coarseCollector, acceptDocs);
    return coarseCollector.topDocs();
  }

  private void searchFine(SpannFieldEntry entry, float[] target, TopDocs topCentroids,
      KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
    IndexInput dataIn = entry.dataIn.clone();
    for (ScoreDoc centroidDoc : topCentroids.scoreDocs) {
      int partitionId = centroidDoc.doc;

      long startOffset = entry.offsets.getOrDefault(partitionId, -1L);
      if (startOffset == -1) {
        continue;
      }

      long lengthBytes = entry.lengths.get(partitionId);
      dataIn.seek(startOffset);
      long endOffset = startOffset + lengthBytes;

      boolean isByte = entry.fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
      byte[] byteTarget = null;
      if (isByte) {
        byteTarget = new byte[target.length];
        for (int i = 0; i < target.length; i++) {
          byteTarget[i] = (byte) target[i];
        }
      }

      while (dataIn.getFilePointer() < endOffset) {
        int docId = dataIn.readInt();
        float score;
        if (isByte) {
          byte[] vector = new byte[target.length];
          dataIn.readBytes(vector, 0, vector.length);
          score = entry.fieldInfo.getVectorSimilarityFunction().compare(byteTarget, vector);
        } else {
          float[] vector = new float[target.length];
          dataIn.readFloats(vector, 0, vector.length);
          score = entry.fieldInfo.getVectorSimilarityFunction().compare(target, vector);
        }

        Bits acceptedBits = acceptDocs == null ? null : acceptDocs.bits();
        knnCollector.incVisitedCount(1);
        if (acceptedBits == null || acceptedBits.get(docId)) {
          knnCollector.collect(docId, score);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(centroidDelegate);
    for (SpannFieldEntry entry : fields.values()) {
      IOUtils.close(entry.dataIn);
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

    SpannFieldEntry(Map<Integer, Long> offsets, Map<Integer, Long> lengths, IndexInput dataIn,
        FieldInfo fieldInfo, int totalSize, int[] ordToDocId, long[] ordToOffset) {
      this.offsets = offsets;
      this.lengths = lengths;
      this.dataIn = dataIn;
      this.fieldInfo = fieldInfo;
      this.totalSize = totalSize;
      this.ordToDocId = ordToDocId;
      this.ordToOffset = ordToOffset;
    }
  }

  private static class SpannFloatVectorValues extends FloatVectorValues {
    private final SpannFieldEntry entry;
    private final IndexInput dataIn;
    private int currentDoc = -1;
    private float[] currentVector;

    SpannFloatVectorValues(SpannFieldEntry entry) throws IOException {
      this.entry = entry;
      this.dataIn = entry.dataIn.clone();
      this.dataIn.seek(0);
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
      if (currentVector == null) {
        currentVector = new float[dimension()];
      }
      long offset = entry.ordToOffset[ord];
      dataIn.seek(offset);
      dataIn.readFloats(currentVector, 0, currentVector.length);
      return currentVector;
    }

    @Override
    public int ordToDoc(int ord) {
      return entry.ordToDocId[ord];
    }

    @Override
    public FloatVectorValues copy() throws IOException {
      return new SpannFloatVectorValues(entry);
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public VectorEncoding getEncoding() {
      return VectorEncoding.FLOAT32;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      SpannFloatVectorValues copy = (SpannFloatVectorValues) copy();
      DocIndexIterator it = copy.iterator();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return entry.fieldInfo.getVectorSimilarityFunction().compare(target, copy.vectorValue(it.index()));
        }

        @Override
        public DocIdSetIterator iterator() {
          return it;
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
      this.dataIn.seek(0);
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
      byte[] vector = new byte[dimension()];
      long offset = entry.ordToOffset[ord];
      dataIn.seek(offset);
      dataIn.readBytes(vector, 0, vector.length);
      return vector;
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

    @Override
    public VectorEncoding getEncoding() {
      return VectorEncoding.BYTE;
    }

    @Override
    public VectorScorer scorer(byte[] target) throws IOException {
      SpannByteVectorValues copy = (SpannByteVectorValues) copy();
      DocIndexIterator it = copy.iterator();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return entry.fieldInfo.getVectorSimilarityFunction().compare(target, copy.vectorValue(it.index()));
        }

        @Override
        public DocIdSetIterator iterator() {
          return it;
        }
      };
    }
  }
}
