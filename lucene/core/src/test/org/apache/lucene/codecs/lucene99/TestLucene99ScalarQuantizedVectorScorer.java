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

package org.apache.lucene.codecs.lucene99;

import static org.apache.lucene.codecs.lucene99.OffHeapQuantizedByteVectorValues.compressBytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

public class TestLucene99ScalarQuantizedVectorScorer extends LuceneTestCase {

  private static Codec getCodec(int bits, boolean compress) {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarQuantizedVectorsFormat(
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            1,
            bits,
            compress,
            0f,
            null);
      }
    };
  }

  public void testNonZeroScores() throws IOException {
    for (int bits : new int[] {4, 7}) {
      for (boolean compress : new boolean[] {true, false}) {
        vectorNonZeroScoringTest(bits, compress);
      }
    }
  }

  private void vectorNonZeroScoringTest(int bits, boolean compress) throws IOException {
    try (Directory dir = newDirectory()) {
      // keep vecs `0` so dot product is `0`
      byte[] vec1 = new byte[32];
      byte[] vec2 = new byte[32];
      if (compress && bits == 4) {
        byte[] vec1Compressed = new byte[16];
        byte[] vec2Compressed = new byte[16];
        compressBytes(vec1, vec1Compressed);
        compressBytes(vec2, vec2Compressed);
        vec1 = vec1Compressed;
        vec2 = vec2Compressed;
      }
      String fileName = getTestName() + "-32";
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        // large negative offset to override any query score correction and
        // ensure negative values that need to be snapped to `0`
        var negativeOffset = floatToByteArray(-50f);
        byte[] bytes = concat(vec1, negativeOffset, vec2, negativeOffset);
        out.writeBytes(bytes, 0, bytes.length);
      }
      ScalarQuantizer scalarQuantizer = new ScalarQuantizer(0.1f, 0.9f, (byte) bits);
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        Lucene99ScalarQuantizedVectorScorer scorer =
            new Lucene99ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer());
        RandomAccessQuantizedByteVectorValues values =
            new RandomAccessQuantizedByteVectorValues() {
              @Override
              public int dimension() {
                return 32;
              }

              @Override
              public int getVectorByteLength() {
                return compress && bits == 4 ? 16 : 32;
              }

              @Override
              public int size() {
                return 2;
              }

              @Override
              public byte[] vectorValue(int ord) {
                return new byte[32];
              }

              @Override
              public float getScoreCorrectionConstant(int ord) {
                return -50;
              }

              @Override
              public RandomAccessQuantizedByteVectorValues copy() throws IOException {
                return this;
              }

              @Override
              public IndexInput getSlice() {
                return in;
              }

              @Override
              public ScalarQuantizer getScalarQuantizer() {
                return scalarQuantizer;
              }
            };
        float[] queryVector = new float[32];
        for (int i = 0; i < 32; i++) {
          queryVector[i] = i * 0.1f;
        }
        for (VectorSimilarityFunction function : VectorSimilarityFunction.values()) {
          RandomVectorScorer randomScorer =
              scorer.getRandomVectorScorer(function, values, queryVector);
          assertTrue(randomScorer.score(0) >= 0f);
          assertTrue(randomScorer.score(1) >= 0f);
        }
      }
    }
  }

  public void testScoringCompressedInt4() throws Exception {
    vectorScoringTest(4, true);
  }

  public void testScoringUncompressedInt4() throws Exception {
    vectorScoringTest(4, false);
  }

  public void testScoringInt7() throws Exception {
    vectorScoringTest(7, random().nextBoolean());
  }

  private void vectorScoringTest(int bits, boolean compress) throws IOException {
    float[][] storedVectors = new float[10][];
    int numVectors = 10;
    int vectorDimensions = random().nextInt(10) + 4;
    if (bits == 4 && vectorDimensions % 2 == 1) {
      vectorDimensions++;
    }
    for (int i = 0; i < numVectors; i++) {
      float[] vector = new float[vectorDimensions];
      for (int j = 0; j < vectorDimensions; j++) {
        vector[j] = i + j;
      }
      VectorUtil.l2normalize(vector);
      storedVectors[i] = vector;
    }

    // create lucene directory with codec
    for (VectorSimilarityFunction similarityFunction :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.DOT_PRODUCT,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
          VectorSimilarityFunction.EUCLIDEAN
        }) {
      try (Directory dir = newDirectory()) {
        indexVectors(dir, storedVectors, similarityFunction, bits, compress);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReader leafReader = reader.leaves().get(0).reader();
          float[] vector = new float[vectorDimensions];
          for (int i = 0; i < vectorDimensions; i++) {
            vector[i] = i + 1;
          }
          VectorUtil.l2normalize(vector);
          RandomVectorScorer randomScorer =
              getRandomVectorScorer(similarityFunction, leafReader, vector);
          float[] rawScores = new float[10];
          for (int i = 0; i < 10; i++) {
            rawScores[i] = similarityFunction.compare(vector, storedVectors[i]);
          }
          for (int i = 0; i < 10; i++) {
            assertEquals(similarityFunction.toString(), rawScores[i], randomScorer.score(i), 0.05f);
          }
        }
      }
    }
  }

  private RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction function, LeafReader leafReader, float[] vector) throws IOException {
    if (leafReader instanceof CodecReader) {
      KnnVectorsReader format = ((CodecReader) leafReader).getVectorReader();
      if (format instanceof PerFieldKnnVectorsFormat.FieldsReader) {
        format = ((PerFieldKnnVectorsFormat.FieldsReader) format).getFieldReader("field");
      }
      if (format instanceof Lucene99HnswVectorsReader) {
        OffHeapQuantizedByteVectorValues quantizedByteVectorReader =
            (OffHeapQuantizedByteVectorValues)
                ((Lucene99HnswVectorsReader) format).getQuantizedVectorValues("field");
        return new Lucene99ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer())
            .getRandomVectorScorer(function, quantizedByteVectorReader, vector);
      }
    }
    throw new IllegalArgumentException("Unsupported reader");
  }

  private static void indexVectors(
      Directory dir,
      float[][] vectors,
      VectorSimilarityFunction function,
      int bits,
      boolean compress)
      throws IOException {
    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig().setCodec(getCodec(bits, compress)))) {
      for (int i = 0; i < vectors.length; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) {
          // index a document without a vector
          writer.addDocument(doc);
        }
        writer.addDocument(doc);
        doc.add(new KnnFloatVectorField("field", vectors[i], function));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
    }
  }

  public void testSingleVectorPerSegmentCosine() throws IOException {
    testSingleVectorPerSegment(VectorSimilarityFunction.COSINE);
  }

  public void testSingleVectorPerSegmentDot() throws IOException {
    testSingleVectorPerSegment(VectorSimilarityFunction.DOT_PRODUCT);
  }

  public void testSingleVectorPerSegmentEuclidean() throws IOException {
    testSingleVectorPerSegment(VectorSimilarityFunction.EUCLIDEAN);
  }

  public void testSingleVectorPerSegmentMIP() throws IOException {
    testSingleVectorPerSegment(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
  }

  private void testSingleVectorPerSegment(VectorSimilarityFunction sim) throws IOException {
    var codec = getCodec(7, false);
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
        Document doc2 = new Document();
        doc2.add(new KnnFloatVectorField("field", new float[] {0.8f, 0.6f}, sim));
        doc2.add(newTextField("id", "A", Field.Store.YES));
        writer.addDocument(doc2);
        writer.commit();

        Document doc1 = new Document();
        doc1.add(new KnnFloatVectorField("field", new float[] {0.6f, 0.8f}, sim));
        doc1.add(newTextField("id", "B", Field.Store.YES));
        writer.addDocument(doc1);
        writer.commit();

        Document doc3 = new Document();
        doc3.add(new KnnFloatVectorField("field", new float[] {-0.6f, -0.8f}, sim));
        doc3.add(newTextField("id", "C", Field.Store.YES));
        writer.addDocument(doc3);
        writer.commit();

        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        StoredFields storedFields = reader.storedFields();
        float[] queryVector = new float[] {0.6f, 0.8f};
        var hits = leafReader.searchNearestVectors("field", queryVector, 3, null, 100);
        assertEquals(hits.scoreDocs.length, 3);
        assertEquals("B", storedFields.document(hits.scoreDocs[0].doc).get("id"));
        assertEquals("A", storedFields.document(hits.scoreDocs[1].doc).get("id"));
        assertEquals("C", storedFields.document(hits.scoreDocs[2].doc).get("id"));
      }
    }
  }

  private static byte[] floatToByteArray(float value) {
    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
  }

  private static byte[] concat(byte[]... arrays) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (var ba : arrays) {
        baos.write(ba);
      }
      return baos.toByteArray();
    }
  }
}
