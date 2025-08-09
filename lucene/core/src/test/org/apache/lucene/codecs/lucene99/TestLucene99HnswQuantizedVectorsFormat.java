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

import static java.lang.String.format;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.junit.Before;

public class TestLucene99HnswQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  KnnVectorsFormat format;
  Float confidenceInterval;
  int bits;

  @Before
  @Override
  public void setUp() throws Exception {
    bits = random().nextBoolean() ? 4 : 7;
    confidenceInterval = random().nextBoolean() ? random().nextFloat(0.90f, 1.0f) : null;
    if (random().nextBoolean()) {
      confidenceInterval = 0f;
    }
    format = getKnnFormat(bits);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  private final KnnVectorsFormat getKnnFormat(int bits) {
    return new Lucene99HnswScalarQuantizedVectorsFormat(
        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
        1,
        bits,
        bits == 4 ? random().nextBoolean() : false,
        confidenceInterval,
        null);
  }

  // verifies it's fine to change your mind on the number of bits quantization you want for the same
  // field in the same index by changing up the Codec.  This is allowed because at merge time we
  // requantize the vectors.
  public void testMixedQuantizedBits() throws Exception {

    try (Directory dir = newDirectory()) {

      // add first vector using 4 bit quantization, then close index:
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(4))))) {

        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {0.6f, 0.8f}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      // create another writer using 7 bit quantization and add 2nd vector
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(7))))) {

        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {0.8f, 0.6f}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }

      // confirm searching works: we find both vectors
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        KnnFloatVectorQuery q = new KnnFloatVectorQuery("f", new float[] {0.7f, 0.7f}, 10);
        TopDocs topDocs = searcher.search(q, 100);
        assertEquals(2, topDocs.totalHits.value());
      }
    }
  }

  // verifies you can change your mind and enable quantization on a previously indexed vector field
  // without quantization
  public void testMixedQuantizedUnQuantized() throws Exception {

    try (Directory dir = newDirectory()) {

      // add first vector using no quantization
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig()
                  .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat())))) {

        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {0.6f, 0.8f}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      // create another writer using (7 bit) quantization and add 2nd vector
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(getKnnFormat(7))))) {

        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "f", new float[] {0.8f, 0.6f}, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }

      // confirm searching works: we find both vectors
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        KnnFloatVectorQuery q = new KnnFloatVectorQuery("f", new float[] {0.7f, 0.7f}, 10);
        TopDocs topDocs = searcher.search(q, 100);
        assertEquals(2, topDocs.totalHits.value());
      }
    }
  }

  public void testQuantizationScoringEdgeCase() throws Exception {
    float[][] vectors = new float[][] {{0.6f, 0.8f}, {0.8f, 0.6f}, {-0.6f, -0.8f}};
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setCodec(
                        TestUtil.alwaysKnnVectorsFormat(
                            new Lucene99HnswScalarQuantizedVectorsFormat(
                                16, 100, 1, (byte) 7, false, 0.9f, null))))) {
      for (float[] vector : vectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.commit();
      }
      w.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        TopKnnCollector topKnnCollector = new TopKnnCollector(5, Integer.MAX_VALUE);
        r.searchNearestVectors(
            "f",
            new float[] {0.6f, 0.8f},
            topKnnCollector,
            AcceptDocs.fromLiveDocs(null, r.maxDoc()));
        TopDocs topDocs = topKnnCollector.topDocs();
        assertEquals(3, topDocs.totalHits.value());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          assertTrue(scoreDoc.score >= 0f);
        }
      }
    }
  }

  public void testQuantizedVectorsWriteAndRead() throws Exception {
    int numVectors = 1 + random().nextInt(50);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    int dim = random().nextInt(64) + 1;
    if (dim % 2 == 1) {
      dim++;
    }
    List<float[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      vectors.add(randomVector(dim));
    }
    FloatVectorValues toQuantize =
        new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(vectors);
    ScalarQuantizer scalarQuantizer =
        Lucene99ScalarQuantizedVectorsWriter.buildScalarQuantizer(
            toQuantize, numVectors, similarityFunction, confidenceInterval, (byte) bits);
    float[] expectedCorrections = new float[numVectors];
    byte[][] expectedVectors = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      float[] vector = vectors.get(i);
      if (similarityFunction == VectorSimilarityFunction.COSINE) {
        float[] copy = new float[vector.length];
        System.arraycopy(vector, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        vector = copy;
      }

      expectedVectors[i] = new byte[dim];
      expectedCorrections[i] =
          scalarQuantizer.quantize(vector, expectedVectors[i], similarityFunction);
    }
    float[] randomlyReusedVector = new float[dim];

    // create lucene directory with codec
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig()
                    .setMaxBufferedDocs(numVectors + 1)
                    .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numVectors; i++) {
        Document doc = new Document();
        // randomly reuse a vector, this ensures the underlying codec doesn't rely on the array
        // reference
        final float[] v;
        if (random().nextBoolean()) {
          System.arraycopy(vectors.get(i), 0, randomlyReusedVector, 0, dim);
          v = randomlyReusedVector;
        } else {
          v = vectors.get(i);
        }
        doc.add(new KnnFloatVectorField("f", v, similarityFunction));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            knnVectorsReader = fieldsReader.getFieldReader("f");
          }
          if (knnVectorsReader instanceof Lucene99HnswVectorsReader hnswReader) {
            assertNotNull(hnswReader.getQuantizationState("f"));
            QuantizedByteVectorValues quantizedByteVectorValues =
                hnswReader.getQuantizedVectorValues("f");
            for (int ord = 0; ord < quantizedByteVectorValues.size(); ord++) {
              byte[] vector = quantizedByteVectorValues.vectorValue(ord);
              float offset = quantizedByteVectorValues.getScoreCorrectionConstant(ord);
              for (int i = 0; i < dim; i++) {
                assertEquals(vector[i], expectedVectors[ord][i]);
              }
              assertEquals(offset, expectedCorrections[ord], 0.00001f);
            }
          } else {
            fail("reader is not Lucene99HnswVectorsReader");
          }
        } else {
          fail("reader is not CodecReader");
        }
      }
    }
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene99HnswScalarQuantizedVectorsFormat(
                10, 20, 1, (byte) 4, false, 0.9f, null);
          }
        };
    String expectedPattern =
        "Lucene99HnswScalarQuantizedVectorsFormat(name=Lucene99HnswScalarQuantizedVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, confidenceInterval=0.9, bits=4, compress=false, flatVectorScorer=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s())))";
    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 7, false, 1.1f, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, -1, false, null, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 5, false, null, null));

    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 9, false, null, null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene99HnswScalarQuantizedVectorsFormat(20, 100, 0, 7, false, 0.8f, null));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene99HnswScalarQuantizedVectorsFormat(
                20, 100, 1, 7, false, null, new SameThreadExecutorService()));
  }

  // Ensures that all expected vector similarity functions are translatable
  // in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
    assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
  }
}
