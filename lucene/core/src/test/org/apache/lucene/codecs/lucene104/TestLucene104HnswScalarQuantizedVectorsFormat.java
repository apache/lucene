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
package org.apache.lucene.codecs.lucene104;

import static java.lang.String.format;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.junit.Before;

public class TestLucene104HnswScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private KnnVectorsFormat format;
  private ScalarEncoding encoding;

  @Before
  @Override
  public void setUp() throws Exception {
    var encodingValues = ScalarEncoding.values();
    encoding = encodingValues[random().nextInt(encodingValues.length)];
    format =
        new Lucene104HnswScalarQuantizedVectorsFormat(
            encoding,
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            1,
            null);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 10, 20, 1, null);
          }
        };
    String expectedPattern =
        "Lucene104HnswScalarQuantizedVectorsFormat(name=Lucene104HnswScalarQuantizedVectorsFormat,"
            + " maxConn=10, beamWidth=20, tinySegmentsThreshold=100,"
            + " flatVectorFormat=Lucene104ScalarQuantizedVectorsFormat(name=Lucene104ScalarQuantizedVectorsFormat,"
            + " encoding=UNSIGNED_BYTE,"
            + " flatVectorScorer=Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate=%s()),"
            + " rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s())))";

    var defaultScorer =
        format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer", "DefaultFlatVectorScorer");
    var memSegScorer =
        format(
            Locale.ROOT,
            expectedPattern,
            "Lucene99MemorySegmentFlatVectorsScorer",
            "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testSingleVectorCase() throws Exception {
    float[] vector = randomVector(random().nextInt(12, 500));
    for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        float[] docVector =
            similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                ? VectorUtil.l2normalize(ArrayUtil.copyArray(vector))
                : vector;
        doc.add(new KnnFloatVectorField("f", docVector, similarityFunction));
        w.addDocument(doc);
        w.commit();
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues("f");
          KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
          assert (vectorValues.size() == 1);
          while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            assertArrayEquals(
                docVector, vectorValues.vectorValue(docIndexIterator.index()), 0.00001f);
          }
          float[] randomVector =
              similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                  ? randomNormalizedVector(vector.length)
                  : randomVector(vector.length);
          float trueScore = similarityFunction.compare(docVector, randomVector);
          TopDocs td =
              r.searchNearestVectors(
                  "f",
                  randomVector,
                  1,
                  AcceptDocs.fromLiveDocs(null, r.maxDoc()),
                  Integer.MAX_VALUE);
          assertEquals(1, td.totalHits.value());
          assertTrue(td.scoreDocs[0].score >= 0);
          // When it's the only vector in a segment, the score should be very close to the true
          // score
          assertEquals(trueScore, td.scoreDocs[0].score, 0.01f);
        }
      }
    }
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 20, 100, 1, new SameThreadExecutorService()));
  }

  // Ensures that all expected vector similarity functions are translatable in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
    assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
  }

  public void testSimpleOffHeapSize() throws IOException {
    float[] vector = randomVector(random().nextInt(12, 500));
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          knnVectorsReader = knnVectorsReader.unwrapReaderForField("f");
          var fieldInfo = r.getFieldInfos().fieldInfo("f");
          var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
          assertEquals(vector.length * Float.BYTES, (long) offHeap.get("vec"));
          assertNotNull(offHeap.get("vex"));
          long corrections = Float.BYTES + Float.BYTES + Float.BYTES + Integer.BYTES;
          long expected = encoding.getDocPackedLength(fieldInfo.getVectorDimension()) + corrections;
          assertEquals(expected, (long) offHeap.get("veq"));
          assertEquals(3, offHeap.size());
        }
      }
    }
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }
}
