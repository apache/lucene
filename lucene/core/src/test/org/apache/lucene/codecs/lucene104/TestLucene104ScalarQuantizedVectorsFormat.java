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
import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloat16VectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.junit.Before;

public class TestLucene104ScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private ScalarEncoding encoding;
  private KnnVectorsFormat format;

  @Before
  @Override
  public void setUp() throws Exception {
    var encodingValues = ScalarEncoding.values();
    encoding = encodingValues[random().nextInt(encodingValues.length)];
    format = new Lucene104ScalarQuantizedVectorsFormat(encoding);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  public void testSearch() throws Exception {
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = random().nextInt(4, 65);
    float[] vector = randomVector(dims);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
    IndexWriterConfig iwc = newIndexWriterConfig();
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomVector(dims));
          doc.add(knnField);
          w.addDocument(doc);
        }
        w.commit();

        try (IndexReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          final int k = random().nextInt(5, 50);
          float[] queryVector = randomVector(dims);
          Query q = new KnnFloatVectorQuery(fieldName, queryVector, k);
          TopDocs collectedDocs = searcher.search(q, k);
          assertEquals(k, collectedDocs.totalHits.value());
          assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
        }
      }
    }
  }

  public void testFloat16Search() throws Exception {
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = 2 * random().nextInt(2, 33);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloat16VectorField knnField =
        new KnnFloat16VectorField(
            fieldName, randomNormalizedFloat16Vector(dims), similarityFunction);
    IndexWriterConfig iwc = newIndexWriterConfig();
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomNormalizedFloat16Vector(dims));
          doc.add(knnField);
          w.addDocument(doc);
        }
        w.commit();

        try (IndexReader reader = DirectoryReader.open(w)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          final int k = random().nextInt(5, 50);
          short[] queryVector = randomNormalizedFloat16Vector(dims);
          // Routes scoring through Lucene104ScalarQuantizedVectorScorer's short[] (fp16) branch.
          Query q = new KnnFloat16VectorQuery(fieldName, queryVector, k);
          TopDocs collectedDocs = searcher.search(q, k);
          assertEquals(k, collectedDocs.totalHits.value());
          assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
        }
      }
    }
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene104ScalarQuantizedVectorsFormat();
          }
        };
    String expectedPattern =
        "Lucene104ScalarQuantizedVectorsFormat("
            + "name=Lucene104ScalarQuantizedVectorsFormat, "
            + "encoding=UNSIGNED_BYTE, "
            + "flatVectorScorer=Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate=%s()), "
            + "rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s()))";
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

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // graph not supported
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // visited limit is not respected, as it is brute force search
  }

  public void testQuantizedVectorsWriteAndRead() throws IOException {
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = random().nextInt(4, 65);

    float[] vector = randomVector(dims);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomVector(dims));
          doc.add(knnField);
          w.addDocument(doc);
          if (i % 101 == 0) {
            w.commit();
          }
        }
        w.commit();
        w.forceMerge(1);

        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          assertEquals(vectorValues.size(), numVectors);
          QuantizedByteVectorValues qvectorValues =
              ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues) vectorValues)
                  .getQuantizedVectorValues();
          float[] centroid = qvectorValues.getCentroid();
          assertEquals(centroid.length, dims);

          OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
          byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];
          byte[] expectedVector = new byte[encoding.getDocPackedLength(scratch.length)];
          if (similarityFunction == VectorSimilarityFunction.COSINE) {
            vectorValues = new NormalizedFloatVectorValues(vectorValues);
          }
          KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();

          while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            OptimizedScalarQuantizer.QuantizationResult corrections =
                quantizer.scalarQuantize(
                    vectorValues.vectorValue(docIndexIterator.index()),
                    scratch,
                    encoding.getBits(),
                    centroid);
            switch (encoding) {
              case UNSIGNED_BYTE, SEVEN_BIT ->
                  System.arraycopy(scratch, 0, expectedVector, 0, dims);
              case PACKED_NIBBLE ->
                  OffHeapScalarQuantizedVectorValues.packNibbles(scratch, expectedVector);
              case SINGLE_BIT_QUERY_NIBBLE ->
                  OptimizedScalarQuantizer.packAsBinary(scratch, expectedVector);
              case DIBIT_QUERY_NIBBLE ->
                  OptimizedScalarQuantizer.transposeDibit(scratch, expectedVector);
            }
            assertArrayEquals(expectedVector, qvectorValues.vectorValue(docIndexIterator.index()));
            var actualCorrections = qvectorValues.getCorrectiveTerms(docIndexIterator.index());
            assertEquals(corrections.lowerInterval(), actualCorrections.lowerInterval(), 0.00001f);
            assertEquals(corrections.upperInterval(), actualCorrections.upperInterval(), 0.00001f);
            assertEquals(
                corrections.additionalCorrection(),
                actualCorrections.additionalCorrection(),
                0.00001f);
            assertEquals(
                corrections.quantizedComponentSum(), actualCorrections.quantizedComponentSum());
          }
        }
      }
    }
  }

  /**
   * fp16 counterpart of {@link #testQuantizedVectorsWriteAndRead()}: indexes float16 vectors and
   * verifies the persisted quantized bytes and corrective terms match a reference re-quantization.
   * The reference mirrors the writer's fp16 path exactly, inflating fp16 to fp32 and normalizing
   * for COSINE before quantizing, so the comparison is byte-exact rather than MAE-based.
   */
  public void testFloat16QuantizedVectorsWriteAndRead() throws IOException {
    String fieldName = "field";
    int numVectors = random().nextInt(99, 500);
    int dims = 2 * random().nextInt(2, 33);

    VectorSimilarityFunction similarityFunction = randomSimilarity();
    KnnFloat16VectorField knnField =
        new KnnFloat16VectorField(
            fieldName, randomNormalizedFloat16Vector(dims), similarityFunction);
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          knnField.setVectorValue(randomNormalizedFloat16Vector(dims));
          doc.add(knnField);
          w.addDocument(doc);
          if (i % 101 == 0) {
            w.commit();
          }
        }
        w.commit();
        w.forceMerge(1);

        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          Float16VectorValues vectorValues = r.getFloat16VectorValues(fieldName);
          assertEquals(vectorValues.size(), numVectors);
          QuantizedByteVectorValues qvectorValues =
              ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedFloat16VectorValues)
                      vectorValues)
                  .getQuantizedVectorValues();
          float[] centroid = qvectorValues.getCentroid();
          assertEquals(centroid.length, dims);

          OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
          byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];
          byte[] expectedVector = new byte[encoding.getDocPackedLength(scratch.length)];
          float[] inflated = new float[dims];

          KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
          while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            // Reproduce the writer's fp16 reference: inflate to fp32, normalize for COSINE.
            short[] raw = vectorValues.vectorValue(docIndexIterator.index());
            for (int i = 0; i < dims; i++) {
              inflated[i] = Float.float16ToFloat(raw[i]);
            }
            if (similarityFunction == VectorSimilarityFunction.COSINE) {
              VectorUtil.l2normalize(inflated);
            }
            OptimizedScalarQuantizer.QuantizationResult corrections =
                quantizer.scalarQuantize(inflated, scratch, encoding.getBits(), centroid);
            switch (encoding) {
              case UNSIGNED_BYTE, SEVEN_BIT ->
                  System.arraycopy(scratch, 0, expectedVector, 0, dims);
              case PACKED_NIBBLE ->
                  OffHeapScalarQuantizedVectorValues.packNibbles(scratch, expectedVector);
              case SINGLE_BIT_QUERY_NIBBLE ->
                  OptimizedScalarQuantizer.packAsBinary(scratch, expectedVector);
              case DIBIT_QUERY_NIBBLE ->
                  OptimizedScalarQuantizer.transposeDibit(scratch, expectedVector);
            }
            assertArrayEquals(expectedVector, qvectorValues.vectorValue(docIndexIterator.index()));
            var actualCorrections = qvectorValues.getCorrectiveTerms(docIndexIterator.index());
            assertEquals(corrections.lowerInterval(), actualCorrections.lowerInterval(), 0.00001f);
            assertEquals(corrections.upperInterval(), actualCorrections.upperInterval(), 0.00001f);
            assertEquals(
                corrections.additionalCorrection(),
                actualCorrections.additionalCorrection(),
                0.00001f);
            assertEquals(
                corrections.quantizedComponentSum(), actualCorrections.quantizedComponentSum());
          }
        }
      }
    }
  }

  /**
   * Reads float16 vectors back from an index whose raw {@code .vec} data has been dropped, so
   * values are reconstructed through {@link OffHeapScalarQuantizedFloat16VectorValues}, and asserts
   * they stay within the quantization error bound.
   */
  public void testReadQuantizedFloat16VectorWithEmptyRawVectors() throws Exception {
    String vectorFieldName = "vec1";
    int numVectors = 1 + random().nextInt(50);
    int dim = 2 * random().nextInt(1, 33);
    // Quantization error bound, plus a small slack for the extra fp16 rounding applied on top of
    // quantization (both the stored input and the dequantized output are fp16-rounded).
    float eps = (1f / (float) (1 << getQuantizationBits())) + 1e-3f;
    VectorSimilarityFunction similarityFunction = randomSimilarity();

    // Build fp16 (short-bit) source vectors; keep them to form the MAE reference on read-back.
    List<short[]> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      vectors.add(randomNormalizedFloat16Vector(dim));
    }

    try (BaseDirectoryWrapper dir = newDirectory()) {
      dir.setCheckIndexOnClose(false); // raw .vec is deliberately emptied below

      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMaxBufferedDocs(numVectors + 1)
                  .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                  .setMergePolicy(NoMergePolicy.INSTANCE)
                  .setUseCompoundFile(false)
                  .setCodec(getCodecForFloatVectorFallbackTest()))) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          doc.add(new KnnFloat16VectorField(vectorFieldName, vectors.get(i), similarityFunction));
          w.addDocument(doc);
        }
      }

      // Drop the raw float16 vectors, leaving only the quantized data.
      simulateEmptyRawVectors(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          knnVectorsReader = knnVectorsReader.unwrapReaderForField(vectorFieldName);
          // With raw vectors dropped this routes through OffHeapScalarQuantizedFloat16VectorValues.
          Float16VectorValues float16VectorValues =
              knnVectorsReader.getFloat16VectorValues(vectorFieldName);
          if (float16VectorValues.size() > 0) {
            KnnVectorValues.DocIndexIterator iter = float16VectorValues.iterator();
            for (int docId = iter.nextDoc(); docId != NO_MORE_DOCS; docId = iter.nextDoc()) {
              short[] dequantizedVector = float16VectorValues.vectorValue(iter.index());
              short[] originalVector = vectors.get(docId);
              float mae = 0;
              for (int i = 0; i < dim; i++) {
                mae +=
                    Math.abs(
                        Float.float16ToFloat(dequantizedVector[i])
                            - Float.float16ToFloat(originalVector[i]));
              }
              mae /= dim;
              assertTrue(
                  "bits: " + getQuantizationBits() + " mae: " + mae + " > eps: " + eps, mae <= eps);
            }
          } else {
            fail("float16VectorValues size should be non zero");
          }
        } else {
          fail("reader is not CodecReader");
        }
      }
    }
  }

  /**
   * Tests that dropping the raw float16 vectors does not change scoring. {@link
   * Float16VectorValues#scorer(short[])} is documented to score against the quantized vectors when
   * the underlying format quantizes, so a quantized index must produce identical scores before and
   * after its raw vector file is emptied.
   */
  public void testFloat16ScoresUnchangedWithEmptyRawVectors() throws Exception {
    String vectorFieldName = "vec1";
    int numVectors = 1 + random().nextInt(50);
    int dim = 2 * random().nextInt(1, 33);
    VectorSimilarityFunction similarityFunction = randomSimilarity();
    short[] query = randomNormalizedFloat16Vector(dim);

    try (BaseDirectoryWrapper dir = newDirectory()) {
      dir.setCheckIndexOnClose(false); // raw .vec is deliberately emptied below

      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setMaxBufferedDocs(numVectors + 1)
                  .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                  .setMergePolicy(NoMergePolicy.INSTANCE)
                  .setUseCompoundFile(false)
                  .setCodec(getCodecForFloatVectorFallbackTest()))) {
        for (int i = 0; i < numVectors; i++) {
          Document doc = new Document();
          doc.add(
              new KnnFloat16VectorField(
                  vectorFieldName, randomNormalizedFloat16Vector(dim), similarityFunction));
          w.addDocument(doc);
        }
      }

      // Scores while the raw float16 vectors are still present.
      Map<Integer, Float> expectedScores = scoreAllFloat16Docs(dir, vectorFieldName, query);
      assertEquals("expected every document to be scored", numVectors, expectedScores.size());

      simulateEmptyRawVectors(dir);

      // Both reads are expected to score against the same quantized vectors, so the scores must
      // match exactly rather than merely within a quantization-error tolerance.
      Map<Integer, Float> actualScores = scoreAllFloat16Docs(dir, vectorFieldName, query);
      assertEquals(expectedScores.keySet(), actualScores.keySet());
      for (Map.Entry<Integer, Float> entry : expectedScores.entrySet()) {
        assertEquals(
            "score changed for doc " + entry.getKey() + " after dropping raw vectors",
            entry.getValue(),
            actualScores.get(entry.getKey()),
            0f);
      }
    }
  }

  /**
   * Scores every document holding a float16 vector for {@code field} against {@code query}, keyed
   * by doc id. Keying by doc id rather than ordinal keeps the comparison meaningful even when a
   * different {@link Float16VectorValues} implementation backs the iterator.
   */
  private Map<Integer, Float> scoreAllFloat16Docs(Directory dir, String field, short[] query)
      throws IOException {
    Map<Integer, Float> scores = new HashMap<>();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      LeafReader leafReader = getOnlyLeafReader(reader);
      if (leafReader instanceof CodecReader codecReader) {
        KnnVectorsReader knnVectorsReader =
            codecReader.getVectorReader().unwrapReaderForField(field);
        Float16VectorValues float16VectorValues = knnVectorsReader.getFloat16VectorValues(field);
        assertNotNull(float16VectorValues);
        VectorScorer scorer = float16VectorValues.scorer(query);
        assertNotNull(scorer);
        DocIdSetIterator iterator = scorer.iterator();
        for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
          scores.put(doc, scorer.score());
        }
      } else {
        fail("reader is not CodecReader");
      }
    }
    return scores;
  }

  public void testBulkScoreMatchesScore() throws IOException {
    try (Directory dir = new MockDirectoryWrapper(random(), new MMapDirectory(createTempDir()))) {
      assertBulkScoreMatchesScore(dir);
    }
  }

  /** {@code bulkScore} must return exactly the scores of scoring one vector at a time. */
  private void assertBulkScoreMatchesScore(Directory dir) throws IOException {
    // Random dims will also run the native code's tail loops.
    int dims = random().nextInt(1, 300);
    int numDocs = random().nextInt(50, 500);
    VectorSimilarityFunction similarity =
        RandomPicks.randomFrom(random(), VectorSimilarityFunction.values());
    var nibbleFormat = new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.PACKED_NIBBLE);
    try (IndexWriter w =
        new IndexWriter(
            dir, newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(nibbleFormat)))) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        // Some docs have no vector, so vector ordinals and doc ids differ.
        if (random().nextInt(5) != 0) {
          float[] vector =
              similarity == VectorSimilarityFunction.DOT_PRODUCT
                  ? randomNormalizedVector(dims)
                  : randomVector(dims);
          doc.add(new KnnFloatVectorField("v", vector, similarity));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      LeafReader leaf = getOnlyLeafReader(reader);
      SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(leaf);
      FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo("v");
      Lucene104ScalarQuantizedVectorsReader vectorsReader =
          (Lucene104ScalarQuantizedVectorsReader)
              segmentReader.getVectorReader().unwrapReaderForField("v");
      SegmentWriteState writeState =
          new SegmentWriteState(
              InfoStream.getDefault(),
              dir,
              segmentReader.getSegmentInfo().info,
              leaf.getFieldInfos(),
              null,
              IOContext.DEFAULT);
      QuantizedByteVectorValues values = vectorsReader.getQuantizedVectorValues("v");
      int numVectors = values.size();
      UpdateableRandomVectorScorer perVector =
          new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)
              .getRandomVectorScorerSupplier(similarity, values)
              .scorer();

      // The scorer that HNSW graph construction uses during merges.
      try (CloseableRandomVectorScorerSupplier supplier =
          vectorsReader.getRandomVectorScorerSupplierForMerge(fieldInfo, writeState)) {
        UpdateableRandomVectorScorer bulk = supplier.scorer();
        // With the native library, this must be the batching scorer, not a fallback.
        assertSame(
            FlatVectorScorerUtil.getLucene104ScalarQuantizedVectorsScorer().getClass(),
            bulk.getClass().getNestHost());

        int[] nodes = new int[64];
        float[] scores = new float[nodes.length];
        for (int iter = 0; iter < 100; iter++) {
          int query = random().nextInt(numVectors);
          bulk.setScoringOrdinal(query);
          perVector.setScoringOrdinal(query);
          int single = random().nextInt(numVectors);
          assertSameScore(perVector.score(single), bulk.score(single));

          int numNodes = random().nextInt(nodes.length + 1);
          for (int i = 0; i < numNodes; i++) {
            nodes[i] = random().nextInt(numVectors);
          }
          float max = bulk.bulkScore(nodes, scores, numNodes);
          float expectedMax = Float.NEGATIVE_INFINITY;
          for (int i = 0; i < numNodes; i++) {
            float expected = perVector.score(nodes[i]);
            assertSameScore(expected, scores[i]);
            expectedMax = Math.max(expectedMax, expected);
          }
          assertSameScore(expectedMax, max);
          // A batch must not change what score() returns.
          assertSameScore(perVector.score(single), bulk.score(single));
        }
      }
    }
  }

  private static void assertSameScore(float expected, float actual) {
    assertEquals(
        "expected " + expected + " but was " + actual,
        Float.floatToRawIntBits(expected),
        Float.floatToRawIntBits(actual));
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return true;
  }

  @Override
  protected int getQuantizationBits() {
    return encoding.getBits();
  }

  /** Simulates empty raw vectors by modifying index files. */
  @Override
  protected void simulateEmptyRawVectors(Directory dir) throws Exception {
    final String[] indexFiles = dir.listAll();
    final String RAW_VECTOR_EXTENSION = "vec";
    final String VECTOR_META_EXTENSION = "vemf";

    for (String file : indexFiles) {
      if (file.endsWith("." + RAW_VECTOR_EXTENSION)) {
        replaceWithEmptyVectorFile(dir, file);
      } else if (file.endsWith("." + VECTOR_META_EXTENSION)) {
        updateVectorMetadataFile(dir, file);
      }
    }
  }

  /** Replaces a raw vector file with an empty one that has valid header/footer. */
  private void replaceWithEmptyVectorFile(Directory dir, String fileName) throws Exception {
    byte[] indexHeader;
    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
      indexHeader = CodecUtil.readIndexHeader(in);
    }
    dir.deleteFile(fileName);
    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
      // Write header
      out.writeBytes(indexHeader, 0, indexHeader.length);
      // Write footer (no content in between)
      CodecUtil.writeFooter(out);
    }
  }

  /** Updates vector metadata file to indicate zero vector length. */
  private void updateVectorMetadataFile(Directory dir, String fileName) throws Exception {
    // Read original metadata
    byte[] indexHeader;
    int fieldNumber, vectorEncoding, vectorSimilarityFunction, dimension;
    long vectorStartPos;

    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
      indexHeader = CodecUtil.readIndexHeader(in);
      fieldNumber = in.readInt();
      vectorEncoding = in.readInt();
      vectorSimilarityFunction = in.readInt();
      vectorStartPos = in.readVLong();
      in.readVLong(); // Skip original vector length
      dimension = in.readVInt();
    }

    // Create updated metadata file
    dir.deleteFile(fileName);
    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
      // Write header
      out.writeBytes(indexHeader, 0, indexHeader.length);

      // Write metadata with zero vector length
      out.writeInt(fieldNumber);
      out.writeInt(vectorEncoding);
      out.writeInt(vectorSimilarityFunction);
      out.writeVLong(vectorStartPos);
      out.writeVLong(0); // Set vector length to 0
      out.writeVInt(dimension);
      out.writeInt(0);

      // Write configuration
      OrdToDocDISIReaderConfiguration.writeStoredMeta(
          DIRECT_MONOTONIC_BLOCK_SHIFT, out, null, 0, 0, null);

      // Mark end of fields and write footer
      out.writeInt(-1);
      CodecUtil.writeFooter(out);
    }
  }
}
