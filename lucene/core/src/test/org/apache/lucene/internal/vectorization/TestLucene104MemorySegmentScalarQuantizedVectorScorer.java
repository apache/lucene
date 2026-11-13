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
package org.apache.lucene.internal.vectorization;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.junit.BeforeClass;

/**
 * Checks that the memory segment scorer of the Lucene104 scalar quantized format gives the same
 * scores as the default scorer, for every encoding, similarity, and scoring path.
 */
public class TestLucene104MemorySegmentScalarQuantizedVectorScorer extends LuceneTestCase {

  // The reference scorer reads vectors on heap and scores them through VectorUtil. With a
  // tests.vectorsize set, which the build picks at random, VectorUtil is the scalar
  // DefaultVectorUtilSupport; otherwise it is the Panama single vector kernels, so that the two
  // scorers are checked against each other.
  private static final Lucene104ScalarQuantizedVectorScorer DEFAULT_SCORER =
      new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE);
  private static final Lucene104ScalarQuantizedVectorScorer MEMSEG_SCORER =
      VectorizationProvider.lookup(true).getLucene104ScalarQuantizedVectorsScorer();

  // dimensions around the widths of the vector kernels and their tails
  private static final int[] DIMENSIONS = {
    1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512,
    513, 1024, 1025
  };

  @BeforeClass
  public static void beforeClass() {
    assumeTrue(
        "Test only works when the memory segment scorer is present.",
        MEMSEG_SCORER.getClass() != DEFAULT_SCORER.getClass());
  }

  public void testAllEncodingsAndSimilarities() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testAll"))) {
      for (ScalarEncoding encoding : ScalarEncoding.values()) {
        for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
          for (int dims : DIMENSIONS) {
            try (Vectors vectors =
                new Vectors(dir, encoding, similarity, dims, random().nextInt(1, 12))) {
              assertSameScores(vectors, true);
            }
          }
        }
      }
    }
  }

  public void testRandom() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testRandom"))) {
      for (int iter = 0; iter < 20; iter++) {
        try (Vectors vectors =
            new Vectors(
                dir,
                RandomPicks.randomFrom(random(), ScalarEncoding.values()),
                RandomPicks.randomFrom(random(), VectorSimilarityFunction.values()),
                random().nextInt(1, 2000),
                random().nextInt(1, 100))) {
          assertSameScores(vectors, true);
        }
      }
    }
  }

  /** Vectors that span mapped chunks are scored by the default scorer, with the same result. */
  public void testSmallChunkSize() throws IOException {
    long maxChunkSize = random().nextLong(32, 4096);
    try (Directory dir = new MMapDirectory(createTempDir("testSmallChunk"), maxChunkSize)) {
      for (int iter = 0; iter < 10; iter++) {
        try (Vectors vectors =
            new Vectors(
                dir,
                RandomPicks.randomFrom(random(), ScalarEncoding.values()),
                RandomPicks.randomFrom(random(), VectorSimilarityFunction.values()),
                random().nextInt(1, 300),
                random().nextInt(1, 50))) {
          // records that fit in one chunk (a power of two) are still scored off heap
          long recordBytes = (long) vectors.values.getVectorByteLength() + 16;
          long chunkSize = Long.highestOneBit(maxChunkSize);
          assertSameScores(vectors, recordBytes * vectors.size <= chunkSize);
        }
      }
    }
  }

  public void testIllegalOrdinal() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testIllegalOrdinal"))) {
      try (Vectors vectors =
          new Vectors(
              dir,
              RandomPicks.randomFrom(random(), ScalarEncoding.values()),
              VectorSimilarityFunction.DOT_PRODUCT,
              random().nextInt(1, 100),
              random().nextInt(1, 10))) {
        RandomVectorScorer scorer =
            MEMSEG_SCORER.getRandomVectorScorer(
                vectors.similarity, vectors.values, vectors.randomQuery());
        float[] scores = new float[2];
        // the same exception as the Lucene99 memory segment scorers; the default scorer differs
        for (int ord : new int[] {-1, vectors.size, vectors.size + random().nextInt(100)}) {
          expectThrows(IllegalArgumentException.class, () -> scorer.score(ord));
          expectThrows(
              IllegalArgumentException.class,
              () -> scorer.bulkScore(new int[] {0, ord}, scores, 2));
          expectThrows(
              IllegalArgumentException.class,
              () -> scorer.bulkScore(new int[] {0, 0, 0, 0, ord}, new float[5], 5));
        }
      }
    }
  }

  /**
   * Vectors with the largest quantized values, at lengths around the accumulator chunks: the 4 bit
   * kernel sums 16 bit lanes over chunks of 1024, 2048 or 4096 packed bytes (2048, 4096 or 8192
   * dims) for 128, 256 or 512 bit vectors, and with all values at 15 a lane reaches 57600 of 65535
   * at the end of a chunk. A larger chunk, or one more product per step, overflows and fails here.
   *
   * <p>Only the bulk path of one query is checked: the vectors are long and the kernels are slow
   * when the test JVM does not compile the Vector API (the other tests cover the paths).
   */
  public void testSaturatedValues() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testSaturated"))) {
      for (ScalarEncoding encoding : ScalarEncoding.values()) {
        // one less, exactly, and one more packed byte than one and two chunks, at each width
        for (int dims :
            new int[] {
              31, 1024, 2046, 2048, 2050, 4094, 4096, 4098, 8190, 8192, 8194, 8320, 8334, 16384,
              16398
            }) {
          try (Vectors vectors =
              new Vectors(
                  dir,
                  encoding,
                  RandomPicks.randomFrom(random(), VectorSimilarityFunction.values()),
                  dims,
                  5,
                  true)) {
            float[] query = vectors.randomQuery();
            RandomVectorScorer expected =
                DEFAULT_SCORER.getRandomVectorScorer(
                    vectors.similarity, vectors.values, query.clone());
            RandomVectorScorer scorer =
                MEMSEG_SCORER.getRandomVectorScorer(
                    vectors.similarity, vectors.values, query.clone());
            assertTrue(scorer.getClass().getName().contains("MemorySegment"));
            // a full pass of the kernel and a tail, then two full passes
            assertSameBulkScores(vectors.toString(), expected, scorer, vectors.size, vectors.size);
            assertSameBulkScores(vectors.toString(), expected, scorer, vectors.size, 8);
          }
        }
      }
    }
  }

  public void testQueryLengthIsChecked() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testQueryLength"))) {
      for (ScalarEncoding encoding : ScalarEncoding.values()) {
        try (Vectors vectors =
            new Vectors(dir, encoding, VectorSimilarityFunction.DOT_PRODUCT, 64, 3)) {
          // a query vector of another dimension is refused before reaching the kernels
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  MEMSEG_SCORER.getRandomVectorScorer(
                      vectors.similarity, vectors.values, new float[65]));
        }
      }
    }
  }

  public void testShortInputIsRejected() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testShortInput"))) {
      try (Vectors vectors =
          new Vectors(
              dir, ScalarEncoding.UNSIGNED_BYTE, VectorSimilarityFunction.DOT_PRODUCT, 16, 4)) {
        // claim one record more than the slice holds
        QuantizedByteVectorValues values = vectors.values(vectors.size + 1);
        expectThrows(
            IllegalArgumentException.class,
            () -> MEMSEG_SCORER.getRandomVectorScorer(vectors.similarity, values, new float[16]));
      }
    }
  }

  public void testScoringOrdinalMustBeSet() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testOrdinalNotSet"))) {
      for (ScalarEncoding encoding : ScalarEncoding.values()) {
        try (Vectors vectors =
            new Vectors(dir, encoding, VectorSimilarityFunction.DOT_PRODUCT, 32, 3)) {
          UpdateableRandomVectorScorer scorer = vectors.supplier(MEMSEG_SCORER).scorer();
          expectThrows(IllegalStateException.class, () -> scorer.score(0));
          expectThrows(
              IllegalStateException.class, () -> scorer.bulkScore(new int[] {0}, new float[1], 1));
        }
      }
    }
  }

  /** Values without records have nothing to score, but a query scorer can still be made. */
  public void testNoVectors() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testNoVectors"))) {
      try (Vectors vectors =
          new Vectors(
              dir,
              RandomPicks.randomFrom(random(), ScalarEncoding.values()),
              VectorSimilarityFunction.DOT_PRODUCT,
              random().nextInt(1, 100),
              1)) {
        RandomVectorScorer scorer =
            MEMSEG_SCORER.getRandomVectorScorer(
                vectors.similarity, vectors.values(0), vectors.randomQuery());
        assertTrue(scorer.getClass().getName().contains("MemorySegment"));
        assertEquals(Float.NEGATIVE_INFINITY, scorer.bulkScore(new int[0], new float[0], 0), 0f);
        expectThrows(IllegalArgumentException.class, () -> scorer.score(0));
      }
    }
  }

  /** Copies of a supplier do not share scoring state. */
  public void testCopiesAcrossThreads() throws Exception {
    try (Directory dir = new MMapDirectory(createTempDir("testCopies"))) {
      for (ScalarEncoding encoding : ScalarEncoding.values()) {
        try (Vectors vectors =
            new Vectors(
                dir,
                encoding,
                RandomPicks.randomFrom(random(), VectorSimilarityFunction.values()),
                random().nextInt(1, 300),
                4)) {
          RandomVectorScorerSupplier expected = vectors.supplier(DEFAULT_SCORER);
          RandomVectorScorerSupplier supplier = vectors.supplier(MEMSEG_SCORER);
          List<Callable<Optional<Throwable>>> tasks =
              List.of(
                  new AssertingScoreCallable(expected.copy(), supplier.copy(), 0, 1),
                  new AssertingScoreCallable(expected.copy(), supplier.copy(), 2, 3),
                  new AssertingScoreCallable(expected.copy(), supplier.copy(), 1, 1));
          var executor = Executors.newFixedThreadPool(3, new NamedThreadFactory("copiesThreads"));
          var results = executor.invokeAll(tasks);
          executor.shutdown();
          assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
          for (var result : results) {
            assertTrue("Unexpected exception " + result.get(), result.get().isEmpty());
          }
        }
      }
    }
  }

  private record AssertingScoreCallable(
      RandomVectorScorerSupplier expectedSupplier,
      RandomVectorScorerSupplier supplier,
      int target,
      int ord)
      implements Callable<Optional<Throwable>> {
    @Override
    public Optional<Throwable> call() throws Exception {
      UpdateableRandomVectorScorer expected = expectedSupplier.scorer();
      expected.setScoringOrdinal(target);
      UpdateableRandomVectorScorer scorer = supplier.scorer();
      scorer.setScoringOrdinal(target);
      try {
        int[] nodes = {ord, 0, 1, 2, 3, ord};
        float[] expectedScores = new float[nodes.length];
        float[] scores = new float[nodes.length];
        for (int i = 0; i < 100; i++) {
          assertEquals(expected.score(ord), scorer.score(ord), 0f);
          assertEquals(
              expected.bulkScore(nodes, expectedScores, nodes.length),
              scorer.bulkScore(nodes, scores, nodes.length),
              0f);
          assertArrayEquals(expectedScores, scores, 0f);
        }
      } catch (Throwable t) {
        return Optional.of(t);
      }
      return Optional.empty();
    }
  }

  /** Compares every scoring path of the two scorers, exactly. */
  private void assertSameScores(Vectors vectors, boolean expectOffHeap) throws IOException {
    // float and float16 queries
    for (int q = 0; q < 2; q++) {
      float[] query = vectors.randomQuery();
      RandomVectorScorer expected =
          DEFAULT_SCORER.getRandomVectorScorer(vectors.similarity, vectors.values, query.clone());
      RandomVectorScorer scorer =
          MEMSEG_SCORER.getRandomVectorScorer(vectors.similarity, vectors.values, query.clone());
      assertEquals(expectOffHeap, scorer.getClass().getName().contains("MemorySegment"));
      assertSameScores(vectors + " float query", expected, scorer, vectors.size);
      short[] query16 = new short[query.length];
      for (int i = 0; i < query.length; i++) {
        query16[i] = Float.floatToFloat16(query[i]);
      }
      expected = DEFAULT_SCORER.getRandomVectorScorer(vectors.similarity, vectors.values, query16);
      scorer = MEMSEG_SCORER.getRandomVectorScorer(vectors.similarity, vectors.values, query16);
      assertSameScores(vectors + " float16 query", expected, scorer, vectors.size);
    }
    // vectors as queries, as when building a graph; also through a copy of the supplier
    UpdateableRandomVectorScorer expected = vectors.supplier(DEFAULT_SCORER).scorer();
    RandomVectorScorerSupplier supplier = vectors.supplier(MEMSEG_SCORER);
    UpdateableRandomVectorScorer scorer =
        random().nextBoolean() ? supplier.scorer() : supplier.copy().scorer();
    assertEquals(expectOffHeap, scorer.getClass().getName().contains("MemorySegment"));
    for (int q = 0; q < 2; q++) {
      int target = random().nextInt(vectors.size);
      expected.setScoringOrdinal(target);
      scorer.setScoringOrdinal(target);
      assertSameScores(vectors + " ordinal " + target, expected, scorer, vectors.size);
    }
  }

  private static void assertSameScores(
      String context, RandomVectorScorer expected, RandomVectorScorer scorer, int size)
      throws IOException {
    for (int ord = 0; ord < size; ord++) {
      assertEquals(context + " ord " + ord, expected.score(ord), scorer.score(ord), 0f);
    }
    // batches of every size up to a neighbor list, with repeated ordinals, and a batch shorter
    // than its arrays
    for (int numNodes = 0; numNodes <= 13; numNodes++) {
      assertSameBulkScores(context, expected, scorer, size, numNodes);
    }
    assertSameBulkScores(context, expected, scorer, size, 64 + random().nextInt(4));
  }

  private static void assertSameBulkScores(
      String context,
      RandomVectorScorer expected,
      RandomVectorScorer scorer,
      int size,
      int numNodes)
      throws IOException {
    int[] nodes = new int[numNodes + random().nextInt(3)];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = random().nextInt(size);
    }
    float[] expectedScores = new float[nodes.length];
    float[] scores = new float[nodes.length];
    float expectedMax = expected.bulkScore(nodes, expectedScores, numNodes);
    float max = scorer.bulkScore(nodes, scores, numNodes);
    assertEquals(context, expectedMax, max, 0f);
    for (int i = 0; i < numNodes; i++) {
      assertEquals(context + " node " + i + " of " + numNodes, expectedScores[i], scores[i], 0f);
    }
  }

  /** Quantized vectors written like the Lucene104 format writes them. */
  private static final class Vectors implements Closeable {
    private static final AtomicInteger fileCount = new AtomicInteger();
    final List<IndexInput> inputs = new ArrayList<>();

    final ScalarEncoding encoding;
    final VectorSimilarityFunction similarity;
    final int dims;
    final int size;
    final OptimizedScalarQuantizer quantizer;
    final float[] centroid;
    final float centroidDp;
    final QuantizedByteVectorValues values;

    /** The same vectors quantized to the query bits of an asymmetric encoding, else null. */
    final QuantizedByteVectorValues queryValues;

    /** Whether every quantized value is the largest one of the encoding, instead of quantized. */
    final boolean saturated;

    Vectors(
        Directory dir,
        ScalarEncoding encoding,
        VectorSimilarityFunction similarity,
        int dims,
        int size)
        throws IOException {
      this(dir, encoding, similarity, dims, size, false);
    }

    Vectors(
        Directory dir,
        ScalarEncoding encoding,
        VectorSimilarityFunction similarity,
        int dims,
        int size,
        boolean saturated)
        throws IOException {
      this.encoding = encoding;
      this.similarity = similarity;
      this.dims = dims;
      this.size = size;
      this.saturated = saturated;
      this.quantizer = new OptimizedScalarQuantizer(similarity);
      float[][] vectors = new float[size][];
      centroid = new float[dims];
      for (int i = 0; i < size; i++) {
        vectors[i] = randomVector();
        for (int d = 0; d < dims; d++) {
          centroid[d] += vectors[i][d] / size;
        }
      }
      if (similarity == VectorSimilarityFunction.COSINE) {
        VectorUtil.l2normalize(centroid);
      }
      centroidDp = VectorUtil.dotProduct(centroid, centroid);
      values = write(dir, vectors, false);
      queryValues = encoding.isAsymmetric() ? write(dir, vectors, true) : null;
    }

    float[] randomQuery() {
      return randomVector();
    }

    private float[] randomVector() {
      float[] vector = new float[dims];
      for (int d = 0; d < dims; d++) {
        vector[d] = random().nextFloat(-1, 1);
      }
      if (similarity == VectorSimilarityFunction.COSINE) {
        // keep the centroid away from zero, so that it can be normalized too
        vector[0] = Math.abs(vector[0]) + 1;
        VectorUtil.l2normalize(vector);
      }
      return vector;
    }

    @Override
    public String toString() {
      return encoding + " " + similarity + " dims=" + dims + " size=" + size;
    }

    RandomVectorScorerSupplier supplier(Lucene104ScalarQuantizedVectorScorer scorer)
        throws IOException {
      return encoding.isAsymmetric()
          ? scorer.getRandomVectorScorerSupplier(similarity, queryValues, values)
          : scorer.getRandomVectorScorerSupplier(similarity, values);
    }

    /** The written vectors as values that claim {@code size} records. */
    QuantizedByteVectorValues values(int size) throws IOException {
      IndexInput in = inputs.get(0);
      return new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
          false,
          dims,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarity,
          DEFAULT_SCORER,
          in.slice("vectors", 0, in.length()));
    }

    private QuantizedByteVectorValues write(Directory dir, float[][] vectors, boolean querySide)
        throws IOException {
      String fileName = "vectors-" + fileCount.getAndIncrement();
      byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];
      byte[] packed =
          new byte
              [querySide ? encoding.getQueryPackedLength(dims) : encoding.getDocPackedLength(dims)];
      byte bits = querySide ? encoding.getQueryBits() : encoding.getBits();
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (float[] vector : vectors) {
          QuantizationResult corrections =
              quantizer.scalarQuantize(ArrayUtil.copyArray(vector), scratch, bits, centroid);
          if (saturated) {
            Arrays.fill(scratch, 0, dims, (byte) ((1 << bits) - 1));
          }
          if (querySide) {
            OptimizedScalarQuantizer.transposeHalfByte(scratch, packed);
          } else {
            switch (encoding) {
              case UNSIGNED_BYTE, SEVEN_BIT ->
                  System.arraycopy(scratch, 0, packed, 0, packed.length);
              case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, packed);
              case SINGLE_BIT_QUERY_NIBBLE ->
                  OptimizedScalarQuantizer.packAsBinary(scratch, packed);
              case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(scratch, packed);
            }
          }
          out.writeBytes(packed, packed.length);
          out.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
          out.writeInt(Float.floatToIntBits(corrections.upperInterval()));
          out.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
          out.writeInt(corrections.quantizedComponentSum());
        }
      }
      IndexInput in = dir.openInput(fileName, IOContext.DEFAULT);
      inputs.add(in);
      return new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
          querySide,
          dims,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarity,
          DEFAULT_SCORER,
          in.slice("vectors", 0, in.length()));
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(inputs);
    }
  }
}
