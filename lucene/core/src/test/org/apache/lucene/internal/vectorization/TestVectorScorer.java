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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.junit.BeforeClass;

public class TestVectorScorer extends LuceneTestCase {

  private static final double DELTA = 1e-5;

  static final FlatVectorsScorer DEFAULT_SCORER = DefaultFlatVectorScorer.INSTANCE;
  static final FlatVectorsScorer MEMSEG_SCORER =
      VectorizationProvider.lookup(true).getLucene99FlatVectorsScorer();

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeTrue(
        "Test only works when the Memory segment scorer is present.",
        MEMSEG_SCORER.getClass() != DEFAULT_SCORER.getClass());
  }

  public void testSimpleScorer() throws IOException {
    testSimpleScorer(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
  }

  public void testSimpleScorerSmallChunkSize() throws IOException {
    long maxChunkSize = TestUtil.nextLong(random(), 4, 16);
    testSimpleScorer(maxChunkSize);
  }

  public void testSimpleScorerMedChunkSize() throws IOException {
    // a chunk size where in some vectors will be copied on-heap, while others remain off-heap
    testSimpleScorer(64);
  }

  void testSimpleScorer(long maxChunkSize) throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testSimpleScorer"), maxChunkSize)) {
      for (int dims : List.of(31, 32, 33)) {
        // dimensions that, in some scenarios, cross the mmap chunk sizes
        byte[][] vectors = new byte[2][dims];
        String fileName = "bar-" + dims;
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
          for (int i = 0; i < dims; i++) {
            vectors[0][i] = (byte) i;
            vectors[1][i] = (byte) (dims - i);
          }
          byte[] bytes = concat(vectors[0], vectors[1]);
          out.writeBytes(bytes, 0, bytes.length);
        }
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            var vectorValues = vectorValues(dims, 2, in, sim);
            for (var ords : List.of(List.of(0, 1), List.of(1, 0))) {
              int idx0 = ords.get(0);
              int idx1 = ords.get(1);

              // getRandomVectorScorerSupplier
              var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              float expected = scorer1.scorer(idx0).score(idx1);
              var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              assertEquals(scorer2.scorer(idx0).score(idx1), expected, DELTA);

              // getRandomVectorScorer
              var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
              assertEquals(scorer3.score(idx1), expected, DELTA);
              var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
              assertEquals(scorer4.score(idx1), expected, DELTA);
            }
          }
        }
      }
    }
  }

  public void testRandomScorer() throws IOException {
    testRandomScorer(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_RANDOM_FUNC);
  }

  public void testRandomScorerMax() throws IOException {
    testRandomScorer(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MAX_FUNC);
  }

  public void testRandomScorerMin() throws IOException {
    testRandomScorer(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MIN_FUNC);
  }

  public void testRandomSmallChunkSize() throws IOException {
    long maxChunkSize = randomLongBetween(32, 128);
    testRandomScorer(maxChunkSize, BYTE_ARRAY_RANDOM_FUNC);
  }

  void testRandomScorer(long maxChunkSize, Function<Integer, byte[]> byteArraySupplier)
      throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testRandomScorer"), maxChunkSize)) {
      final int dims = randomIntBetween(1, 4096);
      final int size = randomIntBetween(2, 100);
      final byte[][] vectors = new byte[size][];
      String fileName = "foo-" + dims;
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (int i = 0; i < size; i++) {
          var vec = byteArraySupplier.apply(dims);
          out.writeBytes(vec, 0, vec.length);
          vectors[i] = vec;
        }
      }

      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        for (int times = 0; times < TIMES; times++) {
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            var vectorValues = vectorValues(dims, size, in, sim);
            int idx0 = randomIntBetween(0, size - 1);
            int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.

            // getRandomVectorScorerSupplier
            var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            float expected = scorer1.scorer(idx0).score(idx1);
            var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            assertEquals(scorer2.scorer(idx0).score(idx1), expected, DELTA);

            // getRandomVectorScorer
            var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertEquals(scorer3.score(idx1), expected, DELTA);
            var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertEquals(scorer4.score(idx1), expected, DELTA);
          }
        }
      }
    }
  }

  public void testRandomSliceSmall() throws IOException {
    testRandomSliceImpl(30, 64, 1, BYTE_ARRAY_RANDOM_FUNC);
  }

  public void testRandomSlice() throws IOException {
    int dims = randomIntBetween(1, 4096);
    long maxChunkSize = randomLongBetween(32, 128);
    int initialOffset = randomIntBetween(1, 129);
    testRandomSliceImpl(dims, maxChunkSize, initialOffset, BYTE_ARRAY_RANDOM_FUNC);
  }

  // Tests with a slice that has a non-zero initial offset
  void testRandomSliceImpl(
      int dims, long maxChunkSize, int initialOffset, Function<Integer, byte[]> byteArraySupplier)
      throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testRandomSliceImpl"), maxChunkSize)) {
      final int size = randomIntBetween(2, 100);
      final byte[][] vectors = new byte[size][];
      String fileName = "baz-" + dims;
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        byte[] ba = new byte[initialOffset];
        out.writeBytes(ba, 0, ba.length);
        for (int i = 0; i < size; i++) {
          var vec = byteArraySupplier.apply(dims);
          out.writeBytes(vec, 0, vec.length);
          vectors[i] = vec;
        }
      }

      try (var outter = dir.openInput(fileName, IOContext.DEFAULT);
          var in = outter.slice("slice", initialOffset, outter.length() - initialOffset)) {
        for (int times = 0; times < TIMES; times++) {
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            var vectorValues = vectorValues(dims, size, in, sim);
            int idx0 = randomIntBetween(0, size - 1);
            int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.

            // getRandomVectorScorerSupplier
            var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            float expected = scorer1.scorer(idx0).score(idx1);
            var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            assertEquals(scorer2.scorer(idx0).score(idx1), expected, DELTA);

            // getRandomVectorScorer
            var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertEquals(scorer3.score(idx1), expected, DELTA);
            var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertEquals(scorer4.score(idx1), expected, DELTA);
          }
        }
      }
    }
  }

  // Tests that copies in threads do not interfere with each other
  public void testCopiesAcrossThreads() throws Exception {
    final long maxChunkSize = 32;
    final int dims = 34; // dimensions that are larger than the chunk size, to force fallback
    byte[] vec1 = new byte[dims];
    byte[] vec2 = new byte[dims];
    IntStream.range(0, dims).forEach(i -> vec1[i] = 1);
    IntStream.range(0, dims).forEach(i -> vec2[i] = 2);
    try (Directory dir = new MMapDirectory(createTempDir("testRace"), maxChunkSize)) {
      String fileName = "biz-" + dims;
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        byte[] bytes = concat(vec1, vec1, vec2, vec2);
        out.writeBytes(bytes, 0, bytes.length);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
          var vectorValues = vectorValues(dims, 4, in, sim);
          var scoreSupplier = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
          var expectedScore1 = scoreSupplier.scorer(0).score(1);
          var expectedScore2 = scoreSupplier.scorer(2).score(3);

          var scorer = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
          var tasks =
              List.<Callable<Optional<Throwable>>>of(
                  new AssertingScoreCallable(scorer.copy().scorer(0), 1, expectedScore1),
                  new AssertingScoreCallable(scorer.copy().scorer(2), 3, expectedScore2));
          var executor = Executors.newFixedThreadPool(2, new NamedThreadFactory("copiesThreads"));
          var results = executor.invokeAll(tasks);
          executor.shutdown();
          assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
          assertEquals(results.stream().filter(Predicate.not(Future::isDone)).count(), 0L);
          for (var res : results) {
            assertTrue("Unexpected exception" + res.get(), res.get().isEmpty());
          }
        }
      }
    }
  }

  // A callable that scores the given ord and scorer and asserts the expected result.
  static class AssertingScoreCallable implements Callable<Optional<Throwable>> {
    final RandomVectorScorer scorer;
    final int ord;
    final float expectedScore;

    AssertingScoreCallable(RandomVectorScorer scorer, int ord, float expectedScore) {
      this.scorer = scorer;
      this.ord = ord;
      this.expectedScore = expectedScore;
    }

    @Override
    public Optional<Throwable> call() throws Exception {
      try {
        for (int i = 0; i < 100; i++) {
          assertEquals(scorer.score(ord), expectedScore, DELTA);
        }
      } catch (Throwable t) {
        return Optional.of(t);
      }
      return Optional.empty();
    }
  }

  // Tests with a large amount of data (> 2GB), which ensures that data offsets do not overflow
  @Nightly
  public void testLarge() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir("testLarge"))) {
      final int dims = 8192;
      final int size = 262500;
      final String fileName = "large-" + dims;
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (int i = 0; i < size; i++) {
          var vec = vector(i, dims);
          out.writeBytes(vec, 0, vec.length);
        }
      }

      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        assert in.length() > Integer.MAX_VALUE;
        for (int times = 0; times < TIMES; times++) {
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            var vectorValues = vectorValues(dims, size, in, sim);
            int ord1 = randomIntBetween(0, size - 1);
            int ord2 = size - 1;
            for (var ords : List.of(List.of(ord1, ord2), List.of(ord2, ord1))) {
              int idx0 = ords.get(0);
              int idx1 = ords.get(1);

              // getRandomVectorScorerSupplier
              var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              float expected = scorer1.scorer(idx0).score(idx1);
              var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              assertEquals(scorer2.scorer(idx0).score(idx1), expected, DELTA);

              // getRandomVectorScorer
              var query = vector(idx0, dims);
              var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, query);
              assertEquals(scorer3.score(idx1), expected, DELTA);
              var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, query);
              assertEquals(scorer4.score(idx1), expected, DELTA);
            }
          }
        }
      }
    }
  }

  RandomAccessVectorValues vectorValues(
      int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("byteValues", 0, in.length()), dims, MEMSEG_SCORER, sim);
  }

  // creates the vector based on the given ordinal, which is reproducible given the ord and dims
  static byte[] vector(int ord, int dims) {
    var random = new Random(Objects.hash(ord, dims));
    byte[] ba = new byte[dims];
    for (int i = 0; i < dims; i++) {
      ba[i] = (byte) RandomNumbers.randomIntBetween(random, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
    return ba;
  }

  /** Concatenates byte arrays. */
  static byte[] concat(byte[]... arrays) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (var ba : arrays) {
        baos.write(ba);
      }
      return baos.toByteArray();
    }
  }

  static int randomIntBetween(int minInclusive, int maxInclusive) {
    return RandomNumbers.randomIntBetween(random(), minInclusive, maxInclusive);
  }

  static long randomLongBetween(long minInclusive, long maxInclusive) {
    return RandomNumbers.randomLongBetween(random(), minInclusive, maxInclusive);
  }

  static Function<Integer, byte[]> BYTE_ARRAY_RANDOM_FUNC =
      size -> {
        byte[] ba = new byte[size];
        for (int i = 0; i < size; i++) {
          ba[i] = (byte) random().nextInt();
        }
        return ba;
      };

  static Function<Integer, byte[]> BYTE_ARRAY_MAX_FUNC =
      size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, Byte.MAX_VALUE);
        return ba;
      };

  static Function<Integer, byte[]> BYTE_ARRAY_MIN_FUNC =
      size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, Byte.MIN_VALUE);
        return ba;
      };

  static final int TIMES = 100; // a loop iteration times
}
