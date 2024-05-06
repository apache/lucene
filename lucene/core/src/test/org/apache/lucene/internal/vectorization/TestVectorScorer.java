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
import static org.hamcrest.Matchers.equalTo;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.junit.BeforeClass;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10)
public class TestVectorScorer extends LuceneTestCase {

  static final FlatVectorsScorer DEFAULT_SCORER = new DefaultFlatVectorScorer();
  static final FlatVectorsScorer MEMSEG_SCORER =
      VectorizationProvider.lookup(true).newFlatVectorScorer();

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
    long maxChunkSize = random().nextLong(4, 16);
    testSimpleScorer(maxChunkSize);
  }

  public void testSimpleScorerMedChunkSize() throws IOException {
    // a chunk size where in some vectors will be copied on-heap, while others remain off-heap
    testSimpleScorer(64);
  }

  void testSimpleScorer(long maxChunkSize) throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir(getTestName()), maxChunkSize)) {
      for (int dims : List.of(31, 32, 33)) {
        // System.out.println("testing with dim=" + dims);
        // dimensions that, in some scenarios, cross the mmap chunk sizes
        byte[][] vectors = new byte[2][dims];
        String fileName = getTestName() + "-" + dims;
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
          for (int i = 0; i < dims; i++) {
            vectors[0][i] = (byte) i;
            vectors[1][i] = (byte) (dims - i);
          }
          byte[] bytes = concat(vectors[0], vectors[1]);
          out.writeBytes(bytes, 0, bytes.length);
        }
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
          var vectorValues = vectorValues(dims, 2, in);
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            for (var ords : List.of(List.of(0, 1), List.of(1, 0))) {
              int idx0 = ords.get(0);
              int idx1 = ords.get(1);

              // getRandomVectorScorerSupplier
              var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              float expected = scorer1.scorer(idx0).score(idx1);
              var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
              assertThat(scorer2.scorer(idx0).score(idx1), equalTo(expected));

              // getRandomVectorScorer
              var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
              assertThat(scorer3.score(idx1), equalTo(expected));
              var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
              assertThat(scorer4.score(idx1), equalTo(expected));
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
    try (Directory dir = new MMapDirectory(createTempDir(getTestName()), maxChunkSize)) {
      final int dims = randomIntBetween(1, 4096);
      final int size = randomIntBetween(2, 100);
      final byte[][] vectors = new byte[size][];
      String fileName = getTestName() + "-" + dims;
      // System.out.println("Testing, maxChunkSize=" + maxChunkSize + ",fn=" + fileName);
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (int i = 0; i < size; i++) {
          var vec = byteArraySupplier.apply(dims);
          out.writeBytes(vec, 0, vec.length);
          vectors[i] = vec;
        }
      }

      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        var vectorValues = vectorValues(dims, size, in);
        for (int times = 0; times < TIMES; times++) {
          for (var sim : List.of(COSINE, EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT)) {
            int idx0 = randomIntBetween(0, size - 1);
            int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.

            // getRandomVectorScorerSupplier
            var scorer1 = DEFAULT_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            float expected = scorer1.scorer(idx0).score(idx1);
            var scorer2 = MEMSEG_SCORER.getRandomVectorScorerSupplier(sim, vectorValues);
            assertThat(scorer2.scorer(idx0).score(idx1), equalTo(expected));

            // getRandomVectorScorer
            var scorer3 = DEFAULT_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertThat(scorer3.score(idx1), equalTo(expected));
            var scorer4 = MEMSEG_SCORER.getRandomVectorScorer(sim, vectorValues, vectors[idx0]);
            assertThat(scorer4.score(idx1), equalTo(expected));
          }
        }
      }
    }
  }

  // TODO: add initial offset tests

  static RandomAccessVectorValues vectorValues(int dims, int size, IndexInput in)
      throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("test", 0, in.length()), dims);
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
