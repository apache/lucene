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
package org.apache.lucene.codecs.hnsw;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.vectorization.BaseVectorizationTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;

public class TestFlatVectorScorer extends BaseVectorizationTestCase {

  private static final AtomicInteger count = new AtomicInteger();
  private final FlatVectorsScorer flatVectorsScorer;
  private final IOSupplier<Directory> newDirectory;

  public TestFlatVectorScorer(
      FlatVectorsScorer flatVectorsScorer, IOSupplier<Directory> newDirectory) {
    this.flatVectorsScorer = flatVectorsScorer;
    this.newDirectory = newDirectory;
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    var scorers =
        List.of(
            DefaultFlatVectorScorer.INSTANCE,
            new Lucene99ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer()),
            FlatVectorScorerUtil.getLucene99FlatVectorsScorer(),
            maybePanamaProvider().getLucene99FlatVectorsScorer());
    var dirs =
        List.<IOSupplier<Directory>>of(
            TestFlatVectorScorer::newDirectory,
            () -> new MMapDirectory(createTempDir(count.getAndIncrement() + "-")),
            () -> new MMapDirectory(createTempDir(count.getAndIncrement() + "-"), 1024));

    List<Object[]> objs = new ArrayList<>();
    for (var scorer : scorers) {
      for (var dir : dirs) {
        objs.add(new Object[] {scorer, dir});
      }
    }
    return objs;
  }

  public void testDefaultOrMemSegScorer() {
    var scorer = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();
    assertThat(
        scorer.toString(),
        is(oneOf("DefaultFlatVectorScorer()", "Lucene99MemorySegmentFlatVectorsScorer()")));
  }

  // Tests that the creation of another scorer does not disturb previous scorers
  public void testMultipleByteScorers() throws IOException {
    byte[] vec0 = new byte[] {0, 0, 0, 0};
    byte[] vec1 = new byte[] {1, 1, 1, 1};
    byte[] vec2 = new byte[] {15, 15, 15, 15};

    String fileName = "testMultipleByteScorers";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(concat(vec0, vec1, vec2), 0, vec0.length * 3);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        var vectorValues = byteVectorValues(4, 3, in, EUCLIDEAN);
        var ss = flatVectorsScorer.getRandomVectorScorerSupplier(EUCLIDEAN, vectorValues);

        var scorerAgainstOrd0 = ss.scorer();
        scorerAgainstOrd0.setScoringOrdinal(0);
        var firstScore = scorerAgainstOrd0.score(1);
        @SuppressWarnings("unused")
        var scorerAgainstOrd2 = ss.scorer();
        scorerAgainstOrd2.setScoringOrdinal(2);
        var scoreAgain = scorerAgainstOrd0.score(1);

        assertThat(scoreAgain, equalTo(firstScore));
      }
    }
  }

  // Tests that the creation of another scorer does not perturb previous scorers
  public void testMultipleFloatScorers() throws IOException {
    float[] vec0 = new float[] {0, 0, 0, 0};
    float[] vec1 = new float[] {1, 1, 1, 1};
    float[] vec2 = new float[] {15, 15, 15, 15};

    String fileName = "testMultipleFloatScorers";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(concat(vec0, vec1, vec2), 0, vec0.length * Float.BYTES * 3);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        var vectorValues = floatVectorValues(4, 3, in, EUCLIDEAN);
        var ss = flatVectorsScorer.getRandomVectorScorerSupplier(EUCLIDEAN, vectorValues);

        var scorerAgainstOrd0 = ss.scorer();
        scorerAgainstOrd0.setScoringOrdinal(0);
        var firstScore = scorerAgainstOrd0.score(1);
        @SuppressWarnings("unused")
        var scorerAgainstOrd2 = ss.scorer();
        scorerAgainstOrd2.setScoringOrdinal(2);
        var scoreAgain = scorerAgainstOrd0.score(1);

        assertThat(scoreAgain, equalTo(firstScore));
      }
    }
  }

  public void testCheckByteDimensions() throws IOException {
    byte[] vec0 = new byte[4];
    String fileName = "testCheckByteDimensions";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(vec0, 0, vec0.length);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
          var vectorValues = byteVectorValues(4, 1, in, sim);
          expectThrows(
              IllegalArgumentException.class,
              () -> flatVectorsScorer.getRandomVectorScorer(sim, vectorValues, new byte[5]));
        }
      }
    }
  }

  public void testCheckFloatDimensions() throws IOException {
    float[] vec0 = new float[4];
    String fileName = "testCheckFloatDimensions";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(concat(vec0), 0, vec0.length * Float.BYTES);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
          var vectorValues = floatVectorValues(4, 1, in, sim);
          expectThrows(
              IllegalArgumentException.class,
              () -> flatVectorsScorer.getRandomVectorScorer(sim, vectorValues, new float[5]));
        }
      }
    }
  }

  public void testBulkScorerBytes() throws IOException {
    int dims = random().nextInt(1, 1024);
    int size = random().nextInt(2, 255);
    String fileName = "testBulkScorerBytes";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (int i = 0; i < size; i++) {
          byte[] ba = randomByteVector(dims);
          out.writeBytes(ba, 0, ba.length);
        }
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        assert in.length() == (long) dims * size * Byte.BYTES;
        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
          var values = byteVectorValues(dims, size, in, sim);
          assertBulkEqualsNonBulk(values, sim);
          assertBulkEqualsNonBulkSupplier(values, sim);
          assertScoresAgainstDefaultFlatScorer(values, sim);
        }
      }
    }
  }

  // TODO: incredibly slow
  @Nightly
  public void testBulkScorerFloats() throws IOException {
    int dims = random().nextInt(1, 1024);
    int size = random().nextInt(2, 255);
    String fileName = "testBulkScorerFloats";
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        for (int i = 0; i < size; i++) {
          byte[] ba = concat(randomFloatVector(dims));
          out.writeBytes(ba, 0, ba.length);
        }
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        assert in.length() == (long) dims * size * Float.BYTES;
        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
          var values = floatVectorValues(dims, size, in, sim);
          assertBulkEqualsNonBulk(values, sim);
          assertBulkEqualsNonBulkSupplier(values, sim);
          assertScoresAgainstDefaultFlatScorer(values, sim);
        }
      }
    }
  }

  public void testOnHeapBulkScorerFloats() throws IOException {
    int dims = random().nextInt(1, 1024);
    int size = random().nextInt(2, 255);
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      vectors.add(randomFloatVector(dims));
    }
    var values = FloatVectorValues.fromFloats(vectors, dims);
    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
      assertBulkEqualsNonBulk(values, sim);
      assertBulkEqualsNonBulkSupplier(values, sim);
      assertScoresAgainstDefaultFlatScorer(values, sim);
    }
  }

  void assertBulkEqualsNonBulk(KnnVectorValues values, VectorSimilarityFunction sim)
      throws IOException {
    final int dims = values.dimension();
    final int size = values.size();
    final float delta = 1e-3f * size;
    var scorer =
        values.getEncoding() == VectorEncoding.BYTE
            ? flatVectorsScorer.getRandomVectorScorer(sim, values, randomByteVector(dims))
            : flatVectorsScorer.getRandomVectorScorer(sim, values, randomFloatVector(dims));
    int[] indices = randomIndices(size);
    float[] expectedScores = new float[size];
    float expectedMaxScore = Float.NEGATIVE_INFINITY;
    for (int i = 0; i < size; i++) {
      expectedScores[i] = scorer.score(indices[i]);
      expectedMaxScore = Math.max(expectedMaxScore, expectedScores[i]);
    }
    float[] bulkScores = new float[size];
    assertEquals(expectedMaxScore, scorer.bulkScore(indices, bulkScores, size), 0.001);
    assertArrayEquals(expectedScores, bulkScores, delta);
    assertNoScoreBeyondNumNodes(scorer, size);
  }

  // score through the supplier/updatableScorer interface
  void assertBulkEqualsNonBulkSupplier(KnnVectorValues values, VectorSimilarityFunction sim)
      throws IOException {
    final int size = values.size();
    final float delta = 1e-3f * size;
    var supplier = flatVectorsScorer.getRandomVectorScorerSupplier(sim, values);
    for (var ss : List.of(supplier, supplier.copy())) {
      var updatableScorer = ss.scorer();
      var targetNode = random().nextInt(size);
      updatableScorer.setScoringOrdinal(targetNode);
      int[] indices = randomIndices(size);
      float[] expectedScores = new float[size];
      for (int i = 0; i < size; i++) {
        expectedScores[i] = updatableScorer.score(indices[i]);
      }
      float[] bulkScores = new float[size];
      updatableScorer.bulkScore(indices, bulkScores, size);
      assertArrayEquals(expectedScores, bulkScores, delta);
      assertNoScoreBeyondNumNodes(updatableScorer, size);
    }
  }

  // asserts scores against the default scorer.
  void assertScoresAgainstDefaultFlatScorer(KnnVectorValues values, VectorSimilarityFunction sim)
      throws IOException {
    final int size = values.size();
    final float delta = 1e-3f * size;
    var targetNode = random().nextInt(size);
    int[] indices = randomIndices(size);
    var defaultScorer =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(sim, values).scorer();
    defaultScorer.setScoringOrdinal(targetNode);
    float[] expectedScores = new float[size];
    float expectedMaxScore = Float.NEGATIVE_INFINITY;
    for (int i = 0; i < size; i++) {
      expectedScores[i] = defaultScorer.score(indices[i]);
      expectedMaxScore = Math.max(expectedMaxScore, expectedScores[i]);
    }

    var supplier = flatVectorsScorer.getRandomVectorScorerSupplier(sim, values);
    for (var ss : List.of(supplier, supplier.copy())) {
      var updatableScorer = ss.scorer();
      updatableScorer.setScoringOrdinal(targetNode);
      float[] bulkScores = new float[size];
      assertEquals(expectedMaxScore, updatableScorer.bulkScore(indices, bulkScores, size), 0.001);
      assertArrayEquals(expectedScores, bulkScores, delta);
    }
  }

  void assertNoScoreBeyondNumNodes(RandomVectorScorer scorer, int maxSize) throws IOException {
    int numNodes = random().nextInt(0, maxSize);
    int[] indices = new int[numNodes + 1];
    float[] bulkScores = new float[numNodes + 1];
    bulkScores[bulkScores.length - 1] = Float.NaN;
    scorer.bulkScore(indices, bulkScores, numNodes);
    assertEquals(Float.NaN, bulkScores[bulkScores.length - 1], 0.0f);
  }

  byte[] randomByteVector(int dims) {
    byte[] ba = new byte[dims];
    random().nextBytes(ba);
    return ba;
  }

  float[] randomFloatVector(int dims) {
    float[] fa = new float[dims];
    for (int i = 0; i < dims; ++i) {
      fa[i] = random().nextFloat();
    }
    return fa;
  }

  ByteVectorValues byteVectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim)
      throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("byteValues", 0, in.length()), dims, flatVectorsScorer, sim);
  }

  FloatVectorValues floatVectorValues(
      int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
    return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
        dims,
        size,
        in.slice("floatValues", 0, in.length()),
        dims * Float.BYTES,
        flatVectorsScorer,
        sim);
  }

  /** Concatenates float arrays as byte[]. */
  public static byte[] concat(float[]... arrays) throws IOException {
    var bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (var fa : arrays) {
        for (var f : fa) {
          bb.putFloat(0, f);
          baos.write(bb.array());
        }
      }
      return baos.toByteArray();
    }
  }

  /** Concatenates byte arrays. */
  public static byte[] concat(byte[]... arrays) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (var ba : arrays) {
        baos.write(ba);
      }
      return baos.toByteArray();
    }
  }

  /** Returns an int[] of the given size with valued from 0 to size shuffled. */
  public static int[] randomIndices(int size) {
    List<Integer> list = IntStream.range(0, size).boxed().collect(Collectors.toList());
    Collections.shuffle(list, random());
    return list.stream().mapToInt(i -> i).toArray();
  }

  public static <T> void assertThat(T actual, Matcher<? super T> matcher) {
    MatcherAssert.assertThat("", actual, matcher);
  }
}
