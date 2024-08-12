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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;

public class TestFlatVectorScorer extends LuceneTestCase {

  static volatile AtomicInteger count = new AtomicInteger();
  final FlatVectorsScorer flatVectorsScorer;
  final ThrowingSupplier<Directory> newDirectory;

  public TestFlatVectorScorer(
      FlatVectorsScorer flatVectorsScorer, ThrowingSupplier<Directory> newDirectory) {
    this.flatVectorsScorer = flatVectorsScorer;
    this.newDirectory = newDirectory;
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    var scorers =
        List.of(
            DefaultFlatVectorScorer.INSTANCE,
            new Lucene99ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer()),
            FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    var dirs =
        List.<ThrowingSupplier<Directory>>of(
            TestFlatVectorScorer::newDirectory,
            () -> new MMapDirectory(createTempDir(count.getAndIncrement() + "-")));

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

        var scorerAgainstOrd0 = ss.scorer(0);
        var firstScore = scorerAgainstOrd0.score(1);
        @SuppressWarnings("unused")
        var scorerAgainstOrd2 = ss.scorer(2);
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

        var scorerAgainstOrd0 = ss.scorer(0);
        var firstScore = scorerAgainstOrd0.score(1);
        @SuppressWarnings("unused")
        var scorerAgainstOrd2 = ss.scorer(2);
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

  RandomAccessVectorValues byteVectorValues(
      int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("byteValues", 0, in.length()), dims, flatVectorsScorer, sim);
  }

  RandomAccessVectorValues floatVectorValues(
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

  public static <T> void assertThat(T actual, Matcher<? super T> matcher) {
    MatcherAssert.assertThat("", actual, matcher);
  }

  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws IOException;
  }
}
