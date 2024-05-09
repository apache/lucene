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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
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

  public void testDefaultOrMemSegScorer() {
    var scorer = FlatVectorScorerUtil.newFlatVectorScorer();
    System.out.println("HEGO: " + scorer);
    assertThat(
        scorer.toString(),
        is(oneOf("DefaultFlatVectorScorer()", "MemorySegmentFlatVectorsScorer()")));
  }

  public void testMultipleScorers() throws IOException {
    testMultipleScorersImpl(TestFlatVectorScorer::newDirectory);
  }

  public void testMultipleScorersMMap() throws IOException {
    testMultipleScorersImpl(() -> new MMapDirectory(createTempDir(getTestName())));
  }

  // Tests that the creation of another scorer does not disturb previous scorers
  void testMultipleScorersImpl(ThrowingSupplier<Directory> newDirectory) throws IOException {
    byte[] vec0 = new byte[] {0, 0, 0, 0};
    byte[] vec1 = new byte[] {1, 1, 1, 1};
    byte[] vec2 = new byte[] {15, 15, 15, 15};

    String fileName = getTestName();
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(vec0, 0, vec0.length);
        out.writeBytes(vec1, 0, vec1.length);
        out.writeBytes(vec2, 0, vec2.length);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        var vectorValues = vectorValues(4, 3, in);
        var factory = FlatVectorScorerUtil.newFlatVectorScorer();
        var ss = factory.getRandomVectorScorerSupplier(EUCLIDEAN, vectorValues);

        var scorerAgainstOrd0 = ss.scorer(0);
        var firstScore = scorerAgainstOrd0.score(1);
        // ensure that the creation of another scorer does not disturb previous scorers
        @SuppressWarnings("unused")
        var scorerAgainstOrd2 = ss.scorer(2);
        var scoreAgain = scorerAgainstOrd0.score(1);

        assertThat(scoreAgain, equalTo(firstScore));
      }
    }
  }

  public void testCheckDimensions() throws IOException {
    testCheckDimensionsImpl(TestFlatVectorScorer::newDirectory);
  }

  public void testCheckDimensionsMMap() throws IOException {
    testCheckDimensionsImpl(() -> new MMapDirectory(createTempDir(getTestName())));
  }

  void testCheckDimensionsImpl(ThrowingSupplier<Directory> newDirectory) throws IOException {
    byte[] vec0 = new byte[4];
    String fileName = getTestName();
    try (Directory dir = newDirectory.get()) {
      try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
        out.writeBytes(vec0, 0, vec0.length);
      }
      try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
        var vectorValues = vectorValues(4, 1, in);
        var factory = FlatVectorScorerUtil.newFlatVectorScorer();
        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
          expectThrows(
              IllegalArgumentException.class,
              () -> factory.getRandomVectorScorer(sim, vectorValues, new byte[5]));
        }
      }
    }
  }

  public static <T> void assertThat(T actual, Matcher<? super T> matcher) {
    MatcherAssert.assertThat("", actual, matcher);
  }

  static RandomAccessVectorValues vectorValues(int dims, int size, IndexInput in)
      throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("test", 0, in.length()), dims);
  }

  @FunctionalInterface
  interface ThrowingSupplier<T> {
    T get() throws IOException;
  }
}
