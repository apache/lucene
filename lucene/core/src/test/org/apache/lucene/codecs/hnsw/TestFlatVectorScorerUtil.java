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

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
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

public class TestFlatVectorScorerUtil extends LuceneTestCase {

  public void testDefaultOrMemSegScorer() {
    var scorer = FlatVectorScorerUtil.newFlatVectorScorer();
    System.out.println("HEGO: " + scorer);
    assertThat(
        scorer.toString(),
        is(oneOf("DefaultFlatVectorScorer()", "MemorySegmentFlatVectorsScorer()")));
  }

  // Tests that the creation of another scorer does not disturb previous scorers
  public void testMultipleScorers() throws IOException {
    byte[] vec0 = new byte[] {0, 0, 0, 0};
    byte[] vec1 = new byte[] {1, 1, 1, 1};
    byte[] vec2 = new byte[] {32, 32, 32, 32};

    try (Directory dir = new MMapDirectory(createTempDir(getTestName()))) {
      try (IndexOutput out = dir.createOutput("testFoo", IOContext.DEFAULT)) {
        out.writeBytes(vec0, 0, vec0.length);
        out.writeBytes(vec1, 0, vec1.length);
        out.writeBytes(vec2, 0, vec2.length);
      }
      try (IndexInput in = dir.openInput("testFoo", IOContext.DEFAULT)) {
        var vectorValues = vectorValues(4, 3, in);
        var factory = FlatVectorScorerUtil.newFlatVectorScorer();
        var ss = factory.getRandomVectorScorerSupplier(EUCLIDEAN, vectorValues);

        var scorerAgainstOrd0 = ss.scorer(0);
        var firstScore = scorerAgainstOrd0.score(1);
        // ensure that the creation of another scorer does not disturb previous scorers
        var scorerAgainstOrd2 = ss.scorer(2);
        var scoreAgain = scorerAgainstOrd0.score(1);

        assertThat(scoreAgain, equalTo(firstScore));
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
}
