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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.junit.After;
import org.junit.Before;

public class TestVectorScorerFloat32Benchmark extends LuceneTestCase {

  VectorScorerFloat32Benchmark bench;
  float delta;

  @Before
  public void setup() throws IOException {
    bench = new VectorScorerFloat32Benchmark();
    bench.size = 1024;
    bench.pollute = true;
    bench.numVectors = random().nextInt(1, 256);
    bench.numVectorsToScore = random().nextInt(bench.numVectors);
    delta = 1e-3f * bench.size;
    bench.setup();
    bench.perIterationInit();
  }

  @After
  public void teardown() throws IOException {
    bench.teardown();
  }

  public void testDotProduct() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductDefault();
    var expectedScores = ArrayUtil.copyArray(bench.scores);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductDefaultBulk();
    var bulkScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductOptScorer();
    var actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductOptBulkScore();
    actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testCosine() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.cosineDefault();
    var expectedScores = ArrayUtil.copyArray(bench.scores);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineDefaultBulk();
    var bulkScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineOptScorer();
    var actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineOptBulkScore();
    actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testEuclidean() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanDefault();
    var expectedScores = ArrayUtil.copyArray(bench.scores);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanDefaultBulk();
    var bulkScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanOptScorer();
    var actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanOptBulkScore();
    actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testMip() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.mipDefault();
    var expectedScores = ArrayUtil.copyArray(bench.scores);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipDefaultBulk();
    var bulkScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipOptScorer();
    var actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipOptBulkScore();
    actualScores = ArrayUtil.copyArray(bench.scores);
    assertArrayEquals(expectedScores, actualScores, delta);
  }
}
