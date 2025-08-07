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
import org.junit.After;
import org.junit.Before;

public class TestVectorScorerFloat32Benchmark extends LuceneTestCase {

  VectorScorerFloat32Benchmark bench;
  float delta;

  @Before
  public void setup() throws IOException {
    bench = new VectorScorerFloat32Benchmark();
    bench.size = 1024;
    bench.numVectors = random().nextInt(1, 256);
    bench.numVectorsToScore = random().nextInt(bench.numVectors);
    delta = 1e-3f * bench.size;
    bench.setup();
  }

  @After
  public void teardown() throws IOException {
    bench.teardown();
  }

  public void testDotProduct() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductDefault();
    var expectedScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductDefaultBulk();
    var bulkScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductOptScorer();
    var actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.dotProductOptBulkScore();
    actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testCosine() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.cosineDefault();
    var expectedScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineDefaultBulk();
    var bulkScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineOptScorer();
    var actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.cosineOptBulkScore();
    actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testEuclidean() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanDefault();
    var expectedScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanDefaultBulk();
    var bulkScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanOptScorer();
    var actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.euclideanOptBulkScore();
    actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);
  }

  public void testMip() throws IOException {
    Arrays.fill(bench.scores, 0.0f);
    bench.mipDefault();
    var expectedScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipDefaultBulk();
    var bulkScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, bulkScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipOptScorer();
    var actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);

    Arrays.fill(bench.scores, 0.0f);
    bench.mipOptBulkScore();
    actualScores = Arrays.copyOfRange(bench.scores, 0, bench.scores.length);
    assertArrayEquals(expectedScores, actualScores, delta);
  }
}
