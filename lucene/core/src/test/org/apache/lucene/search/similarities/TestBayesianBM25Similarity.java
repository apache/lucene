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
package org.apache.lucene.search.similarities;

import java.util.Random;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.tests.search.similarities.BaseSimilarityTestCase;
import org.apache.lucene.util.BytesRef;

public class TestBayesianBM25Similarity extends BaseSimilarityTestCase {

  public void testIllegalK1() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(Float.POSITIVE_INFINITY, 0.75f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(-1, 0.75f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(Float.NaN, 0.75f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
  }

  public void testIllegalB() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 2f);
            });
    assertTrue(expected.getMessage().contains("illegal b value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, -1f);
            });
    assertTrue(expected.getMessage().contains("illegal b value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, Float.NaN);
            });
    assertTrue(expected.getMessage().contains("illegal b value"));
  }

  public void testIllegalAlpha() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 0.75f, -1.0f, null, true);
            });
    assertTrue(expected.getMessage().contains("illegal alpha value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 0.75f, 0.0f, null, true);
            });
    assertTrue(expected.getMessage().contains("illegal alpha value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 0.75f, Float.POSITIVE_INFINITY, null, true);
            });
    assertTrue(expected.getMessage().contains("illegal alpha value"));
  }

  public void testIllegalBeta() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 0.75f, 1.0f, Float.NaN, true);
            });
    assertTrue(expected.getMessage().contains("illegal beta value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new BayesianBM25Similarity(1.2f, 0.75f, 1.0f, Float.POSITIVE_INFINITY, true);
            });
    assertTrue(expected.getMessage().contains("illegal beta value"));
  }

  public void testSigmoidStability() {
    // Large positive value should produce ~1.0 without overflow
    float large = BayesianBM25Similarity.sigmoid(100f);
    assertTrue("sigmoid(100) should be close to 1.0", large > 0.99f && large <= 1.0f);

    // Large negative value should produce ~0.0 without underflow
    float smallVal = BayesianBM25Similarity.sigmoid(-100f);
    assertTrue("sigmoid(-100) should be close to 0.0", smallVal >= 0.0f && smallVal < 0.01f);

    // Zero should be exactly 0.5
    float mid = BayesianBM25Similarity.sigmoid(0f);
    assertEquals("sigmoid(0) should be 0.5", 0.5f, mid, 1e-6f);

    // Sigmoid should be monotonically increasing
    float prev = BayesianBM25Similarity.sigmoid(-50f);
    for (float x = -49f; x <= 50f; x += 1f) {
      float curr = BayesianBM25Similarity.sigmoid(x);
      assertTrue("sigmoid should be monotonically increasing", curr >= prev);
      prev = curr;
    }
  }

  public void testScoreRange() {
    BayesianBM25Similarity sim = new BayesianBM25Similarity();
    CollectionStatistics collStats = new CollectionStatistics("field", 10000, 10000, 50000, 10000);
    TermStatistics termStats = new TermStatistics(new BytesRef("term"), 100, 500);

    Similarity.SimScorer scorer = sim.scorer(1.0f, collStats, termStats);

    // Test various freq/norm combinations
    for (int freq = 1; freq <= 100; freq++) {
      for (int norm = 1; norm < 256; norm++) {
        float score = scorer.score(freq, norm);
        assertTrue(
            "score should be > 0, got " + score + " for freq=" + freq + ", norm=" + norm,
            score > 0);
        assertTrue(
            "score should be < 1, got " + score + " for freq=" + freq + ", norm=" + norm,
            score < 1);
        assertTrue("score should be finite", Float.isFinite(score));
      }
    }
  }

  public void testTfPriorMonotonicity() {
    BayesianBM25Similarity sim = new BayesianBM25Similarity();
    CollectionStatistics collStats = new CollectionStatistics("field", 10000, 10000, 50000, 10000);
    TermStatistics termStats = new TermStatistics(new BytesRef("term"), 100, 500);

    Similarity.SimScorer scorer = sim.scorer(1.0f, collStats, termStats);

    // Score should increase (or stay the same) as freq increases
    for (int norm = 1; norm < 256; norm++) {
      float prevScore = scorer.score(1, norm);
      for (int freq = 2; freq <= 50; freq++) {
        float currScore = scorer.score(freq, norm);
        assertTrue(
            "score should not decrease with increasing freq: score("
                + (freq - 1)
                + ")="
                + prevScore
                + " > score("
                + freq
                + ")="
                + currScore,
            currScore >= prevScore);
        prevScore = currScore;
      }
    }
  }

  public void testDefaultParameters() {
    BayesianBM25Similarity sim = new BayesianBM25Similarity();
    assertEquals(1.2f, sim.getK1(), 0f);
    assertEquals(0.75f, sim.getB(), 0f);
    assertNull(sim.getAlpha());
    assertNull(sim.getBeta());
  }

  public void testExplicitParameters() {
    BayesianBM25Similarity sim = new BayesianBM25Similarity(1.5f, 0.8f, 2.0f, 1.0f, true);
    assertEquals(1.5f, sim.getK1(), 0f);
    assertEquals(0.8f, sim.getB(), 0f);
    assertEquals(Float.valueOf(2.0f), sim.getAlpha());
    assertEquals(Float.valueOf(1.0f), sim.getBeta());
  }

  public void testToString() {
    BayesianBM25Similarity sim = new BayesianBM25Similarity();
    String s = sim.toString();
    assertTrue(s.contains("BayesianBM25"));
    assertTrue(s.contains("k1="));
    assertTrue(s.contains("b="));
  }

  @Override
  protected Similarity getSimilarity(Random random) {
    // term frequency normalization parameter k1
    final float k1;
    switch (random.nextInt(4)) {
      case 0:
        k1 = 0;
        break;
      case 1:
        k1 = Float.MIN_VALUE;
        break;
      case 2:
        // use a more reasonable upper bound than Integer.MAX_VALUE
        // because the Bayesian transform division by weight can cause issues
        k1 = 10f;
        break;
      default:
        k1 = 10f * random.nextFloat();
        break;
    }

    // length normalization parameter b [0 .. 1]
    final float b;
    switch (random.nextInt(4)) {
      case 0:
        b = 0;
        break;
      case 1:
        b = Float.MIN_VALUE;
        break;
      case 2:
        b = 1;
        break;
      default:
        b = random.nextFloat();
        break;
    }

    // alpha and beta: sometimes auto, sometimes explicit
    final Float alpha;
    final Float beta;
    if (random.nextBoolean()) {
      alpha = null;
      beta = null;
    } else {
      alpha = 0.1f + 9.9f * random.nextFloat();
      beta = -5f + 10f * random.nextFloat();
    }

    return new BayesianBM25Similarity(k1, b, alpha, beta, true);
  }
}
