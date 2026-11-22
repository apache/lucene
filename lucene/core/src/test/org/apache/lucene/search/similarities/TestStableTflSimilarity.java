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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldStats;
import org.apache.lucene.search.TermStats;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.tests.search.similarities.BaseSimilarityTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

public class TestStableTflSimilarity extends BaseSimilarityTestCase {

  public void testIllegalK1() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(Float.POSITIVE_INFINITY, 0.917f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(-1, 0.917f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(Float.NaN, 0.917f);
            });
    assertTrue(expected.getMessage().contains("illegal k1 value"));
  }

  public void testIllegalC() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, Float.POSITIVE_INFINITY);
            });
    assertTrue(expected.getMessage().contains("illegal c value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, -1f);
            });
    assertTrue(expected.getMessage().contains("illegal c value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, Float.NaN);
            });
    assertTrue(expected.getMessage().contains("illegal c value"));
  }

  public void testIllegalK3() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, 0.917f, Float.POSITIVE_INFINITY);
            });
    assertTrue(expected.getMessage().contains("illegal k3 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, 0.917f, Float.NEGATIVE_INFINITY);
            });
    assertTrue(expected.getMessage().contains("illegal k3 value"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new StableTflSimilarity(1.2f, 0.917f, Float.NaN);
            });
    assertTrue(expected.getMessage().contains("illegal k3 value"));
  }

  public void testValidK3() {
    // negative k3 disables saturation — should be allowed
    StableTflSimilarity sim = new StableTflSimilarity(1.2f, 0.917f, -1f);
    assertEquals(-1f, sim.getK3(), 0f);

    // zero k3 — maximum saturation
    sim = new StableTflSimilarity(1.2f, 0.917f, 0f);
    assertEquals(0f, sim.getK3(), 0f);

    // typical k3 value
    sim = new StableTflSimilarity(1.2f, 0.917f, 8f);
    assertEquals(8f, sim.getK3(), 0f);
  }

  public void testComputeQueryTermWeight() {
    // negative k3 disables saturation: weight is the (linear) query term frequency
    StableTflSimilarity disabled = new StableTflSimilarity(1.2f, 0.917f, -1f);
    for (int qtf = 1; qtf <= 5; qtf++) {
      assertEquals((float) qtf, disabled.computeQueryTermWeight(qtf), 0f);
    }

    // k3 = 0 fully saturates: any positive frequency yields a weight of 1
    StableTflSimilarity saturated = new StableTflSimilarity(1.2f, 0.917f, 0f);
    for (int qtf = 1; qtf <= 5; qtf++) {
      assertEquals(1f, saturated.computeQueryTermWeight(qtf), 0f);
    }

    // positive k3 saturates: ((k3 + 1) * qtf) / (k3 + qtf), monotonic but sub-linear
    float k3 = 8f;
    StableTflSimilarity sim = new StableTflSimilarity(1.2f, 0.917f, k3);
    float prev = 0f;
    for (int qtf = 1; qtf <= 10; qtf++) {
      float expected = ((k3 + 1f) * qtf) / (k3 + qtf);
      float weight = sim.computeQueryTermWeight(qtf);
      assertEquals(expected, weight, 0f);
      assertTrue("weight should not decrease as qtf increases", weight >= prev);
      assertTrue("saturation should keep weight at or below linear", weight <= qtf);
      prev = weight;
    }
  }

  public void testValidParameters() {
    // boundary values: zero is allowed for both parameters
    StableTflSimilarity sim = new StableTflSimilarity(0f, 0f);
    assertEquals(0f, sim.getK1(), 0f);
    assertEquals(0f, sim.getC(), 0f);

    // typical values are returned as set
    sim = new StableTflSimilarity(1.5f, 0.9f, 8f);
    assertEquals(1.5f, sim.getK1(), 0f);
    assertEquals(0.9f, sim.getC(), 0f);
    assertEquals(8f, sim.getK3(), 0f);

    // the no-arg constructor uses the documented defaults
    StableTflSimilarity defaults = new StableTflSimilarity();
    assertEquals(StableTflSimilarity.DEFAULT_K1, defaults.getK1(), 0f);
    assertEquals(StableTflSimilarity.DEFAULT_C, defaults.getC(), 0f);
    assertEquals(StableTflSimilarity.DEFAULT_K3, defaults.getK3(), 0f);
  }

  public void testToString() {
    StableTflSimilarity sim = new StableTflSimilarity();
    assertEquals("StableTflSimilarity(k1=1.2, c=0.917)", sim.toString());

    // k3 is only rendered when saturation is enabled (non-negative)
    StableTflSimilarity withK3 = new StableTflSimilarity(1.2f, 0.917f, 8.0f);
    assertEquals("StableTflSimilarity(k1=1.2, c=0.917, k3=8.0)", withK3.toString());
  }

  /**
   * Reproduces the canonical scoring example: term rarity is derived from the term length and the
   * document length rather than from corpus statistics, so the supplied {@link FieldStats}/{@link
   * TermStats} do not affect the score.
   */
  public void testExplain() throws Exception {
    StableTflSimilarity similarity = new StableTflSimilarity();
    FieldStats fieldStats = new FieldStats("field", 4, 4, 3003, 2000);
    TermStats termStats = new TermStats(new BytesRef("photosynthesis"), 3, 3);
    SimScorer scorer = similarity.scorer(1, fieldStats, termStats);

    int numTerms = 1000;
    long norm = SmallFloat.intToByte4(numTerms);
    float score = scorer.score(1, norm);
    assertEquals(1.3955811f, score, 0f);

    Explanation explain = scorer.explain(Explanation.match(1, "freq"), norm);
    assertEquals(score, explain.getValue().floatValue(), 0f);

    String explainString =
        """
        1.3955811 = score(freq=1), computed as boost * tr * tf from:
          1.0 = boost
          0.45454544 = tf, computed as freq / (freq + k1) from:
            1 = freq
            1.2 = k1, term saturation parameter
          3.0702786 = tr, term rarity, computed as log(1 + (1 - p + 0.05) / (p + 0.05)) from:
            0.001049047818598714 = p, probability that the term appears in the doc, \
        computed as 1 - (1 - m * 2 ^ (-c * tl)) ^ dl from:
              0.00781 = m, multiplicative constant to term match probability
              0.917 = c, decaying constant for term length
              14.0 = tl, term length
              984.0 = dl, document length
        """;
    assertEquals(explainString, explain.toString());
  }

  /** Multi-term queries sum the term rarity of each query term. */
  public void testMultiTermExplain() throws Exception {
    StableTflSimilarity similarity = new StableTflSimilarity();
    FieldStats fieldStats = new FieldStats("field", 4, 4, 3003, 2000);
    SimScorer scorer =
        similarity.scorer(
            1,
            fieldStats,
            new TermStats(new BytesRef("photosynthesis"), 3, 3),
            new TermStats(new BytesRef("chlorophyll"), 3, 3));

    long norm = SmallFloat.intToByte4(1000);
    Explanation explain = scorer.explain(Explanation.match(1, "freq"), norm);
    // the combined term rarity must be the sum of the individual per-term rarities
    assertTrue(explain.toString().contains("tr, term rarity, computed as the sum of:"));
    assertEquals(scorer.score(1, norm), explain.getValue().floatValue(), 0f);
  }

  /**
   * A term that is not valid UTF-8 falls back to the default term length of 5 code points instead
   * of throwing.
   */
  public void testNonUtf8TermFallsBackToDefaultLength() {
    StableTflSimilarity similarity = new StableTflSimilarity();
    FieldStats fieldStats = new FieldStats("field", 4, 4, 3003, 2000);
    long norm = SmallFloat.intToByte4(1000);

    // 0xFF is never a valid UTF-8 leading byte
    BytesRef invalidUtf8 = new BytesRef(new byte[] {(byte) 0xFF, (byte) 0xFF});
    SimScorer invalidScorer = similarity.scorer(1, fieldStats, new TermStats(invalidUtf8, 3, 3));

    // "hello" has exactly 5 code points, the default term length for undecodable terms
    SimScorer fiveCharScorer =
        similarity.scorer(1, fieldStats, new TermStats(new BytesRef("hello"), 3, 3));

    assertEquals(fiveCharScorer.score(1, norm), invalidScorer.score(1, norm), 0f);
  }

  /** Scores depend only on term length and document length, never on corpus statistics. */
  public void testScoreIndependentOfCorpusStats() {
    StableTflSimilarity similarity = new StableTflSimilarity();
    BytesRef term = new BytesRef("photosynthesis");
    long norm = SmallFloat.intToByte4(1000);

    SimScorer sparse =
        similarity.scorer(1, new FieldStats("field", 4, 4, 3003, 2000), new TermStats(term, 1, 1));
    SimScorer dense =
        similarity.scorer(
            1,
            new FieldStats("field", 1_000_000, 999_999, 123_456_789, 99_999_999),
            new TermStats(term, 999_999, 12_345_678));

    assertEquals(sparse.score(1, norm), dense.score(1, norm), 0f);
  }

  /**
   * A multi-term scorer's score equals the sum of the corresponding single-term scores (modulo
   * float summation order).
   */
  public void testMultiTermScoreIsSumOfSingleTermScores() {
    StableTflSimilarity similarity = new StableTflSimilarity();
    FieldStats fieldStats = new FieldStats("field", 4, 4, 3003, 2000);
    TermStats term1 = new TermStats(new BytesRef("photosynthesis"), 3, 3);
    TermStats term2 = new TermStats(new BytesRef("chlorophyll"), 3, 3);
    long norm = SmallFloat.intToByte4(1000);
    float freq = 2;

    float combined = similarity.scorer(1, fieldStats, term1, term2).score(freq, norm);
    float sum =
        similarity.scorer(1, fieldStats, term1).score(freq, norm)
            + similarity.scorer(1, fieldStats, term2).score(freq, norm);

    assertEquals(sum, combined, 1e-6f);
  }

  @Override
  protected Similarity getSimilarity(Random random) {
    // term frequency saturation parameter k1
    final float k1;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        k1 = 0;
        break;
      case 1:
        // tiny value
        k1 = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        k1 = Integer.MAX_VALUE;
        break;
      default:
        // random value
        k1 = Integer.MAX_VALUE * random.nextFloat();
        break;
    }

    // term length decay parameter c [0 .. infinity)
    final float c;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        c = 0;
        break;
      case 1:
        // tiny value
        c = Float.MIN_VALUE;
        break;
      case 2:
        // large value
        c = Integer.MAX_VALUE;
        break;
      default:
        // random value
        c = Integer.MAX_VALUE * random.nextFloat();
        break;
    }

    // query-term saturation parameter k3: negative (disabled) or a finite non-negative value
    final float k3 = random.nextBoolean() ? -1f : random.nextFloat() * 20f;
    return new StableTflSimilarity(k1, c, k3);
  }
}
