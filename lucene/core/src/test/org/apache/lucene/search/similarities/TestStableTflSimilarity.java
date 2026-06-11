
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

  private static final float SCORE_EPSILON = 1e-7f;

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

  public void testValidParameters() {
    // boundary values: zero is allowed for both parameters
    StableTflSimilarity sim = new StableTflSimilarity(0f, 0f);
    assertNotNull(sim);

    // the no-arg constructor uses the documented defaults
    assertEquals(
        new StableTflSimilarity(StableTflSimilarity.DEFAULT_K1, StableTflSimilarity.DEFAULT_C)
            .toString(),
        new StableTflSimilarity().toString());
  }

  public void testToString() {
    StableTflSimilarity sim = new StableTflSimilarity();
    assertEquals("StableTFLSimilarity(k1=1.2, c=0.917, m=0.00781)", sim.toString());
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
    assertEquals(1.3955811f, score, SCORE_EPSILON);

    Explanation explain = scorer.explain(Explanation.match(1, "freq"), norm);
    assertEquals(score, explain.getValue().floatValue(), SCORE_EPSILON);

    String explainString =
        """
        1.3955811 = score(freq=1), computed as boost * tr * tf from:
          1.0 = boost
          0.45454544 = tf, computed as freq / (freq + k1)) from:
            1 = freq
            1.2 = k1, term saturation parameter
          3.0702786 = tr, term rarity, computed as log(1 + (1 - p + 0.05) / (p + 0.05)) from:
            0.001049047818598714 = p, probability that the term appears in the doc, \
        computed as 1 - (1 - m * 2 ^ (-c * tl)) ^ dl from:
              0.00781 = m, multiplicative constant to term mismatch probability
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
    assertEquals(scorer.score(1, norm), explain.getValue().floatValue(), SCORE_EPSILON);
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
    return new StableTflSimilarity(k1, c);
  }
}
