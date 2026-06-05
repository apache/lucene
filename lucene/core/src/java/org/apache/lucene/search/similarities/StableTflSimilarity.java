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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldStats;
import org.apache.lucene.search.TermStats;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.UnicodeUtil;

/**
 * A Lucene {@link Similarity} implementation that estimates term rarity based on term length and
 * document length, rather than relying on corpus-level statistics like document frequency.
 *
 * <p>Inspired by BM25, this similarity model replaces IDF with a synthetic function that increases
 * with term length (as a proxy for rarity) and adjusts based on document length.
 *
 * <p>Scoring formula:
 *
 * <pre>
 * tf = freq / (freq + k1)
 * p  = 1 - (1 - m * 2 ^ (-c * |t|)) ^ |d|
 * tr = log(1 + (1 - p + 0.05) / (p + 0.05))
 * score = boost * tr * tf
 * </pre>
 *
 * <p>This similarity is parameterized by:
 *
 * <ul>
 *   <li>{@code k1} - saturation parameter for term frequency
 *   <li>{@code c} - decay constant controlling how term length impacts rarity
 * </ul>
 *
 * <p>The constant {@code m} is fixed internally and controls the base term-match probability.
 */
public class StableTflSimilarity extends Similarity {

  /** Default k1 value used in Lucene's implementation of BM25. */
  public static final float DEFAULT_K1 = 1.2f;

  /** Empirical value of c derived from modeling term frequency in English corpuses. */
  public static final float DEFAULT_C = 0.917f;

  /** Multiplicative constant to term matching probability. Non-adjustable by users. */
  private static final float M = 0.00781f;

  private static final StableTflSimilarity INSTANCE =
      new StableTflSimilarity(DEFAULT_K1, DEFAULT_C);

  /** Controls non-linear term frequency normalization (saturation). */
  private final float k1;

  /** Controls how much term length affects term rarity. */
  private final float c;

  /**
   * Default term length if term BytesRef can't be decoded with UTF_8. This is close to the average
   * word length in English.
   */
  private static final float NON_UTF_8_DEFAULT_TERM_LENGTH = 5;

  /** Cache of decoded bytes. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  /**
   * STABLE_TFL with the supplied parameter values.
   *
   * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is
   *     infinite or negative.
   */
  public StableTflSimilarity(float k1, float c) {
    if (!Float.isFinite(k1) || k1 < 0) {
      throw new IllegalArgumentException(
          "illegal k1 value: " + k1 + ", must be a non-negative finite value");
    }
    if (!Float.isFinite(c) || c < 0) {
      throw new IllegalArgumentException(
          "illegal c value: " + c + ", must be a non-negative finite value");
    }
    this.k1 = k1;
    this.c = c;
  }

  public StableTflSimilarity() {
    this(DEFAULT_K1, DEFAULT_C);
  }

  /**
   * Implemented as <code>log(1 + (1 - p + 0.05)/(p + 0.05))</code>, where <code>
   *  p = 1 - (1 - m * 2 ^ (-c * termLength)) ^ docLength </code>
   */
  private float tr(float termLength, float docLength) {
    // The probability that a term is present in a document with length docLength.
    double p = (1 - Math.pow(1 - M * Math.pow(2, -this.c * termLength), docLength));

    return (float) Math.log(1 + (1 - p + 0.05D) / (p + 0.05D));
  }

  /**
   * Returns the number of code points in this UTF8 sequence. If invalid codepoint header byte
   * occurs or the content is prematurely truncated, return NON_UTF_8_DEFAULT_TERM_LENGTH.
   */
  private static float getTermLength(BytesRef termText) {
    try {
      return UnicodeUtil.codePointCount(termText);
    } catch (IllegalArgumentException e) {
      return NON_UTF_8_DEFAULT_TERM_LENGTH;
    }
  }

  @Override
  public final SimScorer scorer(float boost, FieldStats fieldStats, TermStats... termStats) {
    float[] cache = new float[256];
    float[] termLengths = new float[termStats.length];

    for (int i = 0; i < termStats.length; i++) {
      float termLength = getTermLength(termStats[i].term());
      termLengths[i] = termLength;
      for (int j = 0; j < cache.length; j++) {
        float docLength = LENGTH_TABLE[j];
        cache[j] += tr(termLength, docLength);
      }
    }

    return new TflScorer(boost, this.k1, this.c, termLengths, cache);
  }

  private static class TflScorer extends SimScorer {
    private final float boost;
    private final float k1;
    private final float c;

    /**
     * termLengths[i] stores the decoded UTF-8 term length (in code points) of the i-th query term.
     */
    private final float[] termLengths;

    /**
     * cache[i] stores the precomputed sum of term rarity (tr) for all query terms, for a specific
     * document length (LENGTH_TABLE[i]).
     */
    private final float[] cache;

    TflScorer(float boost, float k1, float c, float[] termLengths, float[] cache) {
      this.boost = boost;
      this.k1 = k1;
      this.c = c;
      this.termLengths = termLengths;
      this.cache = cache;
    }

    @Override
    public float score(float freq, long encodedNorm) {
      // In order to guarantee monotonicity with freq without promoting to doubles, we rewrite
      // freq / (freq + k1) to 1 - 1 / (1 + freq / k1).
      // freq / k1 is guaranteed to be monotonic. And then monotonicity is preserved through
      // composition via x -> 1 + x and x -> 1 - 1 / x.
      // Finally, we expand weight * (1 - 1 / (1 + freq / k1)) to
      // weight - weight / (1 + freq / k1), which runs slightly faster.
      float tr = this.cache[((byte) encodedNorm) & 0xFF];
      float weight = this.boost * tr;
      return weight - weight / (1 + freq / this.k1);
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      List<Explanation> subs = new ArrayList<>();
      subs.add(Explanation.match(this.boost, "boost"));
      subs.add(explainTF(freq));
      subs.add(explainTR(this.termLengths, norm));

      float tr = this.cache[((byte) norm) & 0xFF];
      float weight = this.boost * tr;
      return Explanation.match(
          weight - weight / (1f + freq.getValue().floatValue() / this.k1),
          "score(freq=" + freq.getValue() + "), computed as boost * tr * tf from:",
          subs);
    }

    private Explanation explainTR(float termLength, long norm) {
      float docLength = LENGTH_TABLE[((byte) norm) & 0xff];
      List<Explanation> subs = new ArrayList<>(explainTrConstantFactors());
      subs.add(Explanation.match(termLength, "tl, term length"));
      subs.add(Explanation.match(docLength, "dl, document length"));

      // The probability that a term is present in a document with length docLength.
      double p = (1 - Math.pow(1 - M * Math.pow(2, -this.c * termLength), docLength));
      Explanation probExplanation =
          Explanation.match(
              p,
              "p, probability that the term appears in the doc, "
                  + "computed as 1 - (1 - m * 2 ^ (-c * tl)) ^ dl from:",
              subs);

      float tr = (float) Math.log(1 + (1 - p + 0.05D) / (p + 0.05D));
      return Explanation.match(
          tr,
          "tr, term rarity, computed as log(1 + (1 - p + 0.05) / (p + 0.05)) from:",
          probExplanation);
    }

    private Explanation explainTR(float[] termLengths, long norm) {
      if (termLengths.length == 1) {
        return explainTR(termLengths[0], norm);
      }

      double tr = 0d;
      List<Explanation> details = new ArrayList<>();
      for (float termLength : termLengths) {
        Explanation trExplain = explainTR(termLength, norm);
        details.add(trExplain);
        tr += trExplain.getValue().floatValue();
      }
      return Explanation.match(tr, "tr, term rarity, computed as the sum of:", details);
    }

    private List<Explanation> explainTrConstantFactors() {
      return List.of(
          Explanation.match(M, "m, multiplicative constant to term mismatch probability"),
          Explanation.match(this.c, "c, decaying constant for term length"));
    }

    private Explanation explainTF(Explanation freq) {
      List<Explanation> subs =
          List.of(freq, Explanation.match(this.k1, "k1, term saturation parameter"));

      return Explanation.match(
          1f - 1f / (1 + freq.getValue().floatValue() / this.k1),
          "tf, computed as freq / (freq + k1)) from:",
          subs);
    }
  }

  @Override
  public String toString() {
    return "StableTFLSimilarity(k1=" + this.k1 + ", c=" + this.c + ", m=" + M + ")";
  }
}
