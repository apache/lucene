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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldStats;
import org.apache.lucene.search.TermStats;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.UnicodeUtil;

/**
 * StableTfl Similarity. A similarity algorithm that estimates term rarity based on term length and
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
 *   <li>{@code k3} - query-side saturation for repeated query terms (disabled by default)
 * </ul>
 *
 * <p>The constant {@code m} is fixed internally and controls the base term-match probability.
 *
 * @lucene.experimental
 */
public class StableTflSimilarity extends Similarity {

  /** Default k1 value used in Lucene's implementation of BM25. */
  public static final float DEFAULT_K1 = 1.2f;

  /** Empirical value of c derived from modeling term frequency in English corpuses. */
  public static final float DEFAULT_C = 0.917f;

  /** Default k3 value. A negative value disables query-term frequency saturation. */
  public static final float DEFAULT_K3 = -1f;

  /** Multiplicative constant to term matching probability. Non-adjustable by users. */
  private static final float M = 0.00781f;

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

  /** Controls non-linear term frequency normalization (saturation). */
  private final float k1;

  /** Controls how much term length affects term rarity. */
  private final float c;

  /**
   * Controls query-side term frequency saturation. A negative value disables saturation (linear
   * behavior).
   */
  private final float k3;

  /**
   * StableTFL with the supplied parameter values.
   *
   * @param k1 Controls non-linear term frequency normalization (saturation).
   * @param c Controls how much term length affects term rarity (decay constant).
   * @param k3 Controls query-side term frequency saturation. When duplicate terms appear in a
   *     query, their boost is computed as ((k3 + 1) * qtf) / (k3 + qtf) instead of linear summing.
   *     A negative value disables saturation (linear behavior). Common values are 7 or 8.
   * @throws IllegalArgumentException if {@code k1} is infinite or negative
   * @throws IllegalArgumentException if {@code c} is infinite or negative
   * @throws IllegalArgumentException if {@code k3} is infinite
   */
  public StableTflSimilarity(float k1, float c, float k3) {
    if (Float.isFinite(k1) == false || k1 < 0) {
      throw new IllegalArgumentException(
          "illegal k1 value: " + k1 + ", must be a non-negative finite value");
    }
    if (Float.isFinite(c) == false || c < 0) {
      throw new IllegalArgumentException(
          "illegal c value: " + c + ", must be a non-negative finite value");
    }
    if (Float.isFinite(k3) == false) {
      throw new IllegalArgumentException(
          "illegal k3 value: " + k3 + ", must be a finite value (negative to disable)");
    }
    this.k1 = k1;
    this.c = c;
    this.k3 = k3;
  }

  /**
   * StableTFL with the supplied parameter values and query-term frequency saturation disabled.
   *
   * @param k1 Controls non-linear term frequency normalization (saturation).
   * @param c Controls how much term length affects term rarity (decay constant).
   * @throws IllegalArgumentException if {@code k1} is infinite or negative
   * @throws IllegalArgumentException if {@code c} is infinite or negative
   */
  public StableTflSimilarity(float k1, float c) {
    this(k1, c, DEFAULT_K3);
  }

  /**
   * StableTFL with these default values:
   *
   * <ul>
   *   <li>{@code k1 = 1.2}
   *   <li>{@code c = 0.917}
   *   <li>{@code k3 = -1} (query-term frequency saturation disabled)
   * </ul>
   */
  public StableTflSimilarity() {
    this(DEFAULT_K1, DEFAULT_C, DEFAULT_K3);
  }

  /**
   * Computes the query-term weight using the same k3 saturation formula as BM25: ((k3 + 1) * qtf) /
   * (k3 + qtf). When k3 is negative (disabled), falls back to linear weighting where the weight
   * equals the query term frequency.
   */
  @Override
  public float computeQueryTermWeight(int queryTermFrequency) {
    if (k3 < 0) {
      return (float) queryTermFrequency;
    }
    return ((k3 + 1f) * queryTermFrequency) / (k3 + queryTermFrequency);
  }

  /**
   * Implemented as <code>log(1 + (1 - p + 0.05)/(p + 0.05))</code>, where <code>
   *  p = 1 - (1 - m * 2 ^ (-c * termLength)) ^ docLength </code>
   */
  private float tr(float termLength, float docLength) {
    // The probability that a term is present in a document with length docLength.
    double p = (1 - Math.pow(1 - M * Math.pow(2, -c * termLength), docLength));

    return (float) Math.log(1 + (1 - p + 0.05D) / (p + 0.05D));
  }

  /**
   * Returns the number of code points in this UTF8 sequence. If invalid codepoint header byte
   * occurs or the content is prematurely truncated, return NON_UTF_8_DEFAULT_TERM_LENGTH.
   */
  private static float getTermLength(BytesRef termText) {
    try {
      return UnicodeUtil.codePointCount(termText);
    } catch (IllegalArgumentException _) {
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

    // Fold the constant boost into the per-doc-length cache so score() doesn't repeat this
    // multiply for every scored document.
    for (int j = 0; j < cache.length; j++) {
      cache[j] *= boost;
    }

    return new TflScorer(boost, k1, c, termLengths, cache);
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
     * cache[i] stores the precomputed weight (boost times the summed term rarity (tr) of all query
     * terms) for a specific document length (LENGTH_TABLE[i]).
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
      float weight = cache[((byte) encodedNorm) & 0xFF];
      return weight - weight / (1 + freq / k1);
    }

    @Override
    public BulkSimScorer asBulkSimScorer() {
      return new BulkSimScorer() {

        private float[] weights = new float[0];

        @Override
        public void score(int size, float[] freqs, long[] norms, float[] scores) {
          if (weights.length < size) {
            weights = new float[ArrayUtil.oversize(size, Float.BYTES)];
          }
          // Scalar gather of the per-doc-length weight (boost * tr); this lookup can't vectorize.
          for (int i = 0; i < size; ++i) {
            weights[i] = cache[((byte) norms[i]) & 0xFF];
          }

          // This loop auto-vectorizes. Kept bit-identical to score() so asBulkSimScorer() agrees.
          for (int i = 0; i < size; ++i) {
            scores[i] = weights[i] - weights[i] / (1f + freqs[i] / k1);
          }
        }
      };
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      List<Explanation> subs = new ArrayList<>();
      subs.add(Explanation.match(boost, "boost"));
      subs.add(explainTF(freq));
      subs.add(explainTR(termLengths, norm));

      float weight = cache[((byte) norm) & 0xFF];
      return Explanation.match(
          weight - weight / (1f + freq.getValue().floatValue() / k1),
          "score(freq=" + freq.getValue() + "), computed as boost * tr * tf from:",
          subs);
    }

    private Explanation explainTR(float termLength, long norm) {
      float docLength = LENGTH_TABLE[((byte) norm) & 0xff];
      List<Explanation> subs = new ArrayList<>(explainTrConstantFactors());
      subs.add(Explanation.match(termLength, "tl, term length"));
      subs.add(Explanation.match(docLength, "dl, document length"));

      // The probability that a term is present in a document with length docLength.
      double p = (1 - Math.pow(1 - M * Math.pow(2, -c * termLength), docLength));
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
          Explanation.match(M, "m, multiplicative constant to term match probability"),
          Explanation.match(c, "c, decaying constant for term length"));
    }

    private Explanation explainTF(Explanation freq) {
      List<Explanation> subs =
          List.of(freq, Explanation.match(k1, "k1, term saturation parameter"));

      return Explanation.match(
          1f - 1f / (1 + freq.getValue().floatValue() / k1),
          "tf, computed as freq / (freq + k1) from:",
          subs);
    }
  }

  /**
   * Returns the <code>k1</code> parameter.
   *
   * @see #StableTflSimilarity(float, float)
   */
  public final float getK1() {
    return k1;
  }

  /**
   * Returns the <code>c</code> parameter.
   *
   * @see #StableTflSimilarity(float, float)
   */
  public final float getC() {
    return c;
  }

  /**
   * Returns the <code>k3</code> parameter for query-side term frequency saturation. A negative
   * value means saturation is disabled.
   */
  public final float getK3() {
    return k3;
  }

  @Override
  public String toString() {
    return "StableTflSimilarity(k1=" + k1 + ", c=" + c + (k3 >= 0 ? ", k3=" + k3 : "") + ")";
  }
}
