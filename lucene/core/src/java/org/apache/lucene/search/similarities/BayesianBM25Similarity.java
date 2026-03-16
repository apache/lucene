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
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SmallFloat;

/**
 * Bayesian BM25 Similarity. Transforms BM25 scores into calibrated probabilities via Bayesian
 * inference, as described in "Bayesian BM25". The transformation applies a sigmoid likelihood
 * function and a term-frequency-based prior, combining them through Bayes' rule to produce
 * posterior probabilities in (0, 1).
 *
 * <p>The posterior is computed as:
 *
 * <pre>
 *   likelihood = sigmoid(alpha * (bm25 - beta))
 *   tfPrior    = clamp(base + range * min(1, freq / saturation), 0.1, 0.9)
 *   posterior  = (likelihood * tfPrior) / (likelihood * tfPrior + (1 - likelihood) * (1 - tfPrior))
 * </pre>
 *
 * <p>When alpha and beta are null (the default), they are auto-estimated from the IDF weight:
 *
 * <pre>
 *   effectiveAlpha = 2.0 / max(weight, 1e-6)
 *   effectiveBeta  = weight * 0.5
 * </pre>
 *
 * <p>The tf-only prior (rather than composite prior with norm_prior) is used to preserve Lucene's
 * SimScorer monotonicity contract: scores must not increase when unsigned norm increases.
 */
public class BayesianBM25Similarity extends Similarity {
  private final float k1;
  private final float b;
  private final Float alpha;
  private final Float beta;

  /**
   * BayesianBM25 with the supplied parameter values.
   *
   * @param k1 Controls non-linear term frequency normalization (saturation).
   * @param b Controls to what degree document length normalizes tf values.
   * @param alpha Steepness of the sigmoid likelihood function. Null for auto-estimation.
   * @param beta Midpoint of the sigmoid likelihood function. Null for auto-estimation.
   * @param discountOverlaps True if overlap tokens are discounted from the document's length.
   * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is not
   *     within the range {@code [0..1]}
   */
  public BayesianBM25Similarity(
      float k1, float b, Float alpha, Float beta, boolean discountOverlaps) {
    super(discountOverlaps);
    if (Float.isFinite(k1) == false || k1 < 0) {
      throw new IllegalArgumentException(
          "illegal k1 value: " + k1 + ", must be a non-negative finite value");
    }
    if (Float.isNaN(b) || b < 0 || b > 1) {
      throw new IllegalArgumentException("illegal b value: " + b + ", must be between 0 and 1");
    }
    if (alpha != null && (Float.isFinite(alpha) == false || alpha <= 0)) {
      throw new IllegalArgumentException(
          "illegal alpha value: " + alpha + ", must be a positive finite value");
    }
    if (beta != null && Float.isFinite(beta) == false) {
      throw new IllegalArgumentException(
          "illegal beta value: " + beta + ", must be a finite value");
    }
    this.k1 = k1;
    this.b = b;
    this.alpha = alpha;
    this.beta = beta;
  }

  /**
   * BayesianBM25 with the supplied parameter values and auto-estimated alpha/beta.
   *
   * @param k1 Controls non-linear term frequency normalization (saturation).
   * @param b Controls to what degree document length normalizes tf values.
   * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is not
   *     within the range {@code [0..1]}
   */
  public BayesianBM25Similarity(float k1, float b) {
    this(k1, b, null, null, true);
  }

  /** BayesianBM25 with default BM25 parameters (k1=1.2, b=0.75) and auto-estimated alpha/beta. */
  public BayesianBM25Similarity() {
    this(1.2f, 0.75f, null, null, true);
  }

  /** Implemented as <code>log(1 + (docCount - docFreq + 0.5)/(docFreq + 0.5))</code>. */
  protected float idf(long docFreq, long docCount) {
    return (float) Math.log(1 + (docCount - docFreq + 0.5D) / (docFreq + 0.5D));
  }

  /** The default implementation computes the average as <code>sumTotalTermFreq / docCount</code> */
  protected float avgFieldLength(CollectionStatistics collectionStats) {
    return (float) (collectionStats.sumTotalTermFreq() / (double) collectionStats.docCount());
  }

  /** Cache of decoded bytes. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  /**
   * Computes a score factor for a simple term and returns an explanation for that score factor.
   *
   * <p>The default implementation uses:
   *
   * <pre><code class="language-java">
   * idf(docFreq, docCount);
   * </code></pre>
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
    final long df = termStats.docFreq();
    final long docCount = collectionStats.docCount();
    final float idf = idf(df, docCount);
    return Explanation.match(
        idf,
        "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
        Explanation.match(df, "n, number of documents containing term"),
        Explanation.match(docCount, "N, total number of documents with field"));
  }

  /**
   * Computes a score factor for a phrase.
   *
   * <p>The default implementation sums the idf factor for each term in the phrase.
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] termStats) {
    double idf = 0d;
    List<Explanation> details = new ArrayList<>();
    for (final TermStatistics stat : termStats) {
      Explanation idfExplain = idfExplain(collectionStats, stat);
      details.add(idfExplain);
      idf += idfExplain.getValue().floatValue();
    }
    return Explanation.match((float) idf, "idf, sum of:", details);
  }

  /** Numerator for auto-estimated alpha: effectiveAlpha = AUTO_ALPHA_NUMERATOR / weight. */
  private static final float AUTO_ALPHA_NUMERATOR = 2.0f;

  /** Multiplier for auto-estimated beta: effectiveBeta = weight * AUTO_BETA_MULTIPLIER. */
  private static final float AUTO_BETA_MULTIPLIER = 0.5f;

  /** Floor for weight to avoid division by zero in auto-alpha estimation. */
  private static final float WEIGHT_FLOOR = 1e-6f;

  @Override
  public final SimScorer scorer(
      float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    Explanation idf =
        termStats.length == 1
            ? idfExplain(collectionStats, termStats[0])
            : idfExplain(collectionStats, termStats);
    float avgdl = avgFieldLength(collectionStats);

    float[] cache = new float[256];
    for (int i = 0; i < cache.length; i++) {
      cache[i] = 1f / (k1 * ((1 - b) + b * LENGTH_TABLE[i] / avgdl));
    }

    float weight = boost * idf.getValue().floatValue();

    float effectiveAlpha =
        (alpha != null) ? alpha : AUTO_ALPHA_NUMERATOR / Math.max(weight, WEIGHT_FLOOR);
    float effectiveBeta = (beta != null) ? beta : weight * AUTO_BETA_MULTIPLIER;

    return new BayesianBM25Scorer(
        boost, k1, b, idf, avgdl, cache, weight, effectiveAlpha, effectiveBeta);
  }

  /**
   * Numerically stable sigmoid function.
   *
   * <p>Uses the split formulation to avoid overflow:
   *
   * <pre>
   *   x >= 0: 1 / (1 + exp(-x))
   *   x <  0: exp(x) / (1 + exp(x))
   * </pre>
   */
  static float sigmoid(float x) {
    if (x >= 0) {
      return (float) (1.0 / (1.0 + Math.exp(-x)));
    } else {
      double expX = Math.exp(x);
      return (float) (expX / (1.0 + expX));
    }
  }

  /** Scorer for BayesianBM25. */
  private static class BayesianBM25Scorer extends SimScorer {
    /** Minimum prior probability when term frequency is zero. */
    private static final float TF_PRIOR_BASE = 0.2f;

    /** Maximum prior probability when term frequency reaches saturation. */
    private static final float TF_PRIOR_MAX = 0.9f;

    /** Derived range: TF_PRIOR_MAX - TF_PRIOR_BASE. */
    private static final float TF_PRIOR_RANGE = TF_PRIOR_MAX - TF_PRIOR_BASE;

    /** Term frequency at which the prior saturates to TF_PRIOR_MAX. */
    private static final float TF_SATURATION_FREQ = 10f;

    /** Absolute lower bound for the prior probability (safety clamp). */
    private static final float PRIOR_CLAMP_MIN = 0.1f;

    /** Absolute upper bound for the prior probability (safety clamp). */
    private static final float PRIOR_CLAMP_MAX = 0.9f;

    private final float boost;
    private final float k1;
    private final float b;
    private final Explanation idf;
    private final float avgdl;
    private final float[] cache;
    private final float weight;
    private final float effectiveAlpha;
    private final float effectiveBeta;

    BayesianBM25Scorer(
        float boost,
        float k1,
        float b,
        Explanation idf,
        float avgdl,
        float[] cache,
        float weight,
        float effectiveAlpha,
        float effectiveBeta) {
      this.boost = boost;
      this.k1 = k1;
      this.b = b;
      this.idf = idf;
      this.avgdl = avgdl;
      this.cache = cache;
      this.weight = weight;
      this.effectiveAlpha = effectiveAlpha;
      this.effectiveBeta = effectiveBeta;
    }

    private float computeBM25(float freq, float normInverse) {
      // Monotonicity-preserving formulation (same as BM25Similarity)
      return weight - weight / (1f + freq * normInverse);
    }

    private double computeTfPrior(double freq) {
      double raw = TF_PRIOR_BASE + TF_PRIOR_RANGE * Math.min(1.0, freq / TF_SATURATION_FREQ);
      return Math.max(PRIOR_CLAMP_MIN, Math.min(PRIOR_CLAMP_MAX, raw));
    }

    private float doScore(float freq, float normInverse) {
      float bm25 = computeBM25(freq, normInverse);
      // Use double for the Bayesian transform to avoid float rounding errors
      // that could break monotonicity (posterior involves L*p / (L*p + (1-L)*(1-p))
      // where small rounding differences in the products can invert ordering).
      double L = sigmoid(effectiveAlpha * (bm25 - effectiveBeta));
      double p = computeTfPrior(freq);
      double numerator = L * p;
      double denominator = numerator + (1.0 - L) * (1.0 - p);
      return (float) (numerator / denominator);
    }

    @Override
    public float score(float freq, long encodedNorm) {
      float normInverse = cache[((byte) encodedNorm) & 0xFF];
      return doScore(freq, normInverse);
    }

    @Override
    public BulkSimScorer asBulkSimScorer() {
      return new BulkSimScorer() {

        private float[] normInverses = new float[0];

        @Override
        public void score(int size, float[] freqs, long[] norms, float[] scores) {
          if (normInverses.length < size) {
            normInverses = new float[ArrayUtil.oversize(size, Float.BYTES)];
          }
          for (int i = 0; i < size; ++i) {
            normInverses[i] = cache[((byte) norms[i]) & 0xFF];
          }

          for (int i = 0; i < size; ++i) {
            scores[i] = doScore(freqs[i], normInverses[i]);
          }
        }
      };
    }

    @Override
    public Explanation explain(Explanation freq, long encodedNorm) {
      float normInverse = cache[((byte) encodedNorm) & 0xFF];
      float freqValue = freq.getValue().floatValue();

      float bm25 = computeBM25(freqValue, normInverse);
      // Use the same double-precision computation as doScore to ensure the
      // explanation value matches the score exactly.
      double L = sigmoid(effectiveAlpha * (bm25 - effectiveBeta));
      double p = computeTfPrior(freqValue);
      double numerator = L * p;
      double denominator = numerator + (1.0 - L) * (1.0 - p);
      float likelihood = (float) L;
      float tfPrior = (float) p;
      float posterior = (float) (numerator / denominator);

      // BM25 sub-explanation
      List<Explanation> bm25Subs = new ArrayList<>(explainConstantFactors());
      bm25Subs.add(explainTF(freq, encodedNorm));
      Explanation bm25Expl =
          Explanation.match(bm25, "bm25, computed as boost * idf * tf from:", bm25Subs);

      // Likelihood sub-explanation
      Explanation likelihoodExpl =
          Explanation.match(
              likelihood,
              "likelihood, computed as sigmoid(alpha * (bm25 - beta)) from:",
              Explanation.match(effectiveAlpha, "alpha, sigmoid steepness"),
              Explanation.match(effectiveBeta, "beta, sigmoid midpoint"),
              bm25Expl);

      // TF prior sub-explanation
      Explanation tfPriorExpl =
          Explanation.match(
              tfPrior,
              "tfPrior, computed as clamp(base + range * min(1, freq / saturation), "
                  + PRIOR_CLAMP_MIN
                  + ", "
                  + PRIOR_CLAMP_MAX
                  + ") from:",
              freq);

      return Explanation.match(
          posterior,
          "score(freq=" + freqValue + "), computed as Bayesian posterior from:",
          likelihoodExpl,
          tfPriorExpl);
    }

    private List<Explanation> explainConstantFactors() {
      List<Explanation> subs = new ArrayList<>();
      if (boost != 1.0f) {
        subs.add(Explanation.match(boost, "boost"));
      }
      subs.add(idf);
      return subs;
    }

    private Explanation explainTF(Explanation freq, long norm) {
      List<Explanation> subs = new ArrayList<>();
      subs.add(freq);
      subs.add(Explanation.match(k1, "k1, term saturation parameter"));
      float doclen = LENGTH_TABLE[((byte) norm) & 0xff];
      subs.add(Explanation.match(b, "b, length normalization parameter"));
      if ((norm & 0xFF) > 39) {
        subs.add(Explanation.match(doclen, "dl, length of field (approximate)"));
      } else {
        subs.add(Explanation.match(doclen, "dl, length of field"));
      }
      subs.add(Explanation.match(avgdl, "avgdl, average length of field"));
      float normInverse = 1f / (k1 * ((1 - b) + b * doclen / avgdl));
      return Explanation.match(
          1f - 1f / (1 + freq.getValue().floatValue() * normInverse),
          "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
          subs);
    }
  }

  @Override
  public String toString() {
    return "BayesianBM25(k1=" + k1 + ",b=" + b + ",alpha=" + alpha + ",beta=" + beta + ")";
  }

  /** Returns the <code>k1</code> parameter. */
  public final float getK1() {
    return k1;
  }

  /** Returns the <code>b</code> parameter. */
  public final float getB() {
    return b;
  }

  /** Returns the <code>alpha</code> parameter, or null if auto-estimated. */
  public final Float getAlpha() {
    return alpha;
  }

  /** Returns the <code>beta</code> parameter, or null if auto-estimated. */
  public final Float getBeta() {
    return beta;
  }
}
