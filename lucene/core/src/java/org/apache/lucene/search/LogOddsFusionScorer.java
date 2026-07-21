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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Scorer for {@link LogOddsFusionQuery}. Combines sub-scorer outputs (assumed to be probabilities
 * in (0, 1)) via log-odds fusion with multiplicative confidence scaling.
 *
 * <p>The default scoring formula is:
 *
 * <pre>
 *   evidence    = logit(clamp(subScore))             for each matching sub-scorer
 *   logitSum    = sum of evidence values
 *   meanLogit   = logitSum / n   (n = total clause count, not just matching)
 *   scaledLogit = meanLogit * pow(n, alpha)
 *   score       = sigmoid(scaledLogit)
 * </pre>
 *
 * <p>Evidence logits may be negative internally, while the final score remains a non-negative
 * probability because it is passed through a sigmoid. Non-matching sub-scorers contribute zero
 * (neutral evidence). Optional softplus gating is available through {@link
 * LogOddsFusionQuery.Gating#SOFTPLUS} when callers explicitly want to discard negative evidence.
 *
 * @lucene.experimental
 */
final class LogOddsFusionScorer extends DisjunctionScorer {
  private final List<Scorer> subScorers;
  private final LogOddsFusionScoreFunction scoreFunction;
  private final IdentityHashMap<Scorer, Integer> scorerIndexMap;

  /**
   * Creates the appropriate fused scorer for the signals that are active on a leaf and adds fused
   * block-max skipping when scores are collected competitively.
   */
  static Scorer create(
      List<Scorer> subScorers,
      LogOddsFusionScoreFunction scoreFunction,
      ScoreMode scoreMode,
      long leadCost)
      throws IOException {
    Scorer fusionScorer =
        subScorers.size() == 1
            ? new SingletonLogOddsFusionScorer(subScorers.get(0), scoreFunction)
            : new LogOddsFusionScorer(subScorers, scoreFunction, scoreMode, leadCost);
    return scoreMode == ScoreMode.TOP_SCORES ? wrapWithBlockMax(fusionScorer) : fusionScorer;
  }

  /** Wraps a fused scorer with block-max skipping at the final-score layer. */
  static Scorer wrapWithBlockMax(Scorer scorer) {
    return new BlockMaxLogOddsFusionScorer(scorer);
  }

  /**
   * Creates a scorer for a disjunction of at least two active signals.
   *
   * @param subScorers the sub scorers to combine
   * @param scoreFunction shared function that transforms and pools sub-scores
   * @param scoreMode the score mode
   * @param leadCost the lead cost for iteration
   */
  private LogOddsFusionScorer(
      List<Scorer> subScorers,
      LogOddsFusionScoreFunction scoreFunction,
      ScoreMode scoreMode,
      long leadCost)
      throws IOException {
    super(subScorers, scoreMode, leadCost);
    this.subScorers = subScorers;
    this.scoreFunction = scoreFunction;
    this.scorerIndexMap = new IdentityHashMap<>(subScorers.size());
    for (int i = 0; i < subScorers.size(); i++) {
      this.scorerIndexMap.put(subScorers.get(i), i);
    }
  }

  @Override
  protected float score(DisiWrapper topList) throws IOException {
    double logitSum = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      int signalIndex = scorerIndexMap.get(w.scorer);
      logitSum += scoreFunction.contribution(w.scorable.score(), signalIndex);
    }
    return scoreFunction.score(logitSum);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    int upTo = DocIdSetIterator.NO_MORE_DOCS;
    for (Scorer scorer : subScorers) {
      if (scorer.docID() <= target) {
        upTo = Math.min(upTo, scorer.advanceShallow(target));
      } else if (scorer.docID() != DocIdSetIterator.NO_MORE_DOCS) {
        upTo = Math.min(upTo, scorer.docID() - 1);
      }
    }
    return upTo;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double maxLogitSum = 0;
    for (int i = 0; i < subScorers.size(); i++) {
      Scorer scorer = subScorers.get(i);
      if (scorer.docID() <= upTo) {
        maxLogitSum += scoreFunction.maxContribution(scorer.getMaxScore(upTo), i);
      }
    }
    return Math.nextUp(scoreFunction.score(maxLogitSum));
  }

  /**
   * Applies the full log-odds fusion transform on a leaf where only one signal has a scorer.
   *
   * <p>A single active scorer does not make fusion an identity operation. The original query may
   * still contain additional signals that are absent on this leaf, and those signals must keep
   * their neutral contributions while remaining part of uniform averaging and {@code pow(n, alpha)}
   * confidence scaling. The active signal may also have a weight, normalization bounds, a base-rate
   * correction, or negative evidence, and the fused result still needs the target prior and outer
   * query boost. Returning the child scorer directly would bypass all of these semantics and make
   * the same document score differently depending on segment layout.
   *
   * <p>{@link LogOddsFusionQuery} narrows per-signal metadata to the sole active signal before it
   * invokes {@link LogOddsFusionScorer#create}, so that signal is addressed as index {@code 0} in
   * the shared {@link LogOddsFusionScoreFunction}. Iteration and two-phase matching are inherited
   * from {@link FilterScorer}; only score transformation and the corresponding block-max bound are
   * overridden. For {@link ScoreMode#TOP_SCORES}, {@link LogOddsFusionScorer#create} wraps this
   * scorer in {@link BlockMaxLogOddsFusionScorer} so the minimum competitive score remains at the
   * fused-score layer.
   *
   * @lucene.experimental
   */
  private static final class SingletonLogOddsFusionScorer extends FilterScorer {
    private final LogOddsFusionScoreFunction scoreFunction;

    /** Creates a singleton fusion view over the only scorer active on the current leaf. */
    private SingletonLogOddsFusionScorer(Scorer scorer, LogOddsFusionScoreFunction scoreFunction) {
      super(scorer);
      this.scoreFunction = scoreFunction;
    }

    @Override
    public float score() throws IOException {
      return scoreFunction.score(scoreFunction.contribution(in.score(), 0));
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      // The signal may be absent, so zero is also a possible contribution. Round upward after the
      // complete fusion transform to keep the returned block bound conservative.
      return Math.nextUp(
          scoreFunction.score(scoreFunction.maxContribution(in.getMaxScore(upTo), 0)));
    }
  }

  /**
   * Adds block-max skipping to a scorer that already computes a fused log-odds score.
   *
   * <p>This wrapper is used for {@link ScoreMode#TOP_SCORES} searches. Unlike a sum of non-negative
   * clause scores, log-odds fusion may treat a matching signal as negative evidence when its
   * probability is below the neutral point. Propagating the minimum competitive score to an
   * individual signal could then prune that match and turn its negative contribution into the
   * neutral contribution of an absent signal, which may incorrectly increase the fused score.
   *
   * <p>Instead, the minimum competitive score is kept at the fused-score layer. For each candidate
   * block, this scorer asks the wrapped scorer to {@link #advanceShallow(int) shallow-advance} all
   * active signals and obtains an upper bound for the fully fused score from {@link
   * #getMaxScore(int)}. The inclusive block is skipped only when this bound is strictly less than
   * the minimum competitive score; equality remains competitive. The threshold is deliberately not
   * forwarded to the wrapped scorer or any of its children.
   *
   * <p>If the wrapped scorer has a {@link TwoPhaseIterator}, block skipping is applied to its
   * approximation and the original {@link TwoPhaseIterator#matches()} check is preserved for the
   * remaining candidate documents.
   *
   * @lucene.experimental
   */
  private static final class BlockMaxLogOddsFusionScorer extends Scorer {
    private final Scorer in;
    private final DocIdSetIterator approximation;
    private final TwoPhaseIterator twoPhase;
    private final DocIdSetIterator iterator;
    private float minCompetitiveScore;

    private BlockMaxLogOddsFusionScorer(Scorer in) {
      this.in = in;
      TwoPhaseIterator innerTwoPhase = in.twoPhaseIterator();
      DocIdSetIterator innerApproximation =
          innerTwoPhase == null ? in.iterator() : innerTwoPhase.approximation();
      this.approximation = blockSkippingIterator(innerApproximation);
      if (innerTwoPhase == null) {
        this.twoPhase = null;
        this.iterator = approximation;
      } else {
        // Replace only the approximation with the block-skipping view. Exact match verification and
        // its cost remain properties of the wrapped scorer.
        this.twoPhase =
            new TwoPhaseIterator(approximation) {
              @Override
              public boolean matches() throws IOException {
                return innerTwoPhase.matches();
              }

              @Override
              public float matchCost() {
                return innerTwoPhase.matchCost();
              }
            };
        this.iterator = TwoPhaseIterator.asDocIdSetIterator(twoPhase);
      }
    }

    private DocIdSetIterator blockSkippingIterator(DocIdSetIterator innerApproximation) {
      return new FilterDocIdSetIterator(innerApproximation) {
        // Inclusive end and fused-score upper bound of the shallow block that contains the current
        // target. The initial values force the first competitive-score lookup to load a block.
        private int upTo = -1;
        private float maxScore = Float.POSITIVE_INFINITY;

        private void moveToNextBlock(int target) throws IOException {
          // The wrapped fusion scorer shallow-advances all active signals before computing the
          // final fused-score bound. No child receives the minimum competitive score.
          upTo = BlockMaxLogOddsFusionScorer.this.advanceShallow(target);
          maxScore = BlockMaxLogOddsFusionScorer.this.getMaxScore(upTo);
        }

        private int advanceTarget(int target) throws IOException {
          // Lucene scores are non-negative, so a zero threshold cannot make any block
          // non-competitive and does not justify the cost of loading shallow bounds.
          if (target == NO_MORE_DOCS || minCompetitiveScore == 0f) {
            return target;
          }
          if (target > upTo) {
            moveToNextBlock(target);
          }
          // upTo is inclusive. Use a strict comparison because documents whose score equals the
          // threshold are still competitive.
          while (maxScore < minCompetitiveScore) {
            if (upTo == NO_MORE_DOCS) {
              return NO_MORE_DOCS;
            }
            target = upTo + 1;
            moveToNextBlock(target);
          }
          return target;
        }

        @Override
        public int nextDoc() throws IOException {
          return advance(docID() + 1);
        }

        @Override
        public int advance(int target) throws IOException {
          while (true) {
            target = advanceTarget(target);
            if (target == NO_MORE_DOCS) {
              return in.advance(target);
            }
            int doc = in.docID() < target ? in.advance(target) : in.docID();
            if (doc == NO_MORE_DOCS || minCompetitiveScore == 0f || doc <= upTo) {
              return doc;
            }
            // The approximation may jump beyond the block whose bound was just checked. Reload the
            // bound for the block containing the document before accepting it as a candidate.
            target = doc;
          }
        }
      };
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return twoPhase;
    }

    @Override
    public float score() throws IOException {
      return in.score();
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) {
      assert minScore >= minCompetitiveScore;
      // Keep the threshold here: forwarding it would let a child scorer erase negative evidence.
      minCompetitiveScore = Math.max(minCompetitiveScore, minScore);
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
      return in.getChildren();
    }
  }
}
