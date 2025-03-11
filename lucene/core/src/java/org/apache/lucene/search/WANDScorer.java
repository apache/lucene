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

import static org.apache.lucene.search.DisiPriorityQueueN.leftNode;
import static org.apache.lucene.search.DisiPriorityQueueN.parentNode;
import static org.apache.lucene.search.DisiPriorityQueueN.rightNode;
import static org.apache.lucene.search.ScorerUtil.costWithMinShouldMatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.util.MathUtil;

/**
 * This implements the WAND (Weak AND) algorithm for dynamic pruning described in "Efficient Query
 * Evaluation using a Two-Level Retrieval Process" by Broder, Carmel, Herscovici, Soffer and Zien.
 * Enhanced with techniques described in "Faster Top-k Document Retrieval Using Block-Max Indexes"
 * by Ding and Suel. For scoreMode == {@link ScoreMode#TOP_SCORES}, this scorer maintains a feedback
 * loop with the collector in order to know at any time the minimum score that is required in order
 * for a hit to be competitive.
 *
 * <p>The implementation supports both minCompetitiveScore by enforce that {@code ∑ max_score >=
 * minCompetitiveScore}, and minShouldMatch by enforcing {@code freq >= minShouldMatch}. It keeps
 * sub scorers in 3 different places: - tail: a heap that contains scorers that are behind the
 * desired doc ID. These scorers are ordered by cost so that we can advance the least costly ones
 * first. - lead: a linked list of scorer that are positioned on the desired doc ID - head: a heap
 * that contains scorers which are beyond the desired doc ID, ordered by doc ID in order to move
 * quickly to the next candidate.
 *
 * <p>When scoreMode == {@link ScoreMode#TOP_SCORES}, it leverages the {@link
 * Scorer#getMaxScore(int) max score} from each scorer in order to know when it may call {@link
 * DocIdSetIterator#advance} rather than {@link DocIdSetIterator#nextDoc} to move to the next
 * competitive hit. When scoreMode != {@link ScoreMode#TOP_SCORES}, block-max scoring related logic
 * is skipped. Finding the next match consists of first setting the desired doc ID to the least
 * entry in 'head', and then advance 'tail' until there is a match, by meeting the configured {@code
 * freq >= minShouldMatch} and / or {@code ∑ max_score >= minCompetitiveScore} requirements.
 */
final class WANDScorer extends Scorer {

  static final int FLOAT_MANTISSA_BITS = 24;
  private static final long MAX_SCALED_SCORE = (1L << 24) - 1;

  /**
   * Return a scaling factor for the given float so that {@code f x 2^scalingFactor} would be in
   * {@code [2^23, 2^24[}. Special cases:
   *
   * <pre>
   *    scalingFactor(0) = scalingFactor(MIN_VALUE) + 1
   *    scalingFactor(+Infty) = scalingFactor(MAX_VALUE) - 1
   * </pre>
   */
  static int scalingFactor(float f) {
    if (f < 0) {
      throw new IllegalArgumentException("Scores must be positive or null");
    } else if (f == 0) {
      return scalingFactor(Float.MIN_VALUE) + 1;
    } else if (Float.isInfinite(f)) {
      return scalingFactor(Float.MAX_VALUE) - 1;
    } else {
      double d = f;
      // Since doubles have more amplitude than floats for the
      // exponent, the cast produces a normal value.
      assert d == 0 || Math.getExponent(d) >= Double.MIN_EXPONENT; // normal double
      return FLOAT_MANTISSA_BITS - 1 - Math.getExponent(d);
    }
  }

  /**
   * Scale max scores in an unsigned integer to avoid overflows (only the lower 32 bits of the long
   * are used) as well as floating-point arithmetic errors. Those are rounded up in order to make
   * sure we do not miss any matches.
   */
  static long scaleMaxScore(float maxScore, int scalingFactor) {
    assert Float.isNaN(maxScore) == false;
    assert maxScore >= 0;

    // NOTE: because doubles have more amplitude than floats for the
    // exponent, the scalb call produces an accurate value.
    final double scaled = Math.scalb((double) maxScore, scalingFactor);

    if (scaled > MAX_SCALED_SCORE) {
      // This happens if one scorer returns +Infty as a max score, or if the scorer returns greater
      // max scores locally than globally - which shouldn't happen with well-behaved scorers
      return MAX_SCALED_SCORE;
    }

    return (long) Math.ceil(scaled); // round up, cast is accurate since value is < 2^24
  }

  /**
   * Scale min competitive scores the same way as max scores but this time by rounding down in order
   * to make sure that we do not miss any matches.
   */
  private static long scaleMinScore(float minScore, int scalingFactor) {
    assert Float.isFinite(minScore);
    assert minScore >= 0;

    // like for scaleMaxScore, this scalb call is accurate
    double scaled = Math.scalb((double) minScore, scalingFactor);
    // round down, cast might lower the value again if scaled > Long.MAX_VALUE,
    // which is fine
    return (long) Math.floor(scaled);
  }

  private final int scalingFactor;
  // scaled min competitive score
  private long minCompetitiveScore;

  private final Scorer[] allScorers;

  // list of scorers which 'lead' the iteration and are currently
  // positioned on 'doc'. This is sometimes called the 'pivot' in
  // some descriptions of WAND (Weak AND).
  DisiWrapper lead;
  int doc; // current doc ID of the leads
  double leadScore; // score of the leads

  // priority queue of scorers that are too advanced compared to the current
  // doc. Ordered by doc ID.
  final DisiPriorityQueue head;

  // priority queue of scorers which are behind the current doc.
  // Ordered by maxScore.
  final DisiWrapper[] tail;
  long tailMaxScore; // sum of the max scores of scorers in 'tail'
  int tailSize;

  final long cost;

  int upTo; // upper bound for which max scores are valid

  final int minShouldMatch;
  int freq;

  final ScoreMode scoreMode;
  final long leadCost;

  WANDScorer(Collection<Scorer> scorers, int minShouldMatch, ScoreMode scoreMode, long leadCost)
      throws IOException {

    if (minShouldMatch >= scorers.size()) {
      throw new IllegalArgumentException("minShouldMatch should be < the number of scorers");
    }

    allScorers = scorers.toArray(Scorer[]::new);
    this.minCompetitiveScore = 0;

    assert minShouldMatch >= 0 : "minShouldMatch should not be negative, but got " + minShouldMatch;
    this.minShouldMatch = minShouldMatch;

    this.doc = -1;
    this.upTo = -1; // will be computed on the first call to nextDoc/advance

    this.scoreMode = scoreMode;

    head = DisiPriorityQueue.ofMaxSize(scorers.size());
    // there can be at most num_scorers - 1 scorers beyond the current position
    tail = new DisiWrapper[scorers.size()];

    if (this.scoreMode == ScoreMode.TOP_SCORES) {
      // To avoid accuracy issues with floating-point numbers, this scorer operates on scaled longs.
      // How do you choose the scaling factor? The thing is that we want to retain as many
      // significant bits as possible, but not too many, otherwise operations on longs would be more
      // precise than the equivalent operations on their unscaled counterparts and we might skip too
      // many hits. So we compute the maximum possible score produced by this scorer, which is the
      // sum of the maximum scores of each clause, and compute a scaling factor that would preserve
      // 24 bits of accuracy - the number of mantissa bits of single-precision floating-point
      // numbers.
      double maxScoreSumDouble = 0;
      for (Scorer scorer : scorers) {
        scorer.advanceShallow(0);
        float maxScore = scorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS);
        maxScoreSumDouble += maxScore;
      }
      final float maxScoreSum = (float) MathUtil.sumUpperBound(maxScoreSumDouble, scorers.size());
      this.scalingFactor = scalingFactor(maxScoreSum);
    } else {
      this.scalingFactor = 0;
    }

    for (Scorer scorer : scorers) {
      // Ideally we would pass true when scoreMode == TOP_SCORES and false otherwise, but this would
      // break the optimization as there could then be 3 different impls of DocIdSetIterator
      // (ImpactsEnum, PostingsEnum and <Else>). So we pass true to favor disjunctions sorted by
      // descending score as opposed to non-scoring disjunctions whose minShouldMatch is greater
      // than 1.
      addUnpositionedLead(new DisiWrapper(scorer, true));
    }

    this.cost =
        costWithMinShouldMatch(
            scorers.stream().map(Scorer::iterator).mapToLong(DocIdSetIterator::cost),
            scorers.size(),
            minShouldMatch);
    this.leadCost = leadCost;
  }

  // returns a boolean so that it can be called from assert
  // the return value is useless: it always returns true
  private boolean ensureConsistent() throws IOException {
    if (scoreMode == ScoreMode.TOP_SCORES) {
      long maxScoreSum = 0;
      for (int i = 0; i < tailSize; ++i) {
        assert tail[i].doc < doc;
        maxScoreSum = Math.addExact(maxScoreSum, tail[i].scaledMaxScore);
      }
      assert maxScoreSum == tailMaxScore : maxScoreSum + " " + tailMaxScore;

      List<Float> leadScores = new ArrayList<>();
      for (DisiWrapper w = lead; w != null; w = w.next) {
        assert w.doc == doc;
        leadScores.add(w.scorable.score());
      }
      // Make sure to recompute the sum in the same order to get the same floating point rounding
      // errors.
      Collections.reverse(leadScores);
      double recomputedLeadScore = 0;
      for (float score : leadScores) {
        recomputedLeadScore += score;
      }
      assert recomputedLeadScore == leadScore;

      assert minCompetitiveScore == 0
          || tailMaxScore < minCompetitiveScore
          || tailSize < minShouldMatch;
      assert doc <= upTo;
    }

    for (DisiWrapper w : head) {
      if (lead == null) { // After calling advance() but before matches()
        assert w.doc >= doc;
      } else {
        assert w.doc > doc;
      }
    }

    return true;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    // Let this disjunction know about the new min score so that it can skip
    // over clauses that produce low scores.
    assert scoreMode == ScoreMode.TOP_SCORES
        : "minCompetitiveScore can only be set for ScoreMode.TOP_SCORES, but got: " + scoreMode;
    assert minScore >= 0;
    long scaledMinScore = scaleMinScore(minScore, scalingFactor);
    assert scaledMinScore >= minCompetitiveScore;
    minCompetitiveScore = scaledMinScore;
  }

  @Override
  public final Collection<ChildScorable> getChildren() throws IOException {
    List<ChildScorable> matchingChildren = new ArrayList<>();
    advanceAllTail();
    for (DisiWrapper s = lead; s != null; s = s.next) {
      matchingChildren.add(new ChildScorable(s.scorer, "SHOULD"));
    }
    return matchingChildren;
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    DocIdSetIterator approximation =
        new DocIdSetIterator() {

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }

          @Override
          public int advance(int target) throws IOException {
            // Move 'lead' iterators back to the tail
            pushBackLeads(target);

            // Make sure `head` is also on or beyond `target`
            DisiWrapper headTop = advanceHead(target);

            if (scoreMode == ScoreMode.TOP_SCORES && (headTop == null || headTop.doc > upTo)) {
              // Update score bounds if necessary
              moveToNextBlock(target);
              assert upTo >= target;
              headTop = head.top();
            }

            if (headTop == null) {
              return doc = DocIdSetIterator.NO_MORE_DOCS;
            } else {
              return doc = headTop.doc;
            }
          }

          @Override
          public long cost() {
            return cost;
          }
        };
    return new TwoPhaseIterator(approximation) {

      @Override
      public boolean matches() throws IOException {
        assert lead == null;
        moveToNextCandidate();

        long scaledLeadScore = 0;
        if (scoreMode == ScoreMode.TOP_SCORES) {
          scaledLeadScore =
              scaleMaxScore(
                  (float) MathUtil.sumUpperBound(leadScore, FLOAT_MANTISSA_BITS), scalingFactor);
        }

        while (scaledLeadScore < minCompetitiveScore || freq < minShouldMatch) {
          assert ensureConsistent();
          if (scaledLeadScore + tailMaxScore < minCompetitiveScore
              || freq + tailSize < minShouldMatch) {
            return false;
          } else {
            // a match on doc is still possible, try to
            // advance scorers from the tail
            DisiWrapper prevLead = lead;
            advanceTail();
            if (scoreMode == ScoreMode.TOP_SCORES && lead != prevLead) {
              assert prevLead == lead.next;
              scaledLeadScore =
                  scaleMaxScore(
                      (float) MathUtil.sumUpperBound(leadScore, FLOAT_MANTISSA_BITS),
                      scalingFactor);
            }
          }
        }

        assert ensureConsistent();
        return true;
      }

      @Override
      public float matchCost() {
        // maximum number of scorer that matches() might advance
        return tail.length;
      }
    };
  }

  /** Add a disi to the linked list of leads. */
  private void addLead(DisiWrapper lead) throws IOException {
    lead.next = this.lead;
    this.lead = lead;
    freq += 1;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      leadScore += lead.scorable.score();
    }
  }

  /** Add a disi to the linked list of leads. */
  private void addUnpositionedLead(DisiWrapper lead) {
    assert lead.doc == -1;
    lead.next = this.lead;
    this.lead = lead;
    freq += 1;
  }

  /** Move disis that are in 'lead' back to the tail. */
  private void pushBackLeads(int target) throws IOException {
    for (DisiWrapper s = lead; s != null; s = s.next) {
      final DisiWrapper evicted = insertTailWithOverFlow(s);
      if (evicted != null) {
        evicted.doc = evicted.iterator.advance(target);
        head.add(evicted);
      }
    }
    lead = null;
  }

  /** Make sure all disis in 'head' are on or after 'target'. */
  private DisiWrapper advanceHead(int target) throws IOException {
    DisiWrapper headTop = head.top();
    while (headTop != null && headTop.doc < target) {
      final DisiWrapper evicted = insertTailWithOverFlow(headTop);
      if (evicted != null) {
        evicted.doc = evicted.iterator.advance(target);
        headTop = head.updateTop(evicted);
      } else {
        head.pop();
        headTop = head.top();
      }
    }
    return headTop;
  }

  private void advanceTail(DisiWrapper disi) throws IOException {
    disi.doc = disi.iterator.advance(doc);
    if (disi.doc == doc) {
      addLead(disi);
    } else {
      head.add(disi);
    }
  }

  /**
   * Pop the entry from the 'tail' that has the greatest score contribution, advance it to the
   * current doc and then add it to 'lead' or 'head' depending on whether it matches.
   */
  private void advanceTail() throws IOException {
    final DisiWrapper top = popTail();
    advanceTail(top);
  }

  private void updateMaxScores(int target) throws IOException {
    int newUpTo = DocIdSetIterator.NO_MORE_DOCS;
    // If we have entries in 'head', we treat them all as leads and take the minimum of their next
    // block boundaries as a next boundary.
    // We don't take entries in 'tail' into account on purpose: 'tail' is supposed to contain the
    // least score contributors, and taking them into account might not move the boundary fast
    // enough, so we'll waste CPU re-computing the next boundary all the time.
    // Likewise, we ignore clauses whose cost is greater than the lead cost to avoid recomputing
    // per-window max scores over and over again. In the event when this makes us compute upTo as
    // NO_MORE_DOCS, this scorer will effectively implement WAND rather than block-max WAND.
    for (DisiWrapper w : head) {
      if (w.doc <= newUpTo && w.cost <= leadCost) {
        newUpTo = Math.min(w.scorer.advanceShallow(w.doc), newUpTo);
      }
    }
    // Only look at the tail if none of the `head` clauses had a block we could reuse and if its
    // cost is less than or equal to the lead cost.
    if (newUpTo == DocIdSetIterator.NO_MORE_DOCS && tailSize > 0 && tail[0].cost <= leadCost) {
      newUpTo = tail[0].scorer.advanceShallow(target);
      // upTo must be on or after the least `head` doc
      DisiWrapper headTop = head.top();
      if (headTop != null) {
        newUpTo = Math.max(newUpTo, headTop.doc);
      }
    }
    upTo = newUpTo;

    // Now update the max scores of clauses that are before upTo.
    for (DisiWrapper w : head) {
      if (w.doc <= upTo) {
        w.scaledMaxScore = scaleMaxScore(w.scorer.getMaxScore(newUpTo), scalingFactor);
      }
    }

    tailMaxScore = 0;
    for (int i = 0; i < tailSize; ++i) {
      DisiWrapper w = tail[i];
      w.scorer.advanceShallow(target);
      w.scaledMaxScore = scaleMaxScore(w.scorer.getMaxScore(upTo), scalingFactor);
      upHeapMaxScore(tail, i); // the heap might need to be reordered
      tailMaxScore += w.scaledMaxScore;
    }

    // We need to make sure that entries in 'tail' alone cannot match
    // a competitive hit.
    while (tailSize > 0 && tailMaxScore >= minCompetitiveScore) {
      DisiWrapper w = popTail();
      w.doc = w.iterator.advance(target);
      head.add(w);
    }
  }

  /**
   * Update {@code upTo} and maximum scores of sub scorers so that {@code upTo} is greater than or
   * equal to the next candidate after {@code target}, i.e. the top of `head`.
   */
  private void moveToNextBlock(int target) throws IOException {
    assert lead == null;

    while (upTo < DocIdSetIterator.NO_MORE_DOCS) {
      if (head.size() == 0) {
        // All clauses could fit in the tail, which means that the sum of the
        // maximum scores of sub clauses is less than the minimum competitive score.
        // Move to the next block until this condition becomes false.
        target = Math.max(target, upTo + 1);
        updateMaxScores(target);
      } else if (head.top().doc > upTo) {
        // We have a next candidate but it's not in the current block. We need to
        // move to the next block in order to not miss any potential hits between
        // `target` and `head.top().doc`.
        assert head.top().doc >= target;
        updateMaxScores(target);
        break;
      } else {
        break;
      }
    }

    assert head.size() == 0 || head.top().doc <= upTo;
    assert upTo >= target;
  }

  /**
   * Set 'doc' to the next potential match, and move all disis of 'head' that are on this doc into
   * 'lead'.
   */
  private void moveToNextCandidate() throws IOException {
    // The top of `head` defines the next potential match
    // pop all documents which are on this doc
    lead = head.pop();
    assert doc == lead.doc;
    lead.next = null;
    freq = 1;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      leadScore = lead.scorable.score();
    }
    while (head.size() > 0 && head.top().doc == doc) {
      addLead(head.pop());
    }
  }

  /** Advance all entries from the tail to know about all matches on the current doc. */
  private void advanceAllTail() throws IOException {
    // we return the next doc when the sum of the scores of the potential
    // matching clauses is high enough but some of the clauses in 'tail' might
    // match as well
    // since we are advancing all clauses in tail, we just iterate the array
    // without reorganizing the PQ
    for (int i = tailSize - 1; i >= 0; --i) {
      advanceTail(tail[i]);
    }
    tailSize = 0;
    tailMaxScore = 0;
    assert ensureConsistent();
  }

  @Override
  public float score() throws IOException {
    // we need to know about all matches
    advanceAllTail();

    double leadScore = this.leadScore;
    if (scoreMode != ScoreMode.TOP_SCORES) {
      // With TOP_SCORES, the score was already computed on the fly.
      for (DisiWrapper s = lead; s != null; s = s.next) {
        leadScore += s.scorable.score();
      }
    }
    return (float) leadScore;
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    // Propagate to improve score bounds
    for (Scorer scorer : allScorers) {
      if (scorer.docID() < target) {
        scorer.advanceShallow(target);
      }
    }
    if (target <= upTo) {
      return upTo;
    }
    // TODO: implement
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double maxScoreSum = 0;
    for (Scorer scorer : allScorers) {
      if (scorer.docID() <= upTo) {
        maxScoreSum += scorer.getMaxScore(upTo);
      }
    }
    return (float) MathUtil.sumUpperBound(maxScoreSum, allScorers.length);
  }

  @Override
  public int docID() {
    return doc;
  }

  /** Insert an entry in 'tail' and evict the least-costly scorer if full. */
  private DisiWrapper insertTailWithOverFlow(DisiWrapper s) {
    if (tailMaxScore + s.scaledMaxScore < minCompetitiveScore || tailSize + 1 < minShouldMatch) {
      // we have free room for this new entry
      addTail(s);
      tailMaxScore += s.scaledMaxScore;
      return null;
    } else if (tailSize == 0) {
      return s;
    } else {
      final DisiWrapper top = tail[0];
      if (greaterMaxScore(top, s) == false) {
        return s;
      }
      // Swap top and s
      tail[0] = s;
      downHeapMaxScore(tail, tailSize);
      tailMaxScore = tailMaxScore - top.scaledMaxScore + s.scaledMaxScore;
      return top;
    }
  }

  /** Add an entry to 'tail'. Fails if over capacity. */
  private void addTail(DisiWrapper s) {
    tail[tailSize] = s;
    upHeapMaxScore(tail, tailSize);
    tailSize += 1;
  }

  /** Pop the least-costly scorer from 'tail'. */
  private DisiWrapper popTail() {
    assert tailSize > 0;
    final DisiWrapper result = tail[0];
    tail[0] = tail[--tailSize];
    downHeapMaxScore(tail, tailSize);
    tailMaxScore -= result.scaledMaxScore;
    return result;
  }

  /** Heap helpers */
  private static void upHeapMaxScore(DisiWrapper[] heap, int i) {
    final DisiWrapper node = heap[i];
    int j = parentNode(i);
    while (j >= 0 && greaterMaxScore(node, heap[j])) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  private static void downHeapMaxScore(DisiWrapper[] heap, int size) {
    int i = 0;
    final DisiWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && greaterMaxScore(heap[k], heap[j])) {
        j = k;
      }
      if (greaterMaxScore(heap[j], node)) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && greaterMaxScore(heap[k], heap[j])) {
            j = k;
          }
        } while (j < size && greaterMaxScore(heap[j], node));
        heap[i] = node;
      }
    }
  }

  /**
   * In the tail, we want to get first entries that produce the maximum scores and in case of ties
   * (eg. constant-score queries), those that have the least cost so that they are likely to advance
   * further.
   */
  private static boolean greaterMaxScore(DisiWrapper w1, DisiWrapper w2) {
    if (w1.scaledMaxScore > w2.scaledMaxScore) {
      return true;
    } else if (w1.scaledMaxScore < w2.scaledMaxScore) {
      return false;
    } else {
      return w1.cost < w2.cost;
    }
  }
}
