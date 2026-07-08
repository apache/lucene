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

import java.util.Random;
import org.apache.lucene.tests.search.RandomApproximationQuery.RandomTwoPhaseView;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * Tests the default (non-overridden) {@link TwoPhaseIterator#applyMask} implementation, using
 * {@link RandomTwoPhaseView} as a generic two-phase iterator whose approximation is a random
 * superset of the true matches -- exactly the shape {@code applyMask} must handle correctly without
 * any subclass-specific bulk optimization. {@link RandomTwoPhaseView} itself asserts that {@code
 * matches()} is never called on an unpositioned doc and never called twice for the same doc, so
 * these tests also validate that the default implementation drives the two-phase protocol
 * correctly, not just that it produces the right bits.
 */
public class TestTwoPhaseIterator extends LuceneTestCase {

  private static FixedBitSet randomBitSet(Random rng, int numDocs, double density) {
    FixedBitSet bitSet = new FixedBitSet(numDocs);
    for (int d = 0; d < numDocs; d++) {
      if (rng.nextDouble() < density) {
        bitSet.set(d);
      }
    }
    return bitSet;
  }

  /**
   * The default applyMask must match a linear scan restricted to the initial candidates, across
   * random true-match and candidate densities.
   */
  public void testApplyMaskMatchesLinearScanRestrictedToCandidates() throws Exception {
    Random rng = random();
    for (int iter = 0; iter < 20; iter++) {
      int numDocs = TestUtil.nextInt(rng, 100, 5000);
      double matchDensity = rng.nextDouble();
      double candidateDensity = rng.nextDouble();

      FixedBitSet trueMatches = randomBitSet(rng, numDocs, matchDensity);
      FixedBitSet candidates = randomBitSet(rng, numDocs, candidateDensity);

      FixedBitSet expected = candidates.clone();
      expected.and(trueMatches);

      TwoPhaseIterator twoPhase =
          new RandomTwoPhaseView(
              rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
      FixedBitSet actual = candidates.clone();
      twoPhase.applyMask(numDocs, actual, 0);

      assertEquals(
          "applyMask must match linear scan restricted to candidates (numDocs="
              + numDocs
              + ", matchDensity="
              + matchDensity
              + ", candidateDensity="
              + candidateDensity
              + ")",
          expected,
          actual);
    }
  }

  /** The default applyMask must never set a bit that wasn't already a candidate. */
  public void testApplyMaskNeverSetsBitsOutsideInitialCandidates() throws Exception {
    Random rng = random();
    int numDocs = TestUtil.nextInt(rng, 100, 5000);
    FixedBitSet trueMatches = randomBitSet(rng, numDocs, rng.nextDouble());
    FixedBitSet candidates = randomBitSet(rng, numDocs, rng.nextDouble());
    FixedBitSet originalCandidates = candidates.clone();

    TwoPhaseIterator twoPhase =
        new RandomTwoPhaseView(
            rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
    twoPhase.applyMask(numDocs, candidates, 0);

    FixedBitSet extra = candidates.clone();
    extra.andNot(originalCandidates);
    assertEquals(
        "applyMask must never set a bit that wasn't already a candidate", 0, extra.cardinality());
  }

  /**
   * applyMask must correctly clear candidates that the approximation doesn't even cover (i.e. a
   * candidate bit for a doc where approximation().advance() lands past it), without ever calling
   * matches() for such a doc. RandomTwoPhaseView's approximation is a superset of the true matches
   * with random extra false-positive positions, so a dense candidate set exercises this alongside
   * genuine confirmations.
   */
  public void testApplyMaskHandlesCandidatesNotReachedByApproximation() throws Exception {
    Random rng = random();
    int numDocs = TestUtil.nextInt(rng, 100, 5000);
    // Sparse true matches so most candidates fall in gaps the approximation may skip over.
    FixedBitSet trueMatches = randomBitSet(rng, numDocs, 0.01);
    FixedBitSet candidates = new FixedBitSet(numDocs);
    candidates.set(0, numDocs); // every doc is a candidate

    FixedBitSet expected = candidates.clone();
    expected.and(trueMatches);

    TwoPhaseIterator twoPhase =
        new RandomTwoPhaseView(
            rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
    twoPhase.applyMask(numDocs, candidates, 0);

    assertEquals(expected, candidates);
  }

  /** applyMask must leave the approximation positioned at or beyond upTo, mirroring intoBitSet. */
  public void testApplyMaskAdvancesApproximationToUpTo() throws Exception {
    Random rng = random();
    int numDocs = TestUtil.nextInt(rng, 100, 5000);
    FixedBitSet trueMatches = randomBitSet(rng, numDocs, rng.nextDouble());
    FixedBitSet candidates = randomBitSet(rng, numDocs, rng.nextDouble());

    TwoPhaseIterator twoPhase =
        new RandomTwoPhaseView(
            rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
    twoPhase.applyMask(numDocs, candidates, 0);

    assertTrue(
        "approximation must be positioned at or beyond upTo after applyMask, but was "
            + twoPhase.approximation().docID(),
        twoPhase.approximation().docID() >= numDocs);
  }

  /**
   * The bitSet passed to applyMask may be larger than the requested window ({@code upTo - offset})
   * -- DenseConjunctionBulkScorer's windowMatches is a fixed WINDOW_SIZE bitset reused across
   * windows that are often smaller than that. applyMask must not confirm, clear, or otherwise touch
   * bits at or beyond upTo, even though they're addressable in bitSet.
   */
  public void testApplyMaskDoesNotTouchBitsAtOrBeyondUpTo() throws Exception {
    Random rng = random();
    int bitSetLength = TestUtil.nextInt(rng, 200, 5000);
    int upTo = TestUtil.nextInt(rng, 10, bitSetLength - 10); // strictly smaller than bitSetLength

    FixedBitSet trueMatches = randomBitSet(rng, bitSetLength, rng.nextDouble());
    FixedBitSet candidates = randomBitSet(rng, bitSetLength, rng.nextDouble());

    // Guarantee at least one candidate at or beyond upTo that is a false positive (a candidate
    // that is NOT a true match): a buggy implementation that processes bits beyond upTo would
    // observably clear it, since matches() correctly returns false for it.
    int falsePositiveDoc = TestUtil.nextInt(rng, upTo, bitSetLength - 1);
    trueMatches.clear(falsePositiveDoc);
    candidates.set(falsePositiveDoc);

    FixedBitSet original = candidates.clone();

    FixedBitSet expectedBelowUpTo = candidates.clone();
    expectedBelowUpTo.and(trueMatches);

    TwoPhaseIterator twoPhase =
        new RandomTwoPhaseView(
            rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
    twoPhase.applyMask(upTo, candidates, 0);

    for (int d = 0; d < upTo; d++) {
      assertEquals(
          "bit " + d + " is below upTo and must match linear scan",
          expectedBelowUpTo.get(d),
          candidates.get(d));
    }
    for (int d = upTo; d < bitSetLength; d++) {
      assertEquals(
          "bit " + d + " is at or beyond upTo and must not be touched by applyMask",
          original.get(d),
          candidates.get(d));
    }
  }

  /**
   * applyMask must be correct when the mask represents a window offset from doc 0, mirroring how
   * DenseConjunctionBulkScorer calls it once per window.
   */
  public void testApplyMaskWithNonZeroOffset() throws Exception {
    Random rng = random();
    int numDocs = TestUtil.nextInt(rng, 1000, 5000);
    int windowBase = TestUtil.nextInt(rng, 1, numDocs - 500);
    int windowSize = TestUtil.nextInt(rng, 1, Math.min(500, numDocs - windowBase));
    int windowMax = windowBase + windowSize;

    FixedBitSet trueMatches = randomBitSet(rng, numDocs, rng.nextDouble());
    FixedBitSet windowCandidates = randomBitSet(rng, windowSize, rng.nextDouble());

    FixedBitSet expected = new FixedBitSet(windowSize);
    for (int i = 0; i < windowSize; i++) {
      if (windowCandidates.get(i) && trueMatches.get(windowBase + i)) {
        expected.set(i);
      }
    }

    TwoPhaseIterator twoPhase =
        new RandomTwoPhaseView(
            rng, new BitSetIterator(trueMatches, trueMatches.approximateCardinality()));
    twoPhase.approximation().advance(windowBase);

    FixedBitSet actual = windowCandidates.clone();
    twoPhase.applyMask(windowMax, actual, windowBase);

    assertEquals(expected, actual);
  }
}
