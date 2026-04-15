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
import org.apache.lucene.index.DocValuesSkipper;

/**
 * Wrapper around a {@link TwoPhaseIterator} for a doc-values range query that speeds things up by
 * taking advantage of a {@link DocValuesSkipper}.
 *
 * @lucene.experimental
 */
public final class DocValuesRangeIterator extends TwoPhaseIterator {

  enum Match {
    /** None of the documents in the range match */
    NO,
    /** Document values need to be checked to verify matches */
    MAYBE,
    /** All documents in the range that have a value match */
    IF_DOC_HAS_VALUE,
    /** All docs in the range match */
    YES;
  }

  private final Approximation approximation;
  private final TwoPhaseIterator innerTwoPhase;

  public DocValuesRangeIterator(
      TwoPhaseIterator twoPhase,
      DocValuesSkipper skipper,
      long lowerValue,
      long upperValue,
      boolean queryRangeHasGaps) {
    super(
        queryRangeHasGaps
            ? new RangeWithGapsApproximation(
                twoPhase.approximation(), skipper, lowerValue, upperValue)
            : new RangeNoGapsApproximation(
                twoPhase.approximation(), skipper, lowerValue, upperValue));
    this.approximation = (Approximation) approximation();
    this.innerTwoPhase = twoPhase;
  }

  abstract static class Approximation extends AbstractDocIdSetIterator {

    private final DocIdSetIterator innerApproximation;

    protected final DocValuesSkipper skipper;
    protected final long lowerValue;
    protected final long upperValue;

    // Track a decision for all doc IDs between the current doc ID and upTo inclusive.
    Match match = Match.MAYBE;
    int upTo;

    Approximation(
        DocIdSetIterator innerApproximation,
        DocValuesSkipper skipper,
        long lowerValue,
        long upperValue) {
      this.innerApproximation = innerApproximation;
      this.skipper = skipper;
      this.lowerValue = lowerValue;
      this.upperValue = upperValue;
      this.upTo = skipper.maxDocID(0);
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID() + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      while (true) {
        if (target > upTo) {
          skipper.advance(target);
          // If target doesn't have a value and is between two blocks, it is possible that advance()
          // moved to a block that doesn't contain `target`.
          target = Math.max(target, skipper.minDocID(0));
          if (target == NO_MORE_DOCS) {
            return doc = NO_MORE_DOCS;
          }
          upTo = skipper.maxDocID(0);
          match = match(0);

          // If we have a YES or NO decision, see if we still have the same decision on a higher
          // level (= on a wider range of doc IDs)
          int nextLevel = 1;
          while (match != Match.MAYBE
              && nextLevel < skipper.numLevels()
              && match == match(nextLevel)) {
            upTo = skipper.maxDocID(nextLevel);
            nextLevel++;
          }
        }
        switch (match) {
          case YES:
            return doc = target;
          case MAYBE:
          case IF_DOC_HAS_VALUE:
            if (target > innerApproximation.docID()) {
              target = innerApproximation.advance(target);
            }
            if (target <= upTo) {
              return doc = target;
            }
            // Otherwise we are breaking the invariant that `doc` must always be <= upTo, so let
            // the loop run one more iteration to advance the skipper.
            break;
          case NO:
            if (upTo == DocIdSetIterator.NO_MORE_DOCS) {
              return doc = NO_MORE_DOCS;
            }
            target = upTo + 1;
            break;
          default:
            throw new AssertionError("Unknown enum constant: " + match);
        }
      }
    }

    @Override
    public long cost() {
      return innerApproximation.cost();
    }

    protected abstract Match match(int level);
  }

  private static final class RangeNoGapsApproximation extends Approximation {

    RangeNoGapsApproximation(
        DocIdSetIterator innerApproximation,
        DocValuesSkipper skipper,
        long lowerValue,
        long upperValue) {
      super(innerApproximation, skipper, lowerValue, upperValue);
    }

    @Override
    protected Match match(int level) {
      long minValue = skipper.minValue(level);
      long maxValue = skipper.maxValue(level);
      if (minValue > upperValue || maxValue < lowerValue) {
        return Match.NO;
      } else if (minValue >= lowerValue && maxValue <= upperValue) {
        if (skipper.docCount(level) == skipper.maxDocID(level) - skipper.minDocID(level) + 1) {
          return Match.YES;
        } else {
          return Match.IF_DOC_HAS_VALUE;
        }
      } else {
        return Match.MAYBE;
      }
    }
  }

  private static final class RangeWithGapsApproximation extends Approximation {

    RangeWithGapsApproximation(
        DocIdSetIterator innerApproximation,
        DocValuesSkipper skipper,
        long lowerValue,
        long upperValue) {
      super(innerApproximation, skipper, lowerValue, upperValue);
    }

    @Override
    protected Match match(int level) {
      long minValue = skipper.minValue(level);
      long maxValue = skipper.maxValue(level);
      if (minValue > upperValue || maxValue < lowerValue) {
        return Match.NO;
      } else {
        return Match.MAYBE;
      }
    }
  }

  @Override
  public final boolean matches() throws IOException {
    return switch (approximation.match) {
      case YES, IF_DOC_HAS_VALUE -> true;
      case MAYBE -> innerTwoPhase.matches();
      case NO -> throw new IllegalStateException("Unpositioned approximation");
    };
  }

  @Override
  public int docIDRunEnd() throws IOException {
    if (approximation.match == Match.YES) {
      return approximation.upTo + 1;
    }
    return super.docIDRunEnd();
  }

  @Override
  public float matchCost() {
    return innerTwoPhase.matchCost();
  }
}
