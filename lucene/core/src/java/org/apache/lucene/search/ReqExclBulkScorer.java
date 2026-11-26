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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

final class ReqExclBulkScorer extends BulkScorer {

  private static final int WINDOW_SIZE = 4096;
  private final BulkScorer req;
  private final DocIdSetIterator exclApproximation;
  private final TwoPhaseIterator exclTwoPhase;
  private final WindowBits windowBits;

  ReqExclBulkScorer(BulkScorer req, Scorer excl, int maxDoc) {
    this(req, excl.twoPhaseIterator(), excl, maxDoc);
  }

  ReqExclBulkScorer(BulkScorer req, DocIdSetIterator excl, int maxDoc) {
    this(req, null, excl, maxDoc);
  }

  ReqExclBulkScorer(BulkScorer req, TwoPhaseIterator excl, int maxDoc) {
    this(req, excl, excl.approximation(), maxDoc);
  }

  private ReqExclBulkScorer(
      BulkScorer req, TwoPhaseIterator twoPhaseExcl, Scorer fallbackExcl, int maxDoc) {
    this(
        req,
        twoPhaseExcl,
        twoPhaseExcl != null ? twoPhaseExcl.approximation() : fallbackExcl.iterator(),
        maxDoc);
  }

  private ReqExclBulkScorer(
      BulkScorer req, TwoPhaseIterator excl, DocIdSetIterator approximation, int maxDoc) {
    this.req = req;
    this.exclTwoPhase = excl;
    this.exclApproximation = approximation;
    this.windowBits = shouldUseWindow(req, excl, maxDoc) ? new WindowBits() : null;
  }

  private static boolean shouldUseWindow(BulkScorer req, TwoPhaseIterator excl, int maxDoc) {
    if (excl == null) {
      return true;
    }
    if (excl.isBulkOptimized() == true) {
      return true;
    }
    return shouldUseWindow(req.cost(), excl.approximation().cost(), excl.matchCost(), maxDoc);
  }

  static boolean shouldUseWindow(long reqCost, long exclApproxCost, float matchCost, int maxDoc) {
    double reqFraction = (double) Math.min(maxDoc, reqCost) / maxDoc;
    double exclFraction = (double) Math.min(maxDoc, exclApproxCost) / maxDoc;

    double windowCost = exclFraction * (ReqExclScorer.ADVANCE_COST + matchCost * (1 - reqFraction));
    double switches = 2 * exclFraction * reqFraction / (exclFraction + reqFraction);
    double leapFrogCost = switches * 2 * ReqExclScorer.ADVANCE_COST;
    return (windowCost / 128) < leapFrogCost;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int upTo =
        windowBits == null
            ? scoreLeapFrog(collector, acceptDocs, min, max)
            : scoreWindowed(collector, acceptDocs, min, max);
    if (upTo == max) {
      upTo = req.score(collector, acceptDocs, max, max);
    }
    return upTo;
  }

  private int scoreLeapFrog(LeafCollector collector, Bits acceptDocs, int min, int max)
      throws IOException {
    int upTo = min;
    int exclDoc = exclApproximation.docID();

    while (upTo < max) {
      if (exclDoc < upTo) {
        exclDoc = exclApproximation.advance(upTo);
      }
      if (exclDoc == upTo) {
        if (exclTwoPhase.matches()) {
          // upTo is excluded (matches() just confirmed it), so skip the whole run of consecutive
          // excluded docs at once. The default TwoPhaseIterator#docIDRunEnd() conservatively
          // returns the current doc, so clamp to at least upTo+1 to guarantee progress when the
          // run end is not overridden.
          upTo = Math.max(upTo + 1, Math.min(exclTwoPhase.docIDRunEnd(), max));
        }
        exclDoc = exclApproximation.nextDoc();
      } else {
        upTo = req.score(collector, acceptDocs, upTo, Math.min(exclDoc, max));
      }
    }
    return upTo;
  }

  private int scoreWindowed(LeafCollector collector, Bits acceptDocs, int min, int max)
      throws IOException {
    FixedBitSet windowMask = windowBits.windowMask;
    int upTo = min;

    while (upTo < max) {
      int windowMin = upTo;
      int windowMax = (int) Math.min(max, (long) windowMin + WINDOW_SIZE);

      if (exclApproximation.docID() < windowMin) {
        exclApproximation.advance(windowMin);
      }

      windowMask.clear();
      if (exclTwoPhase == null) {
        exclApproximation.intoBitSet(windowMax, windowMask, windowMin);
      } else {
        exclTwoPhase.intoBitSet(windowMax, windowMask, windowMin);
      }
      int windowLength = windowMax - windowMin;
      int validWindowLength =
          acceptDocs == null
              ? windowLength
              : Math.max(0, Math.min(windowLength, acceptDocs.length() - windowMin));
      windowMask.flip(0, validWindowLength);
      if (validWindowLength < windowLength) {
        windowMask.clear(validWindowLength, windowLength);
      }
      if (acceptDocs != null) {
        acceptDocs.applyMask(windowMask, windowMin);
      }
      windowBits.reset(windowMin, windowMax, acceptDocs == null ? max : acceptDocs.length());
      upTo = req.score(collector, windowBits, windowMin, windowMax);
    }
    return upTo;
  }

  @Override
  public long cost() {
    return req.cost();
  }

  static final class WindowBits implements Bits {
    final FixedBitSet windowMask = new FixedBitSet(WINDOW_SIZE);
    private int windowBase;
    private int windowEnd;
    private int length;

    void reset(int windowBase, int windowEnd, int length) {
      this.windowBase = windowBase;
      this.windowEnd = windowEnd;
      this.length = length;
    }

    @Override
    public boolean get(int index) {
      assert index >= windowBase && index < windowEnd;
      return windowMask.get(index - windowBase);
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public void applyMask(FixedBitSet bitSet, int offset) {
      int bitSetEnd = Math.min(DocIdSetIterator.NO_MORE_DOCS, offset + bitSet.length());
      // The absolute doc ID ranges for bitSet and the current window.
      int from = Math.max(windowBase, offset);
      int to = Math.min(windowEnd, bitSetEnd);
      if (from >= to) {
        bitSet.clear();
        return;
      }

      int targetFrom = from - offset;
      if (targetFrom > 0) {
        bitSet.clear(0, targetFrom);
      }

      int targetTo = to - offset;
      if (targetTo < bitSet.length()) {
        bitSet.clear(targetTo, bitSet.length());
      }

      FixedBitSet.andRange(windowMask, from - windowBase, bitSet, targetFrom, to - from);
    }
  }
}
