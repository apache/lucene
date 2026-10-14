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
  private final WindowBits windowBits = new WindowBits();

  ReqExclBulkScorer(BulkScorer req, Scorer excl) {
    this.req = req;
    this.exclTwoPhase = excl.twoPhaseIterator();
    if (exclTwoPhase != null) {
      this.exclApproximation = exclTwoPhase.approximation();
    } else {
      this.exclApproximation = excl.iterator();
    }
  }

  ReqExclBulkScorer(BulkScorer req, DocIdSetIterator excl) {
    this.req = req;
    this.exclTwoPhase = null;
    this.exclApproximation = excl;
  }

  ReqExclBulkScorer(BulkScorer req, TwoPhaseIterator excl) {
    this.req = req;
    this.exclTwoPhase = excl;
    this.exclApproximation = excl.approximation();
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    FixedBitSet windowMask = windowBits.windowMask;

    int upTo = min;

    while (upTo < max) {
      int windowMin = upTo;
      int windowMax = (int) Math.min((long) max, (long) windowMin + WINDOW_SIZE);

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

    if (upTo == max) {
      upTo = req.score(collector, acceptDocs, max, max);
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
