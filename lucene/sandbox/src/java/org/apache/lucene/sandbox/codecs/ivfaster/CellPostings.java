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
package org.apache.lucene.sandbox.codecs.ivfaster;

import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;

/**
 * One cell of an ivfaster segment as a posting list: the doc ids of its slot run, ascending.
 *
 * <p>A cell is the contiguous slot range {@code [base, end)} of the code table, and the writer
 * emits those slots in ascending doc-id order, so the reader's dense {@code slotDoc} column over
 * that range IS a sorted posting list, with no decoding and no separate structure. {@link #slot()}
 * reports the slot behind the current doc, which is what the coarse scan scores.
 *
 * <p>{@link #advance} gallops from the current position and then bisects, so a filter that leads
 * the intersection and skips far ahead costs {@code O(log gap)} per call rather than a linear walk,
 * while a filter that lands on nearly every slot stays {@code O(1)} amortised.
 *
 * <p>A document occupies at most one slot per cell, so ids never repeat within one instance. They
 * do repeat ACROSS cells under spill, which the disjunction over probed cells collapses: every copy
 * carries the same coarse code, so one copy is enough to score.
 */
final class CellPostings extends DocIdSetIterator {

  private final int[] slotDoc;
  private final int base;
  private final int end;
  private int pos;
  private int doc = -1;

  CellPostings(int[] slotDoc, int base, int rows) {
    this.slotDoc = slotDoc;
    this.base = base;
    this.end = base + rows;
    this.pos = base - 1;
  }

  /** The slot of the current doc; valid while {@link #docID()} is a real doc. */
  int slot() {
    return pos;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() {
    pos++;
    if (pos >= end) {
      pos = end;
      return doc = NO_MORE_DOCS;
    }
    return doc = slotDoc[pos];
  }

  @Override
  public int advance(int target) {
    int lo = pos + 1;
    if (lo >= end) {
      pos = end;
      return doc = NO_MORE_DOCS;
    }
    if (slotDoc[lo] >= target) {
      pos = lo;
      return doc = slotDoc[lo];
    }
    // Gallop: slotDoc[lo] < target; find hi with slotDoc[hi] >= target or hi == end.
    int step = 1;
    int hi = lo + 1;
    while (hi < end && slotDoc[hi] < target) {
      lo = hi;
      step <<= 1;
      hi = lo + step;
    }
    if (hi > end) {
      hi = end;
    }
    // Bisect (lo, hi]: the first index whose doc is >= target.
    int a = lo + 1;
    int b = hi;
    while (a < b) {
      final int mid = (a + b) >>> 1;
      if (slotDoc[mid] < target) {
        a = mid + 1;
      } else {
        b = mid;
      }
    }
    pos = a;
    if (a >= end) {
      pos = end;
      return doc = NO_MORE_DOCS;
    }
    return doc = slotDoc[a];
  }

  @Override
  public long cost() {
    return end - base;
  }

  /**
   * A {@link DisiWrapper} that keeps the cell it wraps, so the disjunction's {@code topList()}
   * hands back the slot to score without unwrapping the scorer.
   */
  static final class Wrapper extends DisiWrapper {
    final CellPostings cell;

    Wrapper(CellPostings cell) {
      super(new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, cell), false);
      this.cell = cell;
    }
  }
}
