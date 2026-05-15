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
package org.apache.lucene.document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongPredicate;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;

/**
 * Constant-score supplier that uses a {@link DocValuesSkipper} to approximate the document interval
 * matching a numeric or ordinal range when the leaf's primary {@link SortField} aligns with that
 * field.
 *
 * <p>The skipper yields coarse min/max doc IDs that contain all matching documents. When the
 * skipper interval at a boundary is not exact for the query range, {@link #nextDoc} scans forward
 * from that doc to the first document whose value satisfies the appropriate comparison, producing a
 * tight {@link DocIdSetIterator#range} iterator.
 */
abstract class SortedSkipperScorerSupplier extends ConstantScoreScorerSupplier {

  private final DocValuesSkipper skipper;
  private final SortField sortField;
  private int skipperMinDocId = -1, skipperMaxDocId = -1;
  private boolean skipperMinDocIdExact = false, skipperMaxDocIdExact = false;

  /**
   * @param skipper skipper for the doc values range field (same field as {@code sortField})
   * @param sortField leaf primary sort; {@link SortField#getReverse()} controls how range endpoints
   *     map to ordinal or value predicates
   * @param score constant score assigned to hits
   * @param scoreMode scoring mode passed to {@link ConstantScoreScorerSupplier}
   * @param maxDoc exclusive upper bound of doc IDs on this leaf (typically {@link
   *     org.apache.lucene.index.LeafReader#maxDoc()})
   */
  SortedSkipperScorerSupplier(
      DocValuesSkipper skipper, SortField sortField, float score, ScoreMode scoreMode, int maxDoc) {
    super(score, scoreMode, maxDoc);
    this.skipper = skipper;
    this.sortField = sortField;
  }

  /** Lower bound of the query range in the same coordinate space as the skipper values. */
  protected abstract long getLowerValue() throws IOException;

  /** Upper bound of the query range in the same coordinate space as the skipper values. */
  protected abstract long getUpperValue() throws IOException;

  /**
   * Advance doc values from {@code startDocID} and return the first document whose value makes
   * {@code predicate} true, or {@link DocIdSetIterator#NO_MORE_DOCS} if there is none. Used to
   * refine skipper-derived boundaries when the interval min/max is not exact for the range.
   *
   * @param startDocID first doc ID to examine (inclusive)
   * @param predicate tests the per-document value (ordinal or numeric) at the current doc
   */
  protected abstract int nextDoc(int startDocID, LongPredicate predicate) throws IOException;

  @Override
  public DocIdSetIterator iterator(long leadCost) throws IOException {
    if (skipperMinDocId == -1) {
      computeSkipperDocIds();
    }
    long minOrd = getLowerValue();
    long maxOrd = getUpperValue();
    final int minDocID;
    final int maxDocID;
    if (sortField.getReverse()) {
      minDocID =
          skipperMinDocIdExact ? skipperMinDocId : nextDoc(skipperMinDocId, l -> l <= maxOrd);
      maxDocID = skipperMaxDocIdExact ? skipperMaxDocId : nextDoc(skipperMaxDocId, l -> l < minOrd);
    } else {
      minDocID =
          skipperMinDocIdExact ? skipperMinDocId : nextDoc(skipperMinDocId, l -> l >= minOrd);
      maxDocID = skipperMaxDocIdExact ? skipperMaxDocId : nextDoc(skipperMaxDocId, l -> l > maxOrd);
    }
    return minDocID == maxDocID
        ? DocIdSetIterator.empty()
        : DocIdSetIterator.range(minDocID, maxDocID);
  }

  @Override
  public long cost() {
    if (skipperMinDocId == -1) {
      try {
        // Similar to PointValues, IOExceptions needs to be caught and rethrown as
        // UncheckedIOException
        computeSkipperDocIds();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    if (skipperMinDocIdExact && skipperMaxDocIdExact) {
      return skipperMaxDocId - skipperMinDocId;
    }
    return skipperMaxDocId - skipperMinDocId + skipper.docCount(0);
  }

  private void computeSkipperDocIds() throws IOException {
    long minOrd = getLowerValue();
    long maxOrd = getUpperValue();
    if (minOrd > maxOrd || minOrd > skipper.maxValue() || maxOrd < skipper.minValue()) {
      skipperMinDocId = skipperMaxDocId = DocIdSetIterator.NO_MORE_DOCS;
      skipperMinDocIdExact = skipperMaxDocIdExact = true;
      return;
    }
    if (skipper.minValue() >= minOrd && skipper.maxValue() <= maxOrd) {
      skipperMinDocId = 0;
      skipperMaxDocId = skipper.docCount();
      skipperMinDocIdExact = skipperMaxDocIdExact = true;
      return;
    }
    if (sortField.getReverse()) {
      if (skipper.maxValue() <= maxOrd) {
        skipperMinDocId = 0;
        skipperMinDocIdExact = true;
      } else {
        skipper.advance(Long.MIN_VALUE, maxOrd);
        skipperMinDocId = skipper.minDocID(0);
        skipperMinDocIdExact = skipper.maxValue(0) == maxOrd;
      }
      if (skipper.minValue() >= minOrd) {
        skipperMaxDocId = skipper.docCount();
        skipperMaxDocIdExact = true;
      } else {
        skipper.advance(Long.MIN_VALUE, minOrd);
        if (skipper.minValue(0) == minOrd) {
          skipperMaxDocId = skipper.maxDocID(0) + 1;
          skipper.advance(skipperMaxDocId);
          skipperMaxDocIdExact = skipper.maxValue(0) != minOrd;
        } else {
          skipperMaxDocId = skipper.minDocID(0);
          skipperMaxDocIdExact = false;
        }
      }
    } else {
      if (skipper.minValue() >= minOrd) {
        skipperMinDocId = 0;
        skipperMinDocIdExact = true;
      } else {
        skipper.advance(minOrd, Long.MAX_VALUE);
        skipperMinDocId = skipper.minDocID(0);
        skipperMinDocIdExact = skipper.minValue(0) == minOrd;
      }
      if (skipper.maxValue() <= maxOrd) {
        skipperMaxDocId = skipper.docCount();
        skipperMaxDocIdExact = true;
      } else {
        skipper.advance(maxOrd, Long.MAX_VALUE);
        if (skipper.maxValue(0) == maxOrd) {
          skipperMaxDocId = skipper.maxDocID(0) + 1;
          skipper.advance(skipperMaxDocId);
          skipperMaxDocIdExact = skipper.minValue(0) != maxOrd;
        } else {
          skipperMaxDocId = skipper.minDocID(0);
          skipperMaxDocIdExact = false;
        }
      }
    }
  }
}
