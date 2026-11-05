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

/**
 * A {@link LeafFieldComparator} for segments whose index sort starts with the search sort, so docs
 * are visited in sort order. Once the hits threshold is reached and a doc is not competitive
 * against the bottom of the queue, no later doc can be either, and collection of the segment is
 * terminated.
 */
final class IndexSortedLeafFieldComparator implements LeafFieldComparator {

  private final LeafFieldComparator delegate;
  private final int reverseMul;
  // When paging, docs before the top value never fill the queue, so only the delegate can skip
  // them. Otherwise collection terminates anyway and the delegate's skipping, e.g. building
  // iterators from points, is redundant work: it measurably slows down index-sorted searches.
  private final boolean paging;
  private boolean hitsThresholdReached;
  // Set once a doc is not competitive. Not needed for correctness, since the delegate would say the
  // same for every later doc, but until the hits threshold is reached those docs are still visited,
  // and this saves a doc values read and comparison for each of them.
  private boolean exhausted;

  /**
   * @param delegate the comparator for this segment
   * @param reverseMul the multiplier that the collector applies to {@code delegate}'s comparisons
   * @param paging whether a top value is set, in which case {@link #setHitsThresholdReached()} and
   *     the delegate's competitive iterator are passed through
   */
  IndexSortedLeafFieldComparator(LeafFieldComparator delegate, int reverseMul, boolean paging) {
    this.delegate = delegate;
    this.reverseMul = reverseMul;
    this.paging = paging;
  }

  @Override
  public void setBottom(int slot) throws IOException {
    delegate.setBottom(slot);
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    int cmp = exhausted ? 0 : delegate.compareBottom(doc);
    if (reverseMul * cmp <= 0) {
      // docs are visited in sort order, so nothing after this can be competitive
      exhausted = true;
      if (hitsThresholdReached) {
        throw new CollectionTerminatedException();
      }
    }
    return cmp;
  }

  @Override
  public int compareTop(int doc) throws IOException {
    return delegate.compareTop(doc);
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    delegate.copy(slot, doc);
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    delegate.setScorer(scorer);
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    return paging ? delegate.competitiveIterator() : null;
  }

  @Override
  public void setHitsThresholdReached() throws IOException {
    hitsThresholdReached = true;
    if (paging) {
      delegate.setHitsThresholdReached();
    }
  }
}
