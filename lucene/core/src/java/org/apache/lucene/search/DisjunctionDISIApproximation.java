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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of the provided
 * iterators.
 *
 * @lucene.internal
 */
public final class DisjunctionDISIApproximation extends DocIdSetIterator {

  public static DisjunctionDISIApproximation of(
      Collection<DisiWrapper> subIterators, long leadCost) {

    return new DisjunctionDISIApproximation(subIterators, leadCost);
  }

  // Heap of iterators that lead iteration.
  private final DisiPriorityQueue leadIterators;
  // List of iterators that will likely advance on every call to nextDoc() / advance()
  private final DisiWrapper[] otherIterators;
  private final long cost;
  private DisiWrapper leadTop;
  private int minOtherDoc;

  public DisjunctionDISIApproximation(Collection<DisiWrapper> subIterators, long leadCost) {
    // Using a heap to store disjunctive clauses is great for exhaustive evaluation, when a single
    // clause needs to move through the heap on every iteration on average. However, when
    // intersecting with a selective filter, it is possible that all clauses need advancing, which
    // makes the reordering cost scale in O(N * log(N)) per advance() call when checking clauses
    // linearly would scale in O(N).
    // To protect against this reordering overhead, we try to have 1.5 clauses or less that advance
    // on every advance() call by only putting clauses into the heap as long as Σ min(1, cost /
    // leadCost) <= 1.5, or Σ min(leadCost, cost) <= 1.5 * leadCost. Other clauses are checked
    // linearly.

    List<DisiWrapper> wrappers = new ArrayList<>(subIterators);
    // Sort by descending cost.
    wrappers.sort(Comparator.<DisiWrapper>comparingLong(w -> w.cost).reversed());

    leadIterators = new DisiPriorityQueue(subIterators.size());

    long reorderThreshold = leadCost + (leadCost >> 1);
    if (reorderThreshold < 0) { // overflow
      reorderThreshold = Long.MAX_VALUE;
    }
    long reorderCost = 0;
    while (wrappers.isEmpty() == false) {
      DisiWrapper last = wrappers.getLast();
      long inc = Math.min(last.cost, leadCost);
      if (reorderCost + inc < 0 || reorderCost + inc > reorderThreshold) {
        break;
      }
      leadIterators.add(wrappers.removeLast());
      reorderCost += inc;
    }

    // Make leadIterators not empty. This helps save conditionals in the implementation which are
    // rarely tested.
    if (leadIterators.size() == 0) {
      leadIterators.add(wrappers.removeLast());
    }

    otherIterators = wrappers.toArray(DisiWrapper[]::new);

    long cost = 0;
    for (DisiWrapper w : leadIterators) {
      cost += w.cost;
    }
    for (DisiWrapper w : otherIterators) {
      cost += w.cost;
    }
    this.cost = cost;
    minOtherDoc = Integer.MAX_VALUE;
    for (DisiWrapper w : otherIterators) {
      minOtherDoc = Math.min(minOtherDoc, w.doc);
    }
    leadTop = leadIterators.top();
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public int docID() {
    return Math.min(minOtherDoc, leadTop.doc);
  }

  @Override
  public int nextDoc() throws IOException {
    if (leadTop.doc < minOtherDoc) {
      int curDoc = leadTop.doc;
      do {
        leadTop.doc = leadTop.approximation.nextDoc();
        leadTop = leadIterators.updateTop();
      } while (leadTop.doc == curDoc);
      return Math.min(leadTop.doc, minOtherDoc);
    } else {
      return advance(minOtherDoc + 1);
    }
  }

  @Override
  public int advance(int target) throws IOException {
    while (leadTop.doc < target) {
      leadTop.doc = leadTop.approximation.advance(target);
      leadTop = leadIterators.updateTop();
    }

    minOtherDoc = Integer.MAX_VALUE;
    for (DisiWrapper w : otherIterators) {
      if (w.doc < target) {
        w.doc = w.approximation.advance(target);
      }
      minOtherDoc = Math.min(minOtherDoc, w.doc);
    }

    return Math.min(leadTop.doc, minOtherDoc);
  }

  @Override
  public void intoBitSet(Bits acceptDocs, int upTo, FixedBitSet bitSet, int offset)
      throws IOException {
    while (leadTop.doc < upTo) {
      leadTop.approximation.intoBitSet(acceptDocs, upTo, bitSet, offset);
      leadTop.doc = leadTop.approximation.docID();
      leadTop = leadIterators.updateTop();
    }

    minOtherDoc = Integer.MAX_VALUE;
    for (DisiWrapper w : otherIterators) {
      w.approximation.intoBitSet(acceptDocs, upTo, bitSet, offset);
      w.doc = w.approximation.docID();
      minOtherDoc = Math.min(minOtherDoc, w.doc);
    }
  }

  /** Return the linked list of iterators positioned on the current doc. */
  public DisiWrapper topList() {
    if (leadTop.doc < minOtherDoc) {
      return leadIterators.topList();
    } else {
      return computeTopList();
    }
  }

  private DisiWrapper computeTopList() {
    assert leadTop.doc >= minOtherDoc;
    DisiWrapper topList = null;
    if (leadTop.doc == minOtherDoc) {
      topList = leadIterators.topList();
    }
    for (DisiWrapper w : otherIterators) {
      if (w.doc == minOtherDoc) {
        w.next = topList;
        topList = w;
      }
    }
    return topList;
  }
}
