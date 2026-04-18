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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of the provided
 * iterators.
 *
 * @lucene.internal
 */
public final class DisjunctionDISIApproximation extends AbstractDocIdSetIterator {

  public static DisjunctionDISIApproximation of(
      Collection<? extends DisiWrapper> subIterators, long leadCost) {

    return new DisjunctionDISIApproximation(subIterators, leadCost);
  }

  // Heap of iterators that lead iteration.
  private final DisiPriorityQueue leadIterators;
  // List of iterators that will likely advance on every call to nextDoc() / advance()
  private final DisiWrapper[] otherIterators;
  private final long cost;
  private DisiWrapper leadTop;
  private int minOtherDoc;

  public DisjunctionDISIApproximation(
      Collection<? extends DisiWrapper> subIterators, long leadCost) {
    // Using a heap to store disjunctive clauses is great for exhaustive evaluation, when a single
    // clause needs to move through the heap on every iteration on average. However, when
    // intersecting with a selective filter, it is possible that all clauses need advancing, which
    // makes the reordering cost scale in O(N * log(N)) per advance() call when checking clauses
    // linearly would scale in O(N).
    // To protect against this reordering overhead, we try to have 1.5 clauses or less that advance
    // on every advance() call by only putting clauses into the heap as long as Σ min(1, cost /
    // leadCost) <= 1.5, or Σ min(leadCost, cost) <= 1.5 * leadCost. Other clauses are checked
    // linearly.

    DisiWrapper[] wrappers = subIterators.toArray(DisiWrapper[]::new);
    // Sort by descending cost.
    Arrays.sort(wrappers, Comparator.<DisiWrapper>comparingLong(w -> w.cost).reversed());

    long reorderThreshold = leadCost + (leadCost >> 1);
    if (reorderThreshold < 0) { // overflow
      reorderThreshold = Long.MAX_VALUE;
    }

    long cost = 0; // track total cost
    // Split `wrappers` into those that will remain out of the PQ, and those that will go in
    // (PQ entries at the end). `lastIdx` is the last index of the wrappers that will remain out.
    long reorderCost = 0;
    int lastIdx = wrappers.length - 1;
    for (; lastIdx >= 0; lastIdx--) {
      long lastCost = wrappers[lastIdx].cost;
      long inc = Math.min(lastCost, leadCost);
      if (reorderCost + inc < 0 || reorderCost + inc > reorderThreshold) {
        break;
      }
      reorderCost += inc;
      cost += lastCost;
    }

    // Make leadIterators not empty. This helps save conditionals in the implementation which are
    // rarely tested.
    if (lastIdx == wrappers.length - 1) {
      cost += wrappers[lastIdx].cost;
      lastIdx--;
    }

    // Build the PQ:
    assert lastIdx >= -1 && lastIdx < wrappers.length - 1;
    int pqLen = wrappers.length - lastIdx - 1;
    leadIterators = DisiPriorityQueue.ofMaxSize(pqLen);
    leadIterators.addAll(wrappers, lastIdx + 1, pqLen);

    // Build the non-PQ list:
    otherIterators = ArrayUtil.copyOfSubArray(wrappers, 0, lastIdx + 1);
    minOtherDoc = Integer.MAX_VALUE;
    for (DisiWrapper w : otherIterators) {
      cost += w.cost;
      minOtherDoc = Math.min(minOtherDoc, w.doc);
    }

    this.cost = cost;
    leadTop = leadIterators.top();
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public int nextDoc() throws IOException {
    if (leadTop.doc < minOtherDoc) {
      int curDoc = leadTop.doc;
      do {
        leadTop.doc = leadTop.approximation.nextDoc();
        leadTop = leadIterators.updateTop();
      } while (leadTop.doc == curDoc);
      return doc = Math.min(leadTop.doc, minOtherDoc);
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

    return doc = Math.min(leadTop.doc, minOtherDoc);
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    while (leadTop.doc < upTo) {
      leadTop.approximation.intoBitSet(upTo, bitSet, offset);
      leadTop.doc = leadTop.approximation.docID();
      leadTop = leadIterators.updateTop();
    }

    minOtherDoc = Integer.MAX_VALUE;
    for (DisiWrapper w : otherIterators) {
      w.approximation.intoBitSet(upTo, bitSet, offset);
      w.doc = w.approximation.docID();
      minOtherDoc = Math.min(minOtherDoc, w.doc);
    }

    doc = Math.min(leadTop.doc, minOtherDoc);
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

  @Override
  public int docIDRunEnd() throws IOException {
    // We're only looking at the "top" clauses. In theory, we may be able to find longer runs if
    // other clauses have overlapping runs with the runs of the top clauses, but does it actually
    // happen in practice and would it buy much?
    int maxDocIDRunEnd = super.docIDRunEnd();
    for (DisiWrapper w = topList(); w != null; w = w.next) {
      maxDocIDRunEnd = Math.max(maxDocIDRunEnd, w.approximation.docIDRunEnd());
    }
    return maxDocIDRunEnd;
  }
}
