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
package org.apache.lucene.sandbox.facet.iterators;

import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.util.PriorityQueue;

/**
 * Class that consumes incoming ordinals, sorts them by provided Comparable, and returns first top N
 * ordinals only.
 *
 * @lucene.experimental
 */
public final class TopnOrdinalIterator<T extends Comparable<T>> implements OrdinalIterator {

  private final ComparableSupplier<T> comparableSupplier;
  private final OrdinalIterator sourceOrds;
  private final int topN;
  private int[] result;
  private int currentIndex;

  /** Constructor. */
  public TopnOrdinalIterator(
      OrdinalIterator sourceOrds, ComparableSupplier<T> comparableSupplier, int topN) {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    this.sourceOrds = sourceOrds;
    this.comparableSupplier = comparableSupplier;
    this.topN = topN;
  }

  private void getTopN() throws IOException {
    assert result == null;
    // TODO: current taxonomy implementations limit queue size by taxo reader size too, but it
    //  probably doesn't make sense for large enough taxonomy indexes?
    //  e.g. TopOrdAndIntQueue q = new TopComparableQueue(Math.min(taxoReader.getSize(), topN));
    // TODO: create queue lazily - skip if first nextOrd is NO_MORE_ORDS ?
    PriorityQueue<OrdComparablePair<T>> queue =
        PriorityQueue.usingComparator(topN, Comparator.comparing(p -> p.comparable));
    OrdComparablePair<T> reuse = null;
    for (int ord = sourceOrds.nextOrd(); ord != NO_MORE_ORDS; ord = sourceOrds.nextOrd()) {
      if (reuse == null) {
        reuse = new OrdComparablePair<>(ord, comparableSupplier.createComparable(ord));
      } else {
        reuse.ordinal = ord;
        comparableSupplier.reuseComparable(ord, reuse.comparable);
      }
      reuse = queue.insertWithOverflow(reuse);
    }
    // Now we need to read from the queue as well as the queue gives the least element, not the top.
    result = new int[queue.size()];
    for (int i = result.length - 1; i >= 0; i--) {
      result[i] = queue.pop().ordinal;
    }
    currentIndex = 0;
  }

  @Override
  public int nextOrd() throws IOException {
    if (result == null) {
      getTopN();
    }
    assert result != null;
    if (currentIndex >= result.length) {
      return NO_MORE_ORDS;
    }
    return result[currentIndex++];
  }

  /** Pair of ordinal and comparable to use in TopComparableQueue */
  private static class OrdComparablePair<T extends Comparable<T>> {
    int ordinal;
    T comparable;

    private OrdComparablePair(int ordinal, T comparable) {
      this.ordinal = ordinal;
      this.comparable = comparable;
    }
  }
}
