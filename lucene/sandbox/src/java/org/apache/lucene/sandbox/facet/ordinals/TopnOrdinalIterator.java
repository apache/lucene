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
package org.apache.lucene.sandbox.facet.ordinals;

import java.io.IOException;
import org.apache.lucene.util.PriorityQueue;

/**
 * Class that consumes incoming ordinals, sorts them by provided Comparable, and returns first top N
 * ordinals only.
 */
public class TopnOrdinalIterator<T extends Comparable<T> & OrdinalGetter>
    implements OrdinalIterator {

  private final OrdToComparable<T> ordToComparable;
  private final OrdinalIterator sourceOrds;
  private final int topN;
  private int[] result;
  private int currentIndex;

  /** Constructor. */
  public TopnOrdinalIterator(
      OrdinalIterator sourceOrds, OrdToComparable<T> ordToComparable, int topN) {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    this.sourceOrds = sourceOrds;
    this.ordToComparable = ordToComparable;
    this.topN = topN;
  }

  private void getTopN() throws IOException {
    assert result == null;
    // TODO: current taxonomy implementations limit queue size by taxo reader size too, but it
    //  probably doesn't make sense for large enough taxonomy indexes?
    //  e.g. TopOrdAndIntQueue q = new TopComparableQueue(Math.min(taxoReader.getSize(), topN));
    // TODO: create queue lazily - skip if first nextOrd is NO_MORE_ORDS ?
    TopComparableQueue<T> queue = new TopComparableQueue<>(topN);
    T reuse = null;
    for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS; ) {
      reuse = ordToComparable.getComparable(nextOrdinal, reuse);
      reuse = queue.insertWithOverflow(reuse);
      nextOrdinal = sourceOrds.nextOrd();
    }
    // Now we need to read from the queue as well as the queue gives the least element, not the top.
    result = new int[queue.size()];
    for (int i = result.length - 1; i >= 0; i--) {
      result[i] = queue.pop().getOrd();
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

  /** Keeps top N results ordered by Comparable. */
  private static class TopComparableQueue<T extends Comparable<T>> extends PriorityQueue<T> {

    /** Sole constructor. */
    public TopComparableQueue(int topN) {
      super(topN);
    }

    @Override
    protected boolean lessThan(T a, T b) {
      return a.compareTo(b) < 0;
    }
  }
}
