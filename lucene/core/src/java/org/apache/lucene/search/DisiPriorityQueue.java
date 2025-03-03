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

import org.apache.lucene.util.PriorityQueue;

/**
 * A priority queue of DocIdSetIterators that orders by current doc ID. This specialization is
 * needed over {@link PriorityQueue} because the pluggable comparison function makes the rebalancing
 * quite slow.
 *
 * @lucene.internal
 */
public abstract sealed class DisiPriorityQueue implements Iterable<DisiWrapper>
    permits DisiPriorityQueue2, DisiPriorityQueueN {

  /** Create a {@link DisiPriorityQueue} of the given maximum size. */
  public static DisiPriorityQueue ofMaxSize(int maxSize) {
    if (maxSize <= 2) {
      return new DisiPriorityQueue2();
    } else {
      return new DisiPriorityQueueN(maxSize);
    }
  }

  /** Return the number of entries in this heap. */
  public abstract int size();

  /** Return top value in this heap, or null if the heap is empty. */
  public abstract DisiWrapper top();

  /** Return the 2nd least value in this heap, or null if the heap contains less than 2 values. */
  public abstract DisiWrapper top2();

  /** Get the list of scorers which are on the current doc. */
  public abstract DisiWrapper topList();

  /** Add a {@link DisiWrapper} to this queue and return the top entry. */
  public abstract DisiWrapper add(DisiWrapper entry);

  /** Bulk add. */
  public void addAll(DisiWrapper[] entries, int offset, int len) {
    for (int i = 0; i < len; ++i) {
      add(entries[offset + i]);
    }
  }

  /** Remove the top entry and return it. */
  public abstract DisiWrapper pop();

  /** Rebalance this heap and return the top entry. */
  public abstract DisiWrapper updateTop();

  /**
   * Replace the top entry with the given entry, rebalance the heap, and return the new top entry.
   */
  abstract DisiWrapper updateTop(DisiWrapper topReplacement);

  /** Clear the heap. */
  public abstract void clear();
}
