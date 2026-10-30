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
package org.apache.lucene.util.hnsw;

import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * The nodes that a graph search has visited. When a search is expected to visit a small part of the
 * graph, this is a hash set, which replaces SparseFixedBitSet: clearing it costs the number of
 * visited nodes and not the size of the graph. Otherwise it is a FixedBitSet.
 *
 * @lucene.internal
 */
public interface VisitedSet {

  /**
   * Returns an empty set for a search of a graph.
   *
   * @param expectedVisitedNodes the number of nodes that a search is expected to visit
   * @param graphSize the number of nodes of the graph
   */
  static VisitedSet create(int expectedVisitedNodes, int graphSize) {
    int expected = Math.max(1, expectedVisitedNodes); // negative for an empty graph
    if (expected < (graphSize >>> 7)) {
      IntHashSet nodes = new IntHashSet(expected);
      return new VisitedSet() {
        @Override
        public void set(int node) {
          nodes.add(node);
        }

        @Override
        public boolean getAndSet(int node) {
          return nodes.add(node) == false;
        }

        @Override
        public void clear() {
          nodes.clear();
        }

        @Override
        public int length() {
          return Integer.MAX_VALUE;
        }

        @Override
        public void ensureCapacityAndClear(int capacity) {
          nodes.clear();
        }
      };
    } else {
      return new VisitedSet() {
        private FixedBitSet bits = new FixedBitSet(graphSize);

        @Override
        public void set(int node) {
          bits.set(node);
        }

        @Override
        public boolean getAndSet(int node) {
          return bits.getAndSet(node);
        }

        @Override
        public void clear() {
          bits.clear();
        }

        @Override
        public int length() {
          return bits.length();
        }

        @Override
        public void ensureCapacityAndClear(int capacity) {
          bits = FixedBitSet.ensureCapacityAndClear(bits, capacity);
        }
      };
    }
  }

  /** Marks the node as visited. */
  void set(int node);

  /** Marks the node as visited, and returns true if it was visited before. */
  boolean getAndSet(int node);

  /** Removes all nodes. */
  void clear();

  /** Returns the number of nodes that the set can hold. */
  int length();

  /** Removes all nodes, and grows the set to hold at least {@code capacity} nodes. */
  void ensureCapacityAndClear(int capacity);
}
