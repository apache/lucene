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

package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;

/**
 * Access to per-document neighbor lists in a (hierarchical) knn search graph.
 *
 * @lucene.experimental
 */
public abstract class KnnGraphValues {

  /** Sole constructor */
  protected KnnGraphValues() {}

  /**
   * Move the pointer to exactly the given {@code level}'s {@code target}. After this method
   * returns, call {@link #nextNeighbor()} to return successive (ordered) connected node ordinals.
   *
   * @param level level of the graph
   * @param target ordinal of a node in the graph, must be &ge; 0 and &lt; {@link
   *     VectorValues#size()}.
   */
  public abstract void seek(int level, int target) throws IOException;

  /** Returns the number of nodes in the graph */
  public abstract int size();

  /**
   * Iterates over the neighbor list. It is illegal to call this method after it returns
   * NO_MORE_DOCS without calling {@link #seek(int, int)}, which resets the iterator.
   *
   * @return a node ordinal in the graph, or NO_MORE_DOCS if the iteration is complete.
   */
  public abstract int nextNeighbor() throws IOException;

  /** Returns top level of the graph * */
  public abstract int maxLevel() throws IOException;

  /** Returns graph's entry point on the top level * */
  public abstract int entryNode() throws IOException;

  /** Empty graph value */
  public static KnnGraphValues EMPTY =
      new KnnGraphValues() {

        @Override
        public int nextNeighbor() {
          return NO_MORE_DOCS;
        }

        @Override
        public void seek(int level, int target) {}

        @Override
        public int size() {
          return 0;
        }

        @Override
        public int maxLevel() {
          return 0;
        }

        @Override
        public int entryNode() {
          return 0;
        }
      };
}
