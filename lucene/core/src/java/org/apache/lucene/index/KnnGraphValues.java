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
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

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

  /** Returns the number of levels of the graph */
  public abstract int numLevels() throws IOException;

  /** Returns graph's entry point on the top level * */
  public abstract int entryNode() throws IOException;

  /**
   * Get all nodes on a given level as node 0th ordinals
   *
   * @param level level for which to get all nodes
   * @return an iterator over nodes where {@code nextInt} returns a next node on the level
   */
  public abstract NodesIterator getNodesOnLevel(int level) throws IOException;

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
        public int numLevels() {
          return 0;
        }

        @Override
        public int entryNode() {
          return 0;
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
          return NodesIterator.EMPTY;
        }
      };

  /**
   * Iterator over the graph nodes on a certain level, Iterator also provides the size – the total
   * number of nodes to be iterated over.
   */
  public static final class NodesIterator implements PrimitiveIterator.OfInt {
    static NodesIterator EMPTY = new NodesIterator(0);

    private final int[] nodes;
    private final int size;
    int cur = 0;

    /** Constructor for iterator based on the nodes array up to the size */
    public NodesIterator(int[] nodes, int size) {
      assert nodes != null;
      assert size <= nodes.length;
      this.nodes = nodes;
      this.size = size;
    }

    /** Constructor for iterator based on the size */
    public NodesIterator(int size) {
      this.nodes = null;
      this.size = size;
    }

    @Override
    public int nextInt() {
      if (hasNext() == false) {
        throw new NoSuchElementException();
      }
      if (nodes == null) {
        return cur++;
      } else {
        return nodes[cur++];
      }
    }

    @Override
    public boolean hasNext() {
      return cur < size;
    }

    /** The number of elements in this iterator * */
    public int size() {
      return size;
    }
  }
}
