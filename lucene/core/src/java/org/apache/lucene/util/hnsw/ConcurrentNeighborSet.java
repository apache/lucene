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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;

/** A concurrent set of neighbors. */
public class ConcurrentNeighborSet {
  /** the node id whose neighbors we are storing */
  private final int nodeId;

  /**
   * We use a copy-on-write NeighborArray to store the neighbors. Even though updating this is
   * expensive, it is still faster than using a concurrent Collection because "iterate through a
   * node's neighbors" is a hot loop in adding to the graph, and NeighborArray can do that much
   * faster: no boxing/unboxing, all the data is stored sequentially instead of having to follow
   * references, and no fancy encoding necessary for node/score.
   */
  private final AtomicReference<ConcurrentNeighborArray> neighborsRef;

  private final NeighborSimilarity similarity;

  /** the maximum number of neighbors we can store */
  private final int maxConnections;

  public ConcurrentNeighborSet(int nodeId, int maxConnections, NeighborSimilarity similarity) {
    this.nodeId = nodeId;
    this.maxConnections = maxConnections;
    this.similarity = similarity;
    neighborsRef = new AtomicReference<>(new ConcurrentNeighborArray(maxConnections, true));
  }

  public PrimitiveIterator.OfInt nodeIterator() {
    // don't use a stream here. stream's implementation of iterator buffers
    // very aggressively, which is a big waste for a lot of searches.
    return new NeighborIterator(neighborsRef.get());
  }

  public void backlink(Function<Integer, ConcurrentNeighborSet> neighborhoodOf) throws IOException {
    NeighborArray neighbors = neighborsRef.get();
    for (int i = 0; i < neighbors.size(); i++) {
      int nbr = neighbors.node[i];
      float nbrScore = neighbors.score[i];
      ConcurrentNeighborSet nbrNbr = neighborhoodOf.apply(nbr);
      nbrNbr.insert(nodeId, nbrScore);
    }
  }

  private static class NeighborIterator implements PrimitiveIterator.OfInt {
    private final NeighborArray neighbors;
    private int i;

    private NeighborIterator(NeighborArray neighbors) {
      this.neighbors = neighbors;
      i = 0;
    }

    @Override
    public boolean hasNext() {
      return i < neighbors.size();
    }

    @Override
    public int nextInt() {
      return neighbors.node[i++];
    }
  }

  public int size() {
    return neighborsRef.get().size();
  }

  public int arrayLength() {
    return neighborsRef.get().node.length;
  }

  /**
   * For each candidate (going from best to worst), select it only if it is closer to target than it
   * is to any of the already-selected candidates. This is maintained whether those other neighbors
   * were selected by this method, or were added as a "backlink" to a node inserted concurrently
   * that chose this one as a neighbor.
   */
  public void insertDiverse(NeighborArray candidates) {
    BitSet selected = new FixedBitSet(candidates.size());
    for (int i = candidates.size() - 1; i >= 0; i--) {
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      if (isDiverse(cNode, cScore, candidates, selected)) {
        selected.set(i);
      }
    }
    insertMultiple(candidates, selected);
    // This leaves the paper's keepPrunedConnection option out; we might want to add that
    // as an option in the future.
  }

  private void insertMultiple(NeighborArray others, BitSet selected) {
    neighborsRef.getAndUpdate(
        current -> {
          ConcurrentNeighborArray next = current.copy();
          for (int i = others.size() - 1; i >= 0; i--) {
            if (!selected.get(i)) {
              continue;
            }
            int node = others.node[i];
            float score = others.score[i];
            next.insertSorted(node, score);
          }
          enforceMaxConnLimit(next);
          return next;
        });
  }

  /**
   * Insert a new neighbor, maintaining our size cap by removing the least diverse neighbor if
   * necessary.
   */
  public void insert(int neighborId, float score) throws IOException {
    assert neighborId != nodeId : "can't add self as neighbor at node " + nodeId;
    neighborsRef.getAndUpdate(
        current -> {
          ConcurrentNeighborArray next = current.copy();
          next.insertSorted(neighborId, score);
          enforceMaxConnLimit(next);
          return next;
        });
  }

  // is the candidate node with the given score closer to the base node than it is to any of the
  // existing neighbors
  private boolean isDiverse(int node, float score, NeighborArray others, BitSet selected) {
    if (others.size() == 0) {
      return true;
    }

    NeighborSimilarity.ScoreFunction scoreProvider = similarity.scoreProvider(node);
    for (int i = others.size() - 1; i >= 0; i--) {
      if (!selected.get(i)) {
        continue;
      }
      int candidateNode = others.node[i];
      if (node == candidateNode) {
        break;
      }
      if (scoreProvider.apply(candidateNode) > score) {
        return false;
      }
    }
    return true;
  }

  private void enforceMaxConnLimit(NeighborArray neighbors) {
    while (neighbors.size() > maxConnections) {
      try {
        removeLeastDiverse(neighbors);
      } catch (IOException e) {
        throw new UncheckedIOException(e); // called from closures
      }
    }
  }

  /**
   * For each node e1 starting with the last neighbor (i.e. least similar to the base node), look at
   * all nodes e2 that are closer to the base node than e1 is. If any e2 is closer to e1 than e1 is
   * to the base node, remove e1.
   */
  private void removeLeastDiverse(NeighborArray neighbors) throws IOException {
    for (int i = neighbors.size() - 1; i >= 1; i--) {
      int e1Id = neighbors.node[i];
      float baseScore = neighbors.score[i];
      NeighborSimilarity.ScoreFunction scoreProvider = similarity.scoreProvider(e1Id);

      for (int j = i - 1; j >= 0; j--) {
        int n2Id = neighbors.node[j];
        float n1n2Score = scoreProvider.apply(n2Id);
        if (n1n2Score > baseScore) {
          neighbors.removeIndex(i);
          return;
        }
      }
    }

    // couldn't find any "non-diverse" neighbors, so remove the one farthest from the base node
    neighbors.removeIndex(neighbors.size() - 1);
  }

  /** Only for testing; this is a linear search */
  boolean contains(int i) {
    var it = this.nodeIterator();
    while (it.hasNext()) {
      if (it.nextInt() == i) {
        return true;
      }
    }
    return false;
  }

  /** A NeighborArray that knows how to copy itself and that checks for duplicate entries */
  static class ConcurrentNeighborArray extends NeighborArray {
    public ConcurrentNeighborArray(int maxSize, boolean descOrder) {
      super(maxSize, descOrder);
    }

    // two nodes may attempt to add each other in the Concurrent classes,
    // so we need to check if the node is already present.  this means that we can't use
    // the parent approach of "append it, and then move it into place"
    @Override
    public void insertSorted(int newNode, float newScore) {
      if (size == node.length) {
        growArrays();
      }
      int insertionPoint =
          scoresDescOrder
              ? descSortFindRightMostInsertionPoint(newScore, size)
              : ascSortFindRightMostInsertionPoint(newScore, size);
      if (!duplicateExistsNear(insertionPoint, newNode, newScore)) {
        System.arraycopy(node, insertionPoint, node, insertionPoint + 1, size - insertionPoint);
        System.arraycopy(score, insertionPoint, score, insertionPoint + 1, size - insertionPoint);
        node[insertionPoint] = newNode;
        score[insertionPoint] = newScore;
        ++size;
      }
    }

    private boolean duplicateExistsNear(int insertionPoint, int newNode, float newScore) {
      // Check to the left
      for (int i = insertionPoint - 1; i >= 0 && score[i] == newScore; i--) {
        if (node[i] == newNode) {
          return true;
        }
      }

      // Check to the right
      for (int i = insertionPoint; i < size && score[i] == newScore; i++) {
        if (node[i] == newNode) {
          return true;
        }
      }

      return false;
    }

    public ConcurrentNeighborArray copy() {
      ConcurrentNeighborArray copy = new ConcurrentNeighborArray(node.length, scoresDescOrder);
      copy.size = size;
      System.arraycopy(node, 0, copy.node, 0, size);
      System.arraycopy(score, 0, copy.score, 0, size);
      return copy;
    }
  }
}
