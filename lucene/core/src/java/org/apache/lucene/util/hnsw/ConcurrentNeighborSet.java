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
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A concurrent set of neighbors
 *
 * <p>Neighbors are stored in a concurrent navigable set by encoding ordinal and score together in a
 * long. This means we can quickly iterate either forwards, or backwards.
 *
 * <p>The maximum connection count is loosely maintained -- meaning, we tolerate temporarily
 * exceeding the max size by a number of elements up to the number of threads performing concurrent
 * inserts, but it will always be reduced back to the cap immediately afterwards. This avoids taking
 * out a Big Lock to impose a strict cap.
 */
public class ConcurrentNeighborSet {
  private final int nodeId;
  private final AtomicReference<ConcurrentNeighborArray> neighborsRef;
  private final int maxConnections;

  public ConcurrentNeighborSet(int nodeId, int maxConnections) {
    this.nodeId = nodeId;
    this.maxConnections = maxConnections;
    neighborsRef = new AtomicReference<>(new ConcurrentNeighborArray(maxConnections, true));
  }

  public PrimitiveIterator.OfInt nodeIterator() {
    // don't use a stream here. stream's implementation of iterator buffers
    // very aggressively, which is a big waste for a lot of searches
    return new NeighborIterator(neighborsRef.get());
  }

  public void backlink(
      Function<Integer, ConcurrentNeighborSet> neighborhoodOf,
      BiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    NeighborArray neighbors = neighborsRef.get();
    for (int i = 0; i < neighbors.size(); i++) {
      int nbr = neighbors.node[i];
      float nbrScore = neighbors.score[i];
      ConcurrentNeighborSet nbrNbr = neighborhoodOf.apply(nbr);
      nbrNbr.insert(nodeId, nbrScore, scoreBetween);
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
  public void insertDiverse(
      NeighborArray candidates, BiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    NeighborArray selected = new NeighborArray(candidates.size(), true);
    for (int i = candidates.size() - 1; i >= 0; i--) {
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      if (isDiverse(cNode, cScore, selected, scoreBetween)) {
        selected.add(cNode, cScore);
      }
    }
    insertMultiple(selected, scoreBetween);
    // This leaves the paper's keepPrunedConnection option out; we might want to add that
    // as an option in the future.
  }

  private void insertMultiple(
      NeighborArray selected, BiFunction<Integer, Integer, Float> scoreBetween) {
    neighborsRef.getAndUpdate(
        current -> {
          ConcurrentNeighborArray next = current.copy();
          for (int i = 0; i < selected.size(); i++) {
            int node = selected.node[i];
            float score = selected.score[i];
            next.insertSorted(node, score);
          }
          enforceMaxConnLimit(next, scoreBetween);
          return next;
        });
  }

  /**
   * Insert a new neighbor, maintaining our size cap by removing the least diverse neighbor if
   * necessary.
   */
  public void insert(int neighborId, float score, BiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    assert neighborId != nodeId : "can't add self as neighbor at node " + nodeId;
    neighborsRef.getAndUpdate(
        current -> {
          ConcurrentNeighborArray next = current.copy();
          next.insertSorted(neighborId, score);
          enforceMaxConnLimit(next, scoreBetween);
          return next;
        });
  }

  // is the candidate node with the given score closer to the base node than it is to any of the
  // existing neighbors
  private boolean isDiverse(
      int node, float score, NeighborArray others, BiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    for (int i = 0; i < others.size(); i++) {
      int candidateNode = others.node[i];
      if (scoreBetween.apply(candidateNode, node) > score) {
        return false;
      }
    }
    return true;
  }

  private void enforceMaxConnLimit(
      NeighborArray neighbors, BiFunction<Integer, Integer, Float> scoreBetween) {
    while (neighbors.size() > maxConnections) {
      try {
        removeLeastDiverse(neighbors, scoreBetween);
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
  private void removeLeastDiverse(
      NeighborArray neighbors, BiFunction<Integer, Integer, Float> scoreBetween)
      throws IOException {
    for (int i = neighbors.size() - 1; i >= 1; i--) {
      int e1Id = neighbors.node[i];
      float baseScore = neighbors.score[i];
      for (int j = i - 1; j >= 0; j--) {
        int n2Id = neighbors.node[j];
        float n1n2Score = scoreBetween.apply(e1Id, n2Id);
        if (n1n2Score > baseScore) {
          neighbors.removeIndex(i);
          return;
        }
      }
    }
    // couldn't find any "non-diverse" neighbors, so remove the one farthest from the base node
    neighbors.removeIndex(neighbors.size() - 1);
  }

  /** This is O(n) because we have to decode node ids! Only for testing. */
  boolean contains(int i) {
    var it = this.nodeIterator();
    while (it.hasNext()) {
      if (it.nextInt() == i) {
        return true;
      }
    }
    return false;
  }

  private static class ConcurrentNeighborArray extends NeighborArray {
    public ConcurrentNeighborArray(int maxSize, boolean descOrder) {
      this(maxSize, descOrder, true);
    }

    private ConcurrentNeighborArray(int maxSize, boolean descOrder, boolean fillNodes) {
      super(maxSize, descOrder);
      if (fillNodes) {
        Arrays.fill(node, -1);
      }
    }

    @Override
    protected void growArrays() {
      int oldLength = node.length;
      super.growArrays();
      Arrays.fill(node, oldLength, node.length, -1);
    }

    @Override
    public void insertSorted(int newNode, float newScore) {
      if (size == node.length) {
        growArrays();
      }
      int insertionPoint =
          scoresDescOrder
              ? descSortFindRightMostInsertionPoint(newScore)
              : ascSortFindRightMostInsertionPoint(newScore);
      // two nodes may attempt to add each other in the Concurrent classes,
      // so we need to check if the node is already present
      if (node[insertionPoint] != newNode) {
        System.arraycopy(node, insertionPoint, node, insertionPoint + 1, size - insertionPoint);
        System.arraycopy(score, insertionPoint, score, insertionPoint + 1, size - insertionPoint);
        node[insertionPoint] = newNode;
        score[insertionPoint] = newScore;
        ++size;
      }
    }

    public ConcurrentNeighborArray copy() {
      ConcurrentNeighborArray copy =
          new ConcurrentNeighborArray(node.length, scoresDescOrder, false);
      copy.size = size;
      System.arraycopy(node, 0, copy.node, 0, size);
      Arrays.fill(copy.node, size, node.length, -1);
      System.arraycopy(score, 0, copy.score, 0, size);
      return copy;
    }
  }
}
