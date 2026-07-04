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
import java.util.Arrays;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays. Nodes are arranged in the sorted order of their scores in descending order
 * (if scoresDescOrder is true), or in the ascending order of their scores (if scoresDescOrder is
 * false)
 *
 * @lucene.internal
 */
public class NeighborArray {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(NeighborArray.class);

  private final boolean scoresDescOrder;
  private int size;
  private final int maxSize;
  private float[] scores;
  private int[] nodes;
  private int sortedNodeSize;
  private final LongConsumer onHeapMemoryUsageListener;

  public NeighborArray(int maxSize, boolean descOrder) {
    this(maxSize, descOrder, null);
  }

  public NeighborArray(int maxSize, boolean descOrder, LongConsumer onHeapMemoryUsageListener) {
    this.maxSize = maxSize;
    nodes = new int[maxSize / 8];
    scores = new float[maxSize / 8];
    this.scoresDescOrder = descOrder;
    this.onHeapMemoryUsageListener = onHeapMemoryUsageListener;
    if (onHeapMemoryUsageListener != null) {
      onHeapMemoryUsageListener.accept(
          BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(nodes) + RamUsageEstimator.sizeOf(scores));
    }
  }

  /**
   * Add a new node to the NeighborArray. The new node must be worse than all previously stored
   * nodes. This cannot be called after {@link #addOutOfOrder(int, float)}
   */
  public void addInOrder(int newNode, float newScore) {
    assert size == sortedNodeSize : "cannot call addInOrder after addOutOfOrder";
    if (size == maxSize) {
      throw new IllegalStateException("No growth is allowed");
    }
    if (size > 0) {
      float previousScore = scores[size - 1];
      assert ((scoresDescOrder && (previousScore >= newScore))
              || (scoresDescOrder == false && (previousScore <= newScore)))
          : "Nodes are added in an incorrect order! Comparing "
              + newScore
              + " to "
              + IntStream.range(0, size)
                  .mapToObj(i -> "" + scores[i])
                  .collect(Collectors.joining(", ", "[", "]"));
    }
    growArrays();
    nodes[size] = newNode;
    scores[size] = newScore;
    ++size;
    ++sortedNodeSize;
  }

  /// Grow the [#scores] and [#nodes] fields if they are full, up to [#maxSize]. Grow in larger steps.
  private void growArrays() {
    if (size == maxSize) {
      throw new IllegalStateException("Cannot grow beyond maxSize: " + maxSize);
    }
    if (size == nodes.length) {
      int oldLength = nodes.length;
      nodes = ArrayUtil.growInRange(nodes, size + 1, maxSize);
      scores = ArrayUtil.growInRange(scores, size + 1, maxSize);
      alertOnHeapMemoryUsageChange(nodes.length, oldLength);
    }
  }

  /** Add node and newScore but do not insert as sorted */
  public void addOutOfOrder(int newNode, float newScore) {
    if (size == maxSize) {
      throw new IllegalStateException("No growth is allowed");
    }
    growArrays();
    nodes[size] = newNode;
    scores[size] = newScore;
    size++;
  }

  private void alertOnHeapMemoryUsageChange(int newLength, int previousLength) {
    if (onHeapMemoryUsageListener != null) {
      int lengthDelta = newLength - previousLength;
      onHeapMemoryUsageListener.accept(
          (long) (lengthDelta) * Integer.BYTES + (long) (lengthDelta) * Float.BYTES);
    }
  }

  /**
   * In addition to {@link #addOutOfOrder(int, float)}, this function will also remove the
   * least-diverse node if the node array is full after insertion
   *
   * <p>In multi-threading environment, this method need to be locked as it will be called by
   * multiple threads while other add method is only supposed to be called by one thread.
   *
   * @param nodeId node Id of the owner of this NeighbourArray
   */
  public void addAndEnsureDiversity(
      int newNode, float newScore, int nodeId, UpdateableRandomVectorScorer scorer)
      throws IOException {
    addOutOfOrder(newNode, newScore);
    if (size < maxSize) {
      return;
    }
    // we're oversize, need to do diversity check and pop out the least diverse neighbour
    scorer.setScoringOrdinal(nodeId);
    removeIndex(findWorstNonDiverse(scorer));
    assert size == maxSize - 1;
  }

  /**
   * Sort the array according to scores, and return the sorted indexes of previous unsorted nodes
   * (unchecked nodes)
   *
   * @return indexes of newly sorted (unchecked) nodes, in ascending order, or null if the array is
   *     already fully sorted
   */
  int[] sort(RandomVectorScorer scorer) throws IOException {
    if (size == sortedNodeSize) {
      // all nodes checked and sorted
      return null;
    }
    assert sortedNodeSize < size;

    int count = size - sortedNodeSize;
    int[] uncheckedIndexes = new int[count];
    Integer[] unsortedIndexes = new Integer[count];
    int[] newNodes = new int[size];
    float[] newScores = new float[size];

    for (int i = 0; i < count; i++) {
      int index = sortedNodeSize + i;
      unsortedIndexes[i] = index;
      float score = scores[index];
      if (Float.isNaN(score)) {
        score = scorer.score(nodes[index]);
      }
      scores[index] = score;
    }

    Arrays.sort(
        unsortedIndexes,
        (left, right) -> {
          float leftScore = scores[left];
          float rightScore = scores[right];
          if (scoresDescOrder) {
            if (leftScore > rightScore) {
              return -1;
            } else if (leftScore < rightScore) {
              return 1;
            }
          } else {
            if (leftScore < rightScore) {
              return -1;
            } else if (leftScore > rightScore) {
              return 1;
            }
          }
          return 0;
        });

    int prefixCursor = 0;
    int unsortedCursor = 0;
    int outCursor = 0;
    while (prefixCursor < sortedNodeSize && unsortedCursor < count) {
      int prefixIndex = prefixCursor;
      int unsortedIndex = unsortedIndexes[unsortedCursor];
      if (shouldTakePrefix(prefixIndex, unsortedIndex)) {
        newNodes[outCursor] = nodes[prefixIndex];
        newScores[outCursor] = scores[prefixIndex];
        prefixCursor++;
      } else {
        newNodes[outCursor] = nodes[unsortedIndex];
        newScores[outCursor] = scores[unsortedIndex];
        uncheckedIndexes[unsortedCursor] = outCursor;
        unsortedCursor++;
      }
      outCursor++;
    }

    while (prefixCursor < sortedNodeSize) {
      newNodes[outCursor] = nodes[prefixCursor];
      newScores[outCursor] = scores[prefixCursor];
      prefixCursor++;
      outCursor++;
    }

    while (unsortedCursor < count) {
      int unsortedIndex = unsortedIndexes[unsortedCursor];
      newNodes[outCursor] = nodes[unsortedIndex];
      newScores[outCursor] = scores[unsortedIndex];
      uncheckedIndexes[unsortedCursor] = outCursor;
      unsortedCursor++;
      outCursor++;
    }

    nodes = newNodes;
    scores = newScores;
    sortedNodeSize = size;
    Arrays.sort(uncheckedIndexes);
    return uncheckedIndexes;
  }

  private boolean shouldTakePrefix(int prefixIndex, int unsortedIndex) {
    float prefixScore = scores[prefixIndex];
    float unsortedScore = scores[unsortedIndex];
    if (scoresDescOrder) {
      return prefixScore >= unsortedScore;
    }
    return prefixScore <= unsortedScore;
  }

  /** This method is for test only. */
  void insertSorted(int newNode, float newScore) throws IOException {
    addOutOfOrder(newNode, newScore);
    if (size == sortedNodeSize) {
      return;
    }
    sort(null);
  }

  public int size() {
    return size;
  }

  /**
   * Direct access to the internal list of node ids; provided for efficient writing of the graph
   *
   * @lucene.internal
   */
  public int[] nodes() {
    return nodes;
  }

  /**
   * Get the score at the given index
   *
   * @param i index of the score to get
   * @return the score at the given index
   */
  public float getScores(int i) {
    return scores[i];
  }

  public void clear() {
    size = 0;
    sortedNodeSize = 0;
    Arrays.fill(nodes, 0, size, 0);
    Arrays.fill(scores, 0, size, 0f);
  }

  void removeLast() {
    assert size > 0;
    size--;
    sortedNodeSize = Math.min(sortedNodeSize, size);
  }

  void removeIndex(int idx) {
    if (idx == size - 1) {
      removeLast();
      return;
    }
    System.arraycopy(nodes, idx + 1, nodes, idx, size - idx - 1);
    System.arraycopy(scores, idx + 1, scores, idx, size - idx - 1);
    if (idx < sortedNodeSize) {
      sortedNodeSize--;
    }
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(UpdateableRandomVectorScorer scorer) throws IOException {
    int[] uncheckedIndexes = sort(scorer);
    assert uncheckedIndexes != null : "We will always have something unchecked";
    int uncheckedCursor = uncheckedIndexes.length - 1;
    for (int i = size - 1; i > 0; i--) {
      if (uncheckedCursor < 0) {
        // no unchecked node left
        break;
      }
      scorer.setScoringOrdinal(nodes[i]);
      if (isWorstNonDiverse(i, uncheckedIndexes, uncheckedCursor, scorer)) {
        return i;
      }
      if (i == uncheckedIndexes[uncheckedCursor]) {
        uncheckedCursor--;
      }
    }
    return size - 1;
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, int[] uncheckedIndexes, int uncheckedCursor, RandomVectorScorer scorer)
      throws IOException {
    float minAcceptedSimilarity = scores[candidateIndex];
    if (candidateIndex == uncheckedIndexes[uncheckedCursor]) {
      // the candidate itself is unchecked
      for (int i = candidateIndex - 1; i >= 0; i--) {
        float neighborSimilarity = scorer.score(nodes[i]);
        // candidate node is too similar to node i given its score relative to the base node
        if (neighborSimilarity >= minAcceptedSimilarity) {
          return true;
        }
      }
    } else {
      // else we just need to make sure candidate does not violate diversity with the (newly
      // inserted) unchecked nodes
      assert candidateIndex > uncheckedIndexes[uncheckedCursor];
      for (int i = uncheckedCursor; i >= 0; i--) {
        float neighborSimilarity = scorer.score(nodes[uncheckedIndexes[i]]);
        // candidate node is too similar to node i given its score relative to the base node
        if (neighborSimilarity >= minAcceptedSimilarity) {
          return true;
        }
      }
    }
    return false;
  }

  public int maxSize() {
    return maxSize;
  }
}
