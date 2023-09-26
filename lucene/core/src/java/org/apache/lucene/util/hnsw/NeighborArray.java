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
import org.apache.lucene.util.ArrayUtil;

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays. Nodes are arranged in the sorted order of their scores in descending order
 * (if scoresDescOrder is true), or in the ascending order of their scores (if scoresDescOrder is
 * false)
 *
 * @lucene.internal
 */
public class NeighborArray {
  private final boolean scoresDescOrder;
  private int size;
  float[] score;
  int[] node;
  private int sortedNodeSize;

  public NeighborArray(int maxSize, boolean descOrder) {
    node = new int[maxSize];
    score = new float[maxSize];
    this.scoresDescOrder = descOrder;
  }

  /**
   * Add a new node to the NeighborArray. The new node must be worse than all previously stored
   * nodes. This cannot be called after {@link #addOutOfOrder(int, float)}
   */
  public void addInOrder(int newNode, float newScore) {
    assert size == sortedNodeSize : "cannot call addInOrder after addOutOfOrder";
    if (size == node.length) {
      node = ArrayUtil.grow(node);
      score = ArrayUtil.growExact(score, node.length);
    }
    if (size > 0) {
      float previousScore = score[size - 1];
      assert ((scoresDescOrder && (previousScore >= newScore))
              || (scoresDescOrder == false && (previousScore <= newScore)))
          : "Nodes are added in the incorrect order! Comparing "
              + newScore
              + " to "
              + Arrays.toString(ArrayUtil.copyOfSubArray(score, 0, size));
    }
    node[size] = newNode;
    score[size] = newScore;
    ++size;
    ++sortedNodeSize;
  }

  /** Add node and newScore but do not insert as sorted */
  public void addOutOfOrder(int newNode, float newScore) {
    if (size == node.length) {
      node = ArrayUtil.grow(node);
      score = ArrayUtil.growExact(score, node.length);
    }

    score[size] = newScore;
    node[size] = newNode;
    size++;
  }

  /**
   * Sort the array according to scores, and return the sorted indexes of previous unsorted nodes
   * (unchecked nodes)
   *
   * @return indexes of newly sorted (unchecked) nodes, in ascending order, or null if the array is
   *     already fully sorted
   */
  public int[] sort(RandomVectorScorer scorer) throws IOException {
    if (size == sortedNodeSize) {
      // all nodes checked and sorted
      return null;
    }
    assert sortedNodeSize < size;
    int[] uncheckedIndexes = new int[size - sortedNodeSize];
    int count = 0;
    while (sortedNodeSize != size) {
      uncheckedIndexes[count] = insertSortedInternal(scorer); // sortedNodeSize is increased inside
      for (int i = 0; i < count; i++) {
        if (uncheckedIndexes[i] >= uncheckedIndexes[count]) {
          // the previous inserted nodes has been shifted
          uncheckedIndexes[i]++;
        }
      }
      count++;
    }
    Arrays.sort(uncheckedIndexes);
    return uncheckedIndexes;
  }

  /** insert the first unsorted node into its sorted position */
  private int insertSortedInternal(RandomVectorScorer scorer) throws IOException {
    assert sortedNodeSize < size : "Call this method only when there's unsorted node";
    int tmpNode = node[sortedNodeSize];
    float tmpScore = score[sortedNodeSize];

    if (Float.isNaN(tmpScore)) {
      tmpScore = scorer.score(tmpNode);
    }

    int insertionPoint =
        scoresDescOrder
            ? descSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize)
            : ascSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize);
    System.arraycopy(
        node, insertionPoint, node, insertionPoint + 1, sortedNodeSize - insertionPoint);
    System.arraycopy(
        score, insertionPoint, score, insertionPoint + 1, sortedNodeSize - insertionPoint);
    node[insertionPoint] = tmpNode;
    score[insertionPoint] = tmpScore;
    ++sortedNodeSize;
    return insertionPoint;
  }

  /** This method is for test only. */
  void insertSorted(int newNode, float newScore) throws IOException {
    addOutOfOrder(newNode, newScore);
    insertSortedInternal(null);
  }

  public int size() {
    return size;
  }

  /**
   * Direct access to the internal list of node ids; provided for efficient writing of the graph
   *
   * @lucene.internal
   */
  public int[] node() {
    return node;
  }

  public float[] score() {
    return score;
  }

  public void clear() {
    size = 0;
    sortedNodeSize = 0;
  }

  public void removeLast() {
    size--;
    sortedNodeSize = Math.min(sortedNodeSize, size);
  }

  public void removeIndex(int idx) {
    System.arraycopy(node, idx + 1, node, idx, size - idx - 1);
    System.arraycopy(score, idx + 1, score, idx, size - idx - 1);
    if (idx < sortedNodeSize) {
      sortedNodeSize--;
    }
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }

  private int ascSortFindRightMostInsertionPoint(float newScore, int bound) {
    int insertionPoint = Arrays.binarySearch(score, 0, bound, newScore);
    if (insertionPoint >= 0) {
      // find the right most position with the same score
      while ((insertionPoint < bound - 1) && (score[insertionPoint + 1] == score[insertionPoint])) {
        insertionPoint++;
      }
      insertionPoint++;
    } else {
      insertionPoint = -insertionPoint - 1;
    }
    return insertionPoint;
  }

  private int descSortFindRightMostInsertionPoint(float newScore, int bound) {
    int start = 0;
    int end = bound - 1;
    while (start <= end) {
      int mid = (start + end) / 2;
      if (score[mid] < newScore) end = mid - 1;
      else start = mid + 1;
    }
    return start;
  }
}
