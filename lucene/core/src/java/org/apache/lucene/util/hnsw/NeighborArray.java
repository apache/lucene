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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
  private final ScoreNode[] scoreNodes;
//  private final float[] scores;
//  private final int[] nodes;
  private int sortedNodeSize;
  public final ReadWriteLock rwlock = new ReentrantReadWriteLock(true);

  private static final class ScoreNode {
    private final int node;
    private float score;
    public ScoreNode(int node, float score) {
      this.node = node;
      this.score = score;
    }
  }

  public NeighborArray(int maxSize, boolean descOrder) {
//    nodes = new int[maxSize];
//    scores = new float[maxSize];
    scoreNodes = new ScoreNode[maxSize];
    this.scoresDescOrder = descOrder;
  }

  /**
   * Add a new node to the NeighborArray. The new node must be worse than all previously stored
   * nodes. This cannot be called after {@link #addOutOfOrder(int, float)}
   */
  public void addInOrder(int newNode, float newScore) {
    assert size == sortedNodeSize : "cannot call addInOrder after addOutOfOrder";
//    if (size == nodes.length) {
    if (size == scoreNodes.length) {
      throw new IllegalStateException("No growth is allowed");
    }
    if (size > 0) {
//      float previousScore = scores[size - 1];
      float previousScore = scoreNodes[size - 1].score;
      assert ((scoresDescOrder && (previousScore >= newScore))
              || (scoresDescOrder == false && (previousScore <= newScore)))
          : "Nodes are added in the incorrect order! Comparing "
              + newScore
              + " to "
              + Arrays.toString(ArrayUtil.copyOfSubArray(scoreNodes, 0, size));
    }
    scoreNodes[size] = new ScoreNode(newNode, newScore);
//    nodes[size] = newNode;
//    scores[size] = newScore;
    ++size;
    ++sortedNodeSize;
  }

  /** Add node and newScore but do not insert as sorted */
  public void addOutOfOrder(int newNode, float newScore) {
//    if (size == nodes.length) {
    if (size == scoreNodes.length) {
      throw new IllegalStateException("No growth is allowed");
    }

    scoreNodes[size] = new ScoreNode(newNode, newScore);
//    scores[size] = newScore;
//    nodes[size] = newNode;
    size++;
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
      int newNode, float newScore, int nodeId, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    addOutOfOrder(newNode, newScore);
    if (size < scoreNodes.length) {
      return;
    }
    // we're oversize, need to do diversity check and pop out the least diverse neighbour
    removeIndex(findWorstNonDiverse(nodeId, scorerSupplier));
    assert size == scoreNodes.length - 1;
  }

  /**
   * Sort the array according to scores, and return the sorted indexes of previous unsorted nodes
   * (unchecked nodes)
   *
   * @return indexes of newly sorted (unchecked) nodes, in ascending order, or null if the array is
   *     already fully sorted
   */
//  int[] sort(RandomVectorScorer scorer) throws IOException {
//  void sort(RandomVectorScorer scorer) throws IOException {
  ScoreNode[] sort(RandomVectorScorer scorer) throws IOException {
    if (size == sortedNodeSize) {
      // all nodes checked and sorted
//      return;
      return null;
    }
    assert sortedNodeSize < size;
    ScoreNode[] uncheckedScoreNodes = new ScoreNode[size - sortedNodeSize];
    for (int i = sortedNodeSize; i < size; i++) {
      uncheckedScoreNodes[i - sortedNodeSize] = scoreNodes[i];
    }

    // simply sort the entire array
    Arrays.sort(scoreNodes, (o1, o2) -> {
      try {
        if (Float.isNaN(o1.score)) {
          o1.score = scorer.score(o1.node);
        }
        if (Float.isNaN(o2.score)) {
          o2.score = scorer.score(o2.node);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (scoresDescOrder == false) {
        return Float.compare(o1.score, o2.score);
      }
      return Float.compare(o2.score, o1.score);
    });
    sortedNodeSize = size;

    // sort the unchecked scoreNodes as well
    Arrays.sort(uncheckedScoreNodes, (o1, o2) -> {
      if (scoresDescOrder == false) {
        return Float.compare(o1.score, o2.score);
      }
      return Float.compare(o2.score, o1.score);
    });
    return uncheckedScoreNodes;

//    int[] uncheckedIndexes = new int[size - sortedNodeSize];
//    int count = 0;
//    while (sortedNodeSize != size) {
//      // TODO: Instead of do an array copy on every insertion, I think we can do better here:
//      //       Remember the insertion point of each unsorted node and insert them altogether
//      //       We can save several array copy by doing that
//      uncheckedIndexes[count] = insertSortedInternal(scorer); // sortedNodeSize is increased inside
//      for (int i = 0; i < count; i++) {
//        if (uncheckedIndexes[i] >= uncheckedIndexes[count]) {
//          // the previous inserted nodes has been shifted
//          uncheckedIndexes[i]++;
//        }
//      }
//      count++;
//    }
//    Arrays.sort(uncheckedIndexes);
//    return uncheckedIndexes;
  }

//  /** insert the first unsorted node into its sorted position */
//  private int insertSortedInternal(RandomVectorScorer scorer) throws IOException {
//    assert sortedNodeSize < size : "Call this method only when there's unsorted node";
//    int tmpNode = nodes[sortedNodeSize];
//    float tmpScore = scores[sortedNodeSize];
//
//    if (Float.isNaN(tmpScore)) {
//      tmpScore = scorer.score(tmpNode);
//    }
//
//    int insertionPoint =
//        scoresDescOrder
//            ? descSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize)
//            : ascSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize);
//    System.arraycopy(
//        nodes, insertionPoint, nodes, insertionPoint + 1, sortedNodeSize - insertionPoint);
//    System.arraycopy(
//        scores, insertionPoint, scores, insertionPoint + 1, sortedNodeSize - insertionPoint);
//    nodes[insertionPoint] = tmpNode;
//    scores[insertionPoint] = tmpScore;
//    ++sortedNodeSize;
//    return insertionPoint;
//  }

  /** This method is for test only. */
  void insertSorted(int newNode, float newScore) throws IOException {
    addOutOfOrder(newNode, newScore);
    sort(null);
//    insertSortedInternal(null);
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
    int[] nodes = new int[size];
    for (int i = 0; i < size; i++) {
      nodes[i] = scoreNodes[i].node;
    }
    return nodes;
  }

  public float[] scores() {
    float[] scores = new float[size];
    for (int i = 0; i < size; i++) {
      scores[i] = scoreNodes[i].score;
    }
    return scores;
  }

  public void clear() {
    size = 0;
    sortedNodeSize = 0;
  }

  void removeLast() {
    size--;
    sortedNodeSize = Math.min(sortedNodeSize, size);
  }

  void removeIndex(int idx) {
    if (idx == size - 1) {
      removeLast();
      return;
    }
//    System.arraycopy(nodes, idx + 1, nodes, idx, size - idx - 1);
//    System.arraycopy(scores, idx + 1, scores, idx, size - idx - 1);
    System.arraycopy(scoreNodes, idx + 1, scoreNodes, idx, size - idx - 1);
    if (idx < sortedNodeSize) {
      sortedNodeSize--;
    }
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }

//  private int ascSortFindRightMostInsertionPoint(float newScore, int bound) {
//    int insertionPoint = Arrays.binarySearch(scores, 0, bound, newScore);
//    if (insertionPoint >= 0) {
//      // find the right most position with the same score
//      while ((insertionPoint < bound - 1)
//          && (scores[insertionPoint + 1] == scores[insertionPoint])) {
//        insertionPoint++;
//      }
//      insertionPoint++;
//    } else {
//      insertionPoint = -insertionPoint - 1;
//    }
//    return insertionPoint;
//  }

//  private int descSortFindRightMostInsertionPoint(float newScore, int bound) {
//    int start = 0;
//    int end = bound - 1;
//    while (start <= end) {
//      int mid = (start + end) / 2;
//      if (scores[mid] < newScore) end = mid - 1;
//      else start = mid + 1;
//    }
//    return start;
//  }

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(int nodeOrd, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    RandomVectorScorer scorer = scorerSupplier.scorer(nodeOrd);
//    int[] uncheckedIndexes = sort(scorer);
    ScoreNode[] uncheckedScoreNodes = sort(scorer);
//    assert uncheckedIndexes != null : "We will always have something unchecked";
    assert uncheckedScoreNodes != null : "We will always have something unchecked";
//    int uncheckedCursor = uncheckedIndexes.length - 1;
    int uncheckedCursor = uncheckedScoreNodes.length - 1;
    for (int i = size - 1; i > 0; i--) {
      if (uncheckedCursor < 0) {
        // no unchecked node left
        break;
      }
//      if (isWorstNonDiverse(i, uncheckedIndexes, uncheckedCursor, scorerSupplier)) {
      if (isWorstNonDiverse(i, uncheckedScoreNodes, uncheckedCursor, scorerSupplier)) {
        return i;
      }
      if (scoreNodes[i].node == uncheckedScoreNodes[uncheckedCursor].node) {
        uncheckedCursor--;
      }
    }
    return size - 1;
  }

  /**
   * Returns true if the node at candidateIndex is closer to one of the neighbors than it is
   * to the target node for this NeighborArray.
   */
  private boolean isWorstNonDiverse(
    int candidateIndex,
    ScoreNode[] uncheckedScoreNodes,
    int uncheckedCursor,
    RandomVectorScorerSupplier scorerSupplier)
    throws IOException {
    float targetNodeSimilarity = scoreNodes[candidateIndex].score;
    RandomVectorScorer scorer = scorerSupplier.scorer(scoreNodes[candidateIndex].node);
    if (scoreNodes[candidateIndex].node == uncheckedScoreNodes[uncheckedCursor].node) {
      // if the candidate node is unchecked, compare it against all the neighbor nodes
      for (int i = candidateIndex - 1; i >= 0; i--) {
        float neighborSimilarity = scorer.score(scoreNodes[i].node);
        if (neighborSimilarity >= targetNodeSimilarity) {
          return true;
        }
      }
    } else {
      // we know that the candidate is non-diverse amongst the checked neighbor nodes,
      // only need to compare against the unchecked nodes here.
      for (int i = uncheckedCursor; i >= 0; i--) {
        float neighborSimilarity = scorer.score(uncheckedScoreNodes[i].node);
        if (neighborSimilarity >= targetNodeSimilarity) {
          return true;
        }
      }
    }
    return false;
  }

//  private boolean isWorstNonDiverse(
//      int candidateIndex,
//      int[] uncheckedIndexes,
//      int uncheckedCursor,
//      RandomVectorScorerSupplier scorerSupplier)
//      throws IOException {
//    float minAcceptedSimilarity = scores[candidateIndex];
//    RandomVectorScorer scorer = scorerSupplier.scorer(nodes[candidateIndex]);
//    if (candidateIndex == uncheckedIndexes[uncheckedCursor]) {
//      // the candidate itself is unchecked
//      for (int i = candidateIndex - 1; i >= 0; i--) {
//        float neighborSimilarity = scorer.score(nodes[i]);
//        // candidate node is too similar to node i given its score relative to the base node
//        if (neighborSimilarity >= minAcceptedSimilarity) {
//          return true;
//        }
//      }
//    } else {
//      // else we just need to make sure candidate does not violate diversity with the (newly
//      // inserted) unchecked nodes
//      assert candidateIndex > uncheckedIndexes[uncheckedCursor];
//      for (int i = uncheckedCursor; i >= 0; i--) {
//        float neighborSimilarity = scorer.score(nodes[uncheckedIndexes[i]]);
//        // candidate node is too similar to node i given its score relative to the base node
//        if (neighborSimilarity >= minAcceptedSimilarity) {
//          return true;
//        }
//      }
//    }
//    return false;
//  }
}
