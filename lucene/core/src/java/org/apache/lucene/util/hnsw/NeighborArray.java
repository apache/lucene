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
import java.util.Comparator;
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
  public final ScoreNode[] scoreNodes;
  private int sortedNodeSize;
  public final ReadWriteLock rwlock = new ReentrantReadWriteLock(true);

  /** Stores nodeId and its similarity score in a single object */
  // noCommit
  public static final class ScoreNode {
    public int node;
    public float score;

    public ScoreNode(int node, float score) {
      this.node = node;
      this.score = score;
    }
  }

  /** Comparator for sorting {@link ScoreNode} objects. */
  public static class ScoreNodeComparator implements Comparator<ScoreNode> {
    private final boolean isDescByScore;

    public ScoreNodeComparator(boolean isDesc) {
      this.isDescByScore = isDesc;
    }

    @Override
    public int compare(ScoreNode o1, ScoreNode o2) {
      if (isDescByScore == false) {
        return Float.compare(o1.score, o2.score);
      }
      return Float.compare(o2.score, o1.score);
    }
  }

  public NeighborArray(int maxSize, boolean descOrder) {
    scoreNodes = new ScoreNode[maxSize];
    this.scoresDescOrder = descOrder;
  }

  /**
   * Add a new node to the NeighborArray. The new node must be worse than all previously stored
   * nodes. This cannot be called after {@link #addOutOfOrder(int, float)}
   */
  public void addInOrder(int newNode, float newScore) {
    assert size == sortedNodeSize : "cannot call addInOrder after addOutOfOrder";
    if (size == scoreNodes.length) {
      throw new IllegalStateException("No growth is allowed");
    }
    if (size > 0) {
      float previousScore = scoreNodes[size - 1].score;
      assert ((scoresDescOrder && (previousScore >= newScore))
              || (scoresDescOrder == false && (previousScore <= newScore)))
          : "Nodes are added in the incorrect order! Comparing "
              + newScore
              + " to "
              + Arrays.toString(ArrayUtil.copyOfSubArray(scoreNodes, 0, size));
    }
    scoreNodes[size] = new ScoreNode(newNode, newScore);
    ++size;
    ++sortedNodeSize;
  }

  /** Add node and newScore but do not insert as sorted */
  public void addOutOfOrder(int newNode, float newScore) {
    if (size == scoreNodes.length) {
      throw new IllegalStateException("No growth is allowed");
    }

    scoreNodes[size] = new ScoreNode(newNode, newScore);
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
  ScoreNode[] sort(RandomVectorScorer scorer) throws IOException {
    if (size == sortedNodeSize) {
      // all nodes checked and sorted
      return null;
    }
    assert sortedNodeSize < size;

    // Compute missing scores
    if (scorer != null) {
      for (int i = 0; i < size; i++) {
        if (Float.isNaN(scoreNodes[i].score)) {
          scoreNodes[i].score = scorer.score(scoreNodes[i].node);
        }
      }
    }

    ScoreNode[] uncheckedScoreNodes = new ScoreNode[size - sortedNodeSize];
    System.arraycopy(scoreNodes, sortedNodeSize, uncheckedScoreNodes, 0, size - sortedNodeSize);

    Comparator<ScoreNode> cmp = new ScoreNodeComparator(scoresDescOrder);
    Arrays.sort(scoreNodes, 0, size, cmp);
    Arrays.sort(uncheckedScoreNodes, cmp);
    sortedNodeSize = size;
    return uncheckedScoreNodes;
  }

  /** This method is for test only. */
  void insertSorted(int newNode, float newScore) throws IOException {
    addOutOfOrder(newNode, newScore);
    sort(null);
  }

  public int size() {
    return size;
  }

  /** Returns a copy of NeighborArray nodes, for calls that require concurrent modifications */
  public int[] nodesCopy() {
    int[] nodes = new int[size];
    nodesCopy(nodes);
    return nodes;
  }

  public void nodesCopy(int[] target) {
    assert target.length >= size;
    for (int i = 0; i < size; i++) {
      target[i] = scoreNodes[i].node;
    }
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

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(int nodeOrd, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    RandomVectorScorer scorer = scorerSupplier.scorer(nodeOrd);
    ScoreNode[] uncheckedScoreNodes = sort(scorer);
    assert uncheckedScoreNodes != null : "We will always have something unchecked";
    int uncheckedCursor = uncheckedScoreNodes.length - 1;
    for (int i = size - 1; i > 0; i--) {
      if (uncheckedCursor < 0) {
        // no unchecked node left
        break;
      }
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
   * Returns true if the node at candidateIndex is closer to one of the neighbors than it is to the
   * target node for this NeighborArray.
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
}
