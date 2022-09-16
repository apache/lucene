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

package org.apache.lucene.backward_codecs.lucene91;

import org.apache.lucene.util.ArrayUtil;

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays.
 *
 * @lucene.internal
 */
public class Lucene91NeighborArray {

  private int size;

  float[] score;
  int[] node;

  /** Create a neighbour array with the given initial size */
  public Lucene91NeighborArray(int maxSize) {
    node = new int[maxSize];
    score = new float[maxSize];
  }

  /** Add a new node with a score */
  public void add(int newNode, float newScore) {
    if (size == node.length - 1) {
      node = ArrayUtil.grow(node, (size + 1) * 3 / 2);
      score = ArrayUtil.growExact(score, node.length);
    }
    node[size] = newNode;
    score[size] = newScore;
    ++size;
  }

  /** Get the size, the number of nodes added so far */
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

  /**
   * Direct access to the internal list of scores
   *
   * @lucene.internal
   */
  public float[] score() {
    return score;
  }

  /** Clear all the nodes in the array */
  public void clear() {
    size = 0;
  }

  /** Remove the last nodes from the array */
  public void removeLast() {
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }
}
