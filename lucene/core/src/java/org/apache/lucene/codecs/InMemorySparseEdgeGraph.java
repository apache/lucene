/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple in-memory implementation of {@link SparseEdgeGraph}.
 * Intended for unit testing and non-production use.
 */
public final class InMemorySparseEdgeGraph implements SparseEdgeGraph {

  private final Map<Integer, int[]> neighbors = new HashMap<>();
  private final Map<Integer, float[]> weights = new HashMap<>();

  private void addDirectedEdge(int from, int to, float weight) {
    int[] existingNeighbors = neighbors.get(from);
    float[] existingWeights = weights.get(from);

    int len = existingNeighbors == null ? 0 : existingNeighbors.length;
    int[] newNeighbors = new int[len + 1];
    float[] newWeights = new float[len + 1];

    if (len > 0) {
      System.arraycopy(existingNeighbors, 0, newNeighbors, 0, len);
      System.arraycopy(existingWeights, 0, newWeights, 0, len);
    }

    newNeighbors[len] = to;
    newWeights[len] = weight;

    neighbors.put(from, newNeighbors);
    weights.put(from, newWeights);
  }

  /**
   * Adds a symmetric edge between two documents with the given weight.
   */
  @Override
  public void addEdge(int docA, int docB, float weight) {
    addDirectedEdge(docA, docB, weight);
    addDirectedEdge(docB, docA, weight);
  }

  @Override
  public int[] getNeighbors(int docID) {
    return neighbors.getOrDefault(docID, new int[0]);
  }

  @Override
  public float[] getWeights(int docID) {
    return weights.getOrDefault(docID, new float[0]);
  }

   @Override
  public void ensureVertex(int docID) {
    neighbors.computeIfAbsent(docID, k -> new int[0]);
    weights.computeIfAbsent(docID, k -> new float[0]);
  }

  /**
   * Returns the number of vertices in the graph with at least one edge.
   */
  @Override
  public int size() {
    return neighbors.size();
  }
}