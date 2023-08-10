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

/**
 * Encapsulates comparing node distances for diversity checks.
 */
public interface NeighborSimilarity {
  /**
   * for one-off comparisons between nodes
   */
  float score(int node1, int node2);

  /**
   * For when we're going to compare node1 with multiple other nodes. This allows us to skip
   * loading node1's vector (potentially from disk) redundantly for each comparison.
   */
  ScoreFunction scoreProvider(int node1);

  /**
   * A Function&lt;Integer, Float&gt; without the boxing
   */
  @FunctionalInterface
  interface ScoreFunction {
    float apply(int node);
  }
}
