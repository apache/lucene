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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;

/**
 * The {@link RandomVectorScorer} calculates scores between an abstract query and a random node. It
 * can also score two random nodes stored in the graph. The scores returned must be strictly
 * positive for them to be usable in a {@link Query}.
 */
public interface RandomVectorScorer {
  /**
   * Returns the score between the query and the provided node.
   *
   * @param node a random node in the graph
   * @return the computed score
   */
  float queryScore(int node) throws IOException;

  /**
   * Returns the score between two nodes.
   *
   * @param node1 the first node
   * @param node2 the second node
   * @return the computed score
   */
  float symmetricScore(int node1, int node2) throws IOException;

  /**
   * Creates a default scorer for float vectors.
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to score vectors
   * @param query the actual query
   */
  static RandomVectorScorer createFloats(
      final RandomAccessVectorValues<float[]> vectors,
      final VectorSimilarityFunction similarityFunction,
      final float[] query)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    final RandomAccessVectorValues<float[]> vectorsCopy = vectors.copy();
    return new RandomVectorScorer() {
      @Override
      public float symmetricScore(int node1, int node2) throws IOException {
        return similarityFunction.compare(
            vectors.vectorValue(node1), vectorsCopy.vectorValue(node2));
      }

      @Override
      public float queryScore(int node) throws IOException {
        return similarityFunction.compare(query, vectorsCopy.vectorValue(node));
      }
    };
  }

  /**
   * Creates a default scorer for byte vectors.
   *
   * @param vectors the underlying storage for vectors
   * @param similarityFunction the similarity function to use to score vectors
   * @param query the actual query
   */
  static RandomVectorScorer createBytes(
      final RandomAccessVectorValues<byte[]> vectors,
      final VectorSimilarityFunction similarityFunction,
      final byte[] query)
      throws IOException {
    if (query.length != vectors.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + query.length
              + " differs from field dimension: "
              + vectors.dimension());
    }
    final RandomAccessVectorValues<byte[]> vectorsCopy = vectors.copy();
    return new RandomVectorScorer() {
      @Override
      public float symmetricScore(int node1, int node2) throws IOException {
        return similarityFunction.compare(
            vectors.vectorValue(node1), vectorsCopy.vectorValue(node2));
      }

      @Override
      public float queryScore(int node) throws IOException {
        return similarityFunction.compare(query, vectorsCopy.vectorValue(node));
      }
    };
  }
}
