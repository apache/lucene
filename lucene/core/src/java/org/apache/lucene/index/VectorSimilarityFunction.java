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
package org.apache.lucene.index;

import static org.apache.lucene.util.VectorUtil.*;

/**
 * Vector similarity function; used in search to return top K most similar vectors to a target
 * vector. This is a label describing the method used during indexing and searching of the vectors
 * in order to determine the nearest neighbors.
 */
public enum VectorSimilarityFunction {

  /** Euclidean distance */
  EUCLIDEAN(true) {
    @Override
    public float compare(float[] v1, float[] v2) {
      return squareDistance(v1, v2);
    }

    @Override
    public float convertToScore(float similarity) {
      return 1 / (1 + similarity);
    }
  },

  /**
   * Dot product. NOTE: this similarity is intended as an optimized way to perform cosine
   * similarity. In order to use it, all vectors must be of unit length, including both document and
   * query vectors. Using dot product with vectors that are not unit length can result in errors or
   * poor search results.
   */
  DOT_PRODUCT {
    @Override
    public float compare(float[] v1, float[] v2) {
      return dotProduct(v1, v2);
    }

    @Override
    public float convertToScore(float similarity) {
      return (1 + similarity) / 2;
    }
  },

  /**
   * Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
   * vectors to unit length, and instead use {@link VectorSimilarityFunction#DOT_PRODUCT}. You
   * should only use this function if you need to preserve the original vectors and cannot normalize
   * them in advance.
   */
  COSINE {
    @Override
    public float compare(float[] v1, float[] v2) {
      return cosine(v1, v2);
    }

    @Override
    public float convertToScore(float similarity) {
      return (1 + similarity) / 2;
    }
  },

  /**
   * Dot product using 8 bits of precision. NOTE: this similarity is intended as an optimized way to perform cosine
   * similarity and can enable significant storage savings, albeit with some loss of precision. In order to use it, all
   * vectors must be of the same length, and must have values in the range [-128, 127],
   * including both document and query vectors. Using nonconforming vectors can result in errors or poor search results.
   */
  DOT_PRODUCT8 {

    @Override
    public float compare(float[] v1, float[] v2) {
      return dotProduct(v1, v2);
    }

    @Override
    public float convertToScore(float similarity) {
      return (1 + similarity) / 2;
    }
  };

  /**
   * If true, the scores associated with vector comparisons are nonnegative and in reverse order;
   * that is, lower scores represent more similar vectors. Otherwise, if false, higher scores
   * represent more similar vectors, and scores may be negative or positive.
   */
  public final boolean reversed;

  VectorSimilarityFunction(boolean reversed) {
    this.reversed = reversed;
  }

  VectorSimilarityFunction() {
    reversed = false;
  }

  /**
   * Calculates a similarity score between the two vectors with a specified function.
   *
   * @param v1 a vector
   * @param v2 another vector, of the same dimension
   * @return the value of the similarity function applied to the two vectors
   */
  public abstract float compare(float[] v1, float[] v2);

  /**
   * Converts similarity scores used (may be negative, reversed, etc) into document scores, which
   * must be positive, with higher scores representing better matches.
   *
   * @param similarity the raw internal score as returned by {@link #compare(float[], float[])}.
   * @return normalizedSimilarity
   */
  public abstract float convertToScore(float similarity);
}
