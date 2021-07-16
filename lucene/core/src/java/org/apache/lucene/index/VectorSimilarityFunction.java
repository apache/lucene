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

import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.squareDistance;

import org.apache.lucene.codecs.VectorReader;

/**
 * Vector similarity function; used in search to return top K most similar vectors to a target
 * vector. This is a label describing the method used during indexing and searching of the vectors
 * in order to determine the nearest neighbors.
 */
public enum VectorSimilarityFunction {

  /**
   * No similarity function is provided. Note: {@link VectorReader#search(String, float[], int,
   * int)} is not supported for fields specifying this.
   */
  NONE,

  /** HNSW graph built using Euclidean distance */
  EUCLIDEAN(true),

  /** HNSW graph buit using dot product */
  DOT_PRODUCT;

  /**
   * If true, the scores associated with vector comparisons are in reverse order; that is, lower
   * scores represent more similar vectors. Otherwise, if false, higher scores represent more
   * similar vectors.
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
  public float compare(float[] v1, float[] v2) {
    switch (this) {
      case EUCLIDEAN:
        return squareDistance(v1, v2);
      case DOT_PRODUCT:
        return dotProduct(v1, v2);
      case NONE:
      default:
        throw new IllegalStateException("Incomparable similarity function: " + this);
    }
  }
}
