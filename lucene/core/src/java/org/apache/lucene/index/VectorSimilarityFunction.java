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

import static org.apache.lucene.util.VectorUtil.cosine;
import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.dotProductScore;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;
import static org.apache.lucene.util.VectorUtil.squareDistance;

/**
 * Vector similarity function; used in search to return top K most similar vectors to a target
 * vector. This is a label describing the method used during indexing and searching of the vectors
 * in order to determine the nearest neighbors.
 */
public enum VectorSimilarityFunction {

  /** Euclidean distance */
  EUCLIDEAN {
    @Override
    public float compare(float[] v1, float[] v2) {
      return 1 / (1 + squareDistance(v1, v2));
    }

    @Override
    public float compare(byte[] v1, byte[] v2) {
      return 1 / (1f + squareDistance(v1, v2));
    }
  },

  /**
   * Dot product. NOTE: this similarity is intended as an optimized way to perform cosine
   * similarity. In order to use it, all vectors must be normalized, including both document and
   * query vectors. Using dot product with vectors that are not normalized can result in errors or
   * poor search results. Floating point vectors must be normalized to be of unit length, while byte
   * vectors should simply all have the same norm.
   */
  DOT_PRODUCT {
    @Override
    public float compare(float[] v1, float[] v2) {
      return Math.max((1 + dotProduct(v1, v2)) / 2, 0);
    }

    @Override
    public float compare(byte[] v1, byte[] v2) {
      return dotProductScore(v1, v2);
    }
  },

  /**
   * Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
   * vectors to unit length, and instead use {@link VectorSimilarityFunction#DOT_PRODUCT}. You
   * should only use this function if you need to preserve the original vectors and cannot normalize
   * them in advance. The similarity score is normalised to assure it is positive.
   */
  COSINE {
    @Override
    public float compare(float[] v1, float[] v2) {
      return Math.max((1 + cosine(v1, v2)) / 2, 0);
    }

    @Override
    public float compare(byte[] v1, byte[] v2) {
      return (1 + cosine(v1, v2)) / 2;
    }
  },

  /**
   * Maximum inner product. This is like {@link VectorSimilarityFunction#DOT_PRODUCT}, but does not
   * require normalization of the inputs. Should be used when the embedding vectors store useful
   * information within the vector magnitude
   */
  MAXIMUM_INNER_PRODUCT {
    @Override
    public float compare(float[] v1, float[] v2) {
      return scaleMaxInnerProductScore(dotProduct(v1, v2));
    }

    @Override
    public float compare(byte[] v1, byte[] v2) {
      return scaleMaxInnerProductScore(dotProduct(v1, v2));
    }
  };

  /**
   * Calculates a similarity score between the two vectors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param v1 a vector
   * @param v2 another vector, of the same dimension
   * @return the value of the similarity function applied to the two vectors
   */
  public abstract float compare(float[] v1, float[] v2);

  /**
   * Calculates a similarity score between the two vectors with a specified function. Higher
   * similarity scores correspond to closer vectors. Each (signed) byte represents a vector
   * dimension.
   *
   * @param v1 a vector
   * @param v2 another vector, of the same dimension
   * @return the value of the similarity function applied to the two vectors
   */
  public abstract float compare(byte[] v1, byte[] v2);
}
