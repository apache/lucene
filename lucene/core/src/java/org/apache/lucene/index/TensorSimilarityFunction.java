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

import java.util.List;

import static org.apache.lucene.util.VectorUtil.*;

/**
 * Tensor similarity function; used in search to return top K most similar vectors to a target
 * tensor. This is a label describing the method used during indexing and searching of the tensors
 * in order to determine the nearest neighbors.
 */
public enum TensorSimilarityFunction {

  /**
   * Max Euclidean distance - returns max of pair wise {@link VectorSimilarityFunction#EUCLIDEAN}
   * distance across corresponding vectors in the two tensors.
   */
  MAX_EUCLIDEAN {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      if (t1.size() != t2.size()) {
        throw new IllegalArgumentException("tensor degrees differ: " + t1.size() + "!=" + t2.size());
      }
      float sim = Float.MIN_VALUE;
      for (int i = 0; i < t1.size(); i++) {
        sim = Math.max(sim, VectorSimilarityFunction.EUCLIDEAN.compare(t1.get(i), t2.get(i)));
      }
      return sim;
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      throw new UnsupportedOperationException("not implemented for tensors");
//    }
  },

  /**
   * Max Dot product. NOTE: this similarity is intended as an optimized way to perform cosine
   * similarity. In order to use it, all vectors must be normalized, including both document and
   * query vectors. Using dot product with vectors that are not normalized can result in errors or
   * poor search results. Floating point vectors must be normalized to be of unit length, while byte
   * vectors should simply all have the same norm.
   */
  MAX_DOT_PRODUCT {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      if (t1.size() != t2.size()) {
        throw new IllegalArgumentException("tensor degrees differ: " + t1.size() + "!=" + t2.size());
      }
      float sim = Float.MIN_VALUE;
      for (int i = 0; i < t1.size(); i++) {
        sim = Math.max(sim, VectorSimilarityFunction.DOT_PRODUCT.compare(t1.get(i), t2.get(i)));
      }
      return sim;
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      throw new UnsupportedOperationException("not implemented for tensors");
//    }
  },

  /**
   * Max Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
   * vectors to unit length, and instead use {@link TensorSimilarityFunction#MAX_DOT_PRODUCT}. You
   * should only use this function if you need to preserve the original vectors and cannot normalize
   * them in advance. The cosine similarity score per vector is normalised to assure it is positive.
   */
  MAX_COSINE {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      if (t1.size() != t2.size()) {
        throw new IllegalArgumentException("tensor degrees differ: " + t1.size() + "!=" + t2.size());
      }
      float sim = Float.MIN_VALUE;
      for (int i = 0; i < t1.size(); i++) {
        sim = Math.max(sim, VectorSimilarityFunction.COSINE.compare(t1.get(i), t2.get(i)));
      }
      return sim;
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      return (1 + cosine(v1, v2)) / 2;
//    }
  };

//  /**
//   * Maximum inner product. This is like {@link TensorSimilarityFunction#DOT_PRODUCT}, but does not
//   * require normalization of the inputs. Should be used when the embedding vectors store useful
//   * information within the vector magnitude
//   */
//  MAXIMUM_INNER_PRODUCT {
//    @Override
//    public float compare(float[] v1, float[] v2) {
//      return scaleMaxInnerProductScore(dotProduct(v1, v2));
//    }
//
//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      return scaleMaxInnerProductScore(dotProduct(v1, v2));
//    }
//  };

  /**
   * Calculates a similarity score between the two vectors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors
   * @param t2 another tensor, of the same degree with corresponding vectors of the same dimension.
   * @return the value of the similarity function applied to the two vectors
   */
  public abstract float compare(List<float[]> t1, List<float[]> t2);

//  /**
//   * Calculates a similarity score between the two vectors with a specified function. Higher
//   * similarity scores correspond to closer vectors. Each (signed) byte represents a vector
//   * dimension.
//   *
//   * @param v1 a vector
//   * @param v2 another vector, of the same dimension
//   * @return the value of the similarity function applied to the two vectors
//   */
//  public abstract float compare(byte[] v1, byte[] v2);
}
