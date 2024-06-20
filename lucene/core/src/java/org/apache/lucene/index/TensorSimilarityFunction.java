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

import org.apache.lucene.util.ArrayUtil;

import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.util.VectorUtil.*;

/**
 * Tensor similarity function; used in search to return top K most similar vectors to a target
 * tensor. This is a label describing the method used during indexing and searching of the tensors
 * in order to determine the nearest neighbors.
 */
public enum TensorSimilarityFunction {

  /**
   * SumMax Euclidean distance - returns sum of max pair wise {@link VectorSimilarityFunction#EUCLIDEAN}
   * distance across vectors in the two tensors.
   */
  SUM_MAX_EUCLIDEAN {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.EUCLIDEAN);
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      throw new UnsupportedOperationException("not implemented for tensors");
//    }
  },

  /**
   * SumMax Dot product. NOTE: this similarity is intended as an optimized way to perform cosine
   * similarity. In order to use it, all vectors must be normalized, including both document and
   * query vectors. Using dot product with vectors that are not normalized can result in errors or
   * poor search results. Floating point vectors must be normalized to be of unit length, while byte
   * vectors should simply all have the same norm.
   */
  SUM_MAX_DOT_PRODUCT {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.DOT_PRODUCT);
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      throw new UnsupportedOperationException("not implemented for tensors");
//    }
  },

  /**
   * SumMax Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
   * vectors to unit length, and instead use {@link TensorSimilarityFunction#SUM_MAX_DOT_PRODUCT}. You
   * should only use this function if you need to preserve the original vectors and cannot normalize
   * them in advance. The cosine similarity score per vector is normalised to assure it is positive.
   */
  SUM_MAX_COSINE {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.COSINE);
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      return (1 + cosine(v1, v2)) / 2;
//    }
  },

  /**
   * Sum Maximum inner product. This is like {@link TensorSimilarityFunction#SUM_MAX_DOT_PRODUCT}, but does not
   * require normalization of the inputs. Should be used when the embedding vectors store useful
   * information within the vector magnitude
   */
  SUM_MAXIMUM_INNER_PRODUCT {
    @Override
    public float compare(List<float[]> t1, List<float[]> t2) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

//    @Override
//    public float compare(byte[] v1, byte[] v2) {
//      return scaleMaxInnerProductScore(dotProduct(v1, v2));
//    }
  };

  /**
   * Calculates a similarity score between the two tensors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors
   * @param t2 another tensor, vectors of the same dimension as t1.
   * @return the value of the similarity function applied to the two tensors
   */
  public abstract float compare(List<float[]> t1, List<float[]> t2);

  /** Helper function that works with packed tensor floats */
  public float compare(float[] t1, float[] t2, int dimension) {
    if (t1.length % dimension != 0 || t2.length % dimension != 0) {
      throw new IllegalArgumentException("Tensor vectors do not match provided dimensions");
    }
    List<float[]> a = new ArrayList<>();
    List<float[]> b = new ArrayList<>();
    for (int i = 0; i <= t1.length; i += dimension) {
      a.add(ArrayUtil.copyOfSubArray(t1, i, dimension));
    }
    for (int i = 0; i <= t2.length; i += dimension) {
      b.add(ArrayUtil.copyOfSubArray(t2, i, dimension));
    }
    return compare(a, b);
  }

//    /**
//   * Calculates a similarity score between the two vectors with a specified function. Higher
//   * similarity scores correspond to closer vectors. Each (signed) byte represents a vector
//   * dimension.
//   *
//   * @param v1 a vector
//   * @param v2 another vector, of the same dimension
//   * @return the value of the similarity function applied to the two vectors
//   */
//  public abstract float compare(byte[] v1, byte[] v2);

  /**
   * Compute SumMaxSimilarity between two tensors.
   * Returns the sum of maximum similarity found for each vector in the outer tensor against all vectors
   * in the inner tensor. Uses {@param vectorSimilarityFunction} to compute similarity between two vectors.
   *
   * @param outer Outer tensor
   * @param inner Inner tensor
   * @param vectorSimilarityFunction Function to compute vector similarity
   * @return The sum of max similarity between outer - inner vector tensors
   */
  public float sumMaxSimilarity(List<float[]> outer, List<float[]> inner, VectorSimilarityFunction vectorSimilarityFunction) {
    float result = 0f;
    for (float[] o: outer) {
      float maxSim = Float.MIN_VALUE;
      for (float[] i: inner) {
        maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
      }
      result += maxSim;
    }
    return result;
  }
}
