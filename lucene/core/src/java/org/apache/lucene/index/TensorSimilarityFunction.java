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
    public float compare(float[] t1, float[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.EUCLIDEAN, dimension);
    }

    @Override
    public float compare(byte[] t1, byte[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.EUCLIDEAN, dimension);
    }
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
    public float compare(float[] t1, float[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.DOT_PRODUCT, dimension);
    }

    public float compare(byte[] t1, byte[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.DOT_PRODUCT, dimension);
    }
  },

  /**
   * SumMax Cosine similarity. NOTE: the preferred way to perform cosine similarity is to normalize all
   * vectors to unit length, and instead use {@link TensorSimilarityFunction#SUM_MAX_DOT_PRODUCT}. You
   * should only use this function if you need to preserve the original vectors and cannot normalize
   * them in advance. The cosine similarity score per vector is normalised to assure it is positive.
   */
  SUM_MAX_COSINE {
    @Override
    public float compare(float[] t1, float[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.COSINE, dimension);
    }

    @Override
    public float compare(byte[] t1, byte[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.COSINE, dimension);
    }
  },

  /**
   * Sum Maximum inner product. This is like {@link TensorSimilarityFunction#SUM_MAX_DOT_PRODUCT}, but does not
   * require normalization of the inputs. Should be used when the embedding vectors store useful
   * information within the vector magnitude
   */
  SUM_MAXIMUM_INNER_PRODUCT {
    @Override
    public float compare(float[] t1, float[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, dimension);
    }

    @Override
    public float compare(byte[] t1, byte[] t2, int dimension) {
      return sumMaxSimilarity(t1, t2, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT, dimension);
    }
  };

  // TODO: this is trappy, it has an unchecked requirement for VectorSimilarityFunction to have ordinal < 1000
  // can we merge the two similarity functions?
  /** Uses identifier instead of ordinals to avoid overlap with {@link VectorSimilarityFunction} */
  private final int identifier;

  public static final int ORDINAL_START = 1000;

  TensorSimilarityFunction() {
    this.identifier = this.ordinal() + ORDINAL_START;
  }

  public int identifier() {
    return identifier;
  }

  /**
   * Calculates a similarity score between the two tensors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors
   *           All vector values are concatenated in a single packed array.
   * @param t2 another tensor, vectors of the same dimension as t1.
   *           All vector values are concatenated in a single packed array.
   * @return the value of the similarity function applied to the two tensors
   */
  public abstract float compare(float[] t1, float[] t2, int dimension);

  /**
   * Calculates a similarity score between the two tensors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a tensor with non-empty vectors.
   *           All vector values are concatenated in a single packed array.
   * @param t2 another tensor, vectors of the same dimension as t1.
   *           All vector values are concatenated in a single packed array.
   * @return the value of the similarity function applied to the two tensors
   */
  public abstract float compare(byte[] t1, byte[] t2, int dimension);

  /**
   * Compute SumMaxSimilarity between two tensors.
   * Returns the sum of maximum similarity found for each vector in the outer tensor against all vectors
   * in the inner tensor. Uses {@param vectorSimilarityFunction} to compute similarity between two vectors.
   *
   * @param outerTensor Outer tensor
   * @param innerTensor Inner tensor
   * @param vectorSimilarityFunction Function to compute vector similarity
   * @param dimension Dimension for each vector in the tensors
   * @return The sum of max similarity between outer - inner vector tensors
   */
  public float sumMaxSimilarity(float[] outerTensor,
                                float[] innerTensor,
                                VectorSimilarityFunction vectorSimilarityFunction,
                                int dimension) {
    if (outerTensor.length % dimension != 0 || innerTensor.length % dimension != 0) {
      throw new IllegalArgumentException("Tensor vectors do not match provided dimensions");
    }
    List<float[]> outer = new ArrayList<>();
    List<float[]> inner = new ArrayList<>();
    for (int i = 0; i <= outerTensor.length; i += dimension) {
      outer.add(ArrayUtil.copyOfSubArray(outerTensor, i, dimension));
    }
    for (int i = 0; i <= innerTensor.length; i += dimension) {
      inner.add(ArrayUtil.copyOfSubArray(innerTensor, i, dimension));
    }

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

  /** SumMaxSimilarity for byte tensors */
  public float sumMaxSimilarity(byte[] outerTensor,
                                byte[] innerTensor,
                                VectorSimilarityFunction vectorSimilarityFunction,
                                int dimension) {
    if (outerTensor.length % dimension != 0 || innerTensor.length % dimension != 0) {
      throw new IllegalArgumentException("Tensor vectors do not match provided dimensions");
    }
    List<byte[]> outer = new ArrayList<>();
    List<byte[]> inner = new ArrayList<>();
    for (int i = 0; i <= outerTensor.length; i += dimension) {
      outer.add(ArrayUtil.copyOfSubArray(outerTensor, i, dimension));
    }
    for (int i = 0; i <= innerTensor.length; i += dimension) {
      inner.add(ArrayUtil.copyOfSubArray(innerTensor, i, dimension));
    }

    float result = 0f;
    for (byte[] o: outer) {
      float maxSim = Float.MIN_VALUE;
      for (byte[] i: inner) {
        maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
      }
      result += maxSim;
    }
    return result;
  }
}
