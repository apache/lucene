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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.ArrayUtil;

/**
 * Tensor similarity function; used in search to return top K most similar vectors to a target
 * tensor. This method is used during indexing and searching of the tensors in order to determine
 * the nearest neighbors.
 */
// no commit
public class TensorSimilarityFunction implements TensorSimilarity {

  /** Aggregation function to combine similarity across multiple vector values */
  public enum Aggregation {
    /**
     * SumMaxSimilarity between two tensors. Aggregates using the sum of maximum similarity found
     * for each vector in the first tensor against all vectors in the second tensor.
     */
    SUM_MAX {
      @Override
      public float aggregate(
          float[] outerTensor,
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
        for (float[] o : outer) {
          float maxSim = Float.MIN_VALUE;
          for (float[] i : inner) {
            maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
          }
          result += maxSim;
        }
        return result;
      }

      @Override
      public float aggregate(
          byte[] outerTensor,
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
        for (byte[] o : outer) {
          float maxSim = Float.MIN_VALUE;
          for (byte[] i : inner) {
            maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
          }
          result += maxSim;
        }
        return result;
      }
    };

    /**
     * Computes and aggregates similarity over multiple vector values
     *
     * @param outerTensor first tensor
     * @param innerTensor second tensor
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector value in the tensor
     * @return similarity between the two tensors
     */
    public abstract float aggregate(
        float[] outerTensor,
        float[] innerTensor,
        VectorSimilarityFunction vectorSimilarityFunction,
        int dimension);

    /**
     * Computes and aggregates similarity over multiple vector values
     *
     * @param outerTensor first tensor
     * @param innerTensor second tensor
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector value in the tensor
     * @return similarity between the two tensors
     */
    public abstract float aggregate(
        byte[] outerTensor,
        byte[] innerTensor,
        VectorSimilarityFunction vectorSimilarityFunction,
        int dimension);
  }

  /** Similarity function used for tensor distance calculations */
  public final VectorSimilarityFunction similarityFunction;

  /** Aggregation function to combine similarity across multiple vector values */
  public final Aggregation aggregation;

  /**
   * Similarity function for computing distance between tensor values
   *
   * @param similarityFunction {@link VectorSimilarityFunction} for computing vector proximity
   * @param aggregation {@link Aggregation} to combine similarity across multiple vector values
   */
  public TensorSimilarityFunction(
      VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
    this.similarityFunction = similarityFunction;
    this.aggregation = aggregation;
  }

  @Override
  public float compare(float[] t1, float[] t2, int dimension) {
    return aggregation.aggregate(t1, t2, similarityFunction, dimension);
  }

  @Override
  public float compare(byte[] t1, byte[] t2, int dimension) {
    return aggregation.aggregate(t1, t2, similarityFunction, dimension);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TensorSimilarityFunction == false) {
      return false;
    }
    TensorSimilarityFunction o = (TensorSimilarityFunction) obj;
    return this.similarityFunction == o.similarityFunction && this.aggregation == o.aggregation;
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(similarityFunction.ordinal());
    result = 31 * result + Integer.hashCode(aggregation.ordinal());
    return result;
  }

  @Override
  public String toString() {
    return "TensorSimilarityFunction(similarity="
        + similarityFunction
        + ", aggregation="
        + aggregation
        + ")";
  }
}
