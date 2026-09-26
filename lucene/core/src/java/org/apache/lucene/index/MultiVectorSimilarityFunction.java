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
 * Computes similarity between two multi-vectors.
 *
 * <p>A multi-vector is a collection of multiple vectors that represent a single document or query.
 * MultiVectorSimilarityFunction is used to determine nearest neighbors during indexing and search
 * on multi-vectors.
 */
public class MultiVectorSimilarityFunction {

  /** Aggregation function to combine similarity across multiple vector values */
  public enum Aggregation {

    /**
     * Sum_Max Similarity between two multi-vectors. Computes the sum of maximum similarity found
     * for each vector in the first multi-vector against all vectors in the second multi-vector.
     */
    SUM_MAX {
      @Override
      public float aggregate(
          float[] outer,
          float[] inner,
          VectorSimilarityFunction vectorSimilarityFunction,
          int dimension) {
        if (outer.length % dimension != 0 || inner.length % dimension != 0) {
          throw new IllegalArgumentException("Multi vectors do not match provided dimension value");
        }

        // TODO: can we avoid making vector copies?
        List<float[]> outerList = new ArrayList<>();
        List<float[]> innerList = new ArrayList<>();
        for (int i = 0; i < outer.length; i += dimension) {
          outerList.add(ArrayUtil.copyOfSubArray(outer, i, i + dimension));
        }
        for (int i = 0; i < inner.length; i += dimension) {
          innerList.add(ArrayUtil.copyOfSubArray(inner, i, i + dimension));
        }

        float result = 0f;
        for (float[] o : outerList) {
          float maxSim = Float.MIN_VALUE;
          for (float[] i : innerList) {
            maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
          }
          result += maxSim;
        }
        return result;
      }

      @Override
      public float aggregate(
          byte[] outer,
          byte[] inner,
          VectorSimilarityFunction vectorSimilarityFunction,
          int dimension) {
        if (outer.length % dimension != 0 || inner.length % dimension != 0) {
          throw new IllegalArgumentException("Multi vectors do not match provided dimension value");
        }

        // TODO: can we avoid making vector copies?
        List<byte[]> outerList = new ArrayList<>();
        List<byte[]> innerList = new ArrayList<>();
        for (int i = 0; i < outer.length; i += dimension) {
          outerList.add(ArrayUtil.copyOfSubArray(outer, i, i + dimension));
        }
        for (int i = 0; i < inner.length; i += dimension) {
          innerList.add(ArrayUtil.copyOfSubArray(inner, i, i + dimension));
        }

        float result = 0f;
        for (byte[] o : outerList) {
          float maxSim = Float.MIN_VALUE;
          for (byte[] i : innerList) {
            maxSim = Float.max(maxSim, vectorSimilarityFunction.compare(o, i));
          }
          result += maxSim;
        }
        return result;
      }
    };

    /**
     * Computes and aggregates similarity over multiple vector values.
     *
     * <p>Assumes all vector values in both provided multi-vectors have the same dimension. Slices
     * inner and outer float[] multi-vectors into dimension sized vector values for comparison.
     *
     * @param outer first multi-vector
     * @param inner second multi-vector
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector in the provided multi-vectors
     * @return similarity between the two multi-vectors
     */
    public abstract float aggregate(
        float[] outer,
        float[] inner,
        VectorSimilarityFunction vectorSimilarityFunction,
        int dimension);

    /**
     * Computes and aggregates similarity over multiple vector values.
     *
     * <p>Assumes all vector values in both provided multi-vectors have the same dimension. Slices
     * inner and outer byte[] multi-vectors into dimension sized vector values for comparison.
     *
     * @param outer first multi-vector
     * @param inner second multi-vector
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector in the provided multi-vectors
     * @return similarity between the two multi-vectors
     */
    public abstract float aggregate(
        byte[] outer,
        byte[] inner,
        VectorSimilarityFunction vectorSimilarityFunction,
        int dimension);
  }

  /** Similarity function used for multi-vector distance calculations */
  public final VectorSimilarityFunction similarityFunction;

  /** Aggregation function to combine similarity across multiple vector values */
  public final Aggregation aggregation;

  /**
   * Similarity function for computing distance between multi-vector values
   *
   * @param similarityFunction {@link VectorSimilarityFunction} for computing vector proximity
   * @param aggregation {@link Aggregation} to combine similarity across multiple vector values
   */
  public MultiVectorSimilarityFunction(
      VectorSimilarityFunction similarityFunction, Aggregation aggregation) {
    this.similarityFunction = similarityFunction;
    this.aggregation = aggregation;
  }

  /**
   * Compute similarity between two float multi-vectors.
   *
   * <p>Expects all component vector values as a single packed float[] for each multi-vector. Uses
   * configured aggregation function and vector similarity.
   */
  public float compare(float[] t1, float[] t2, int dimension) {
    return aggregation.aggregate(t1, t2, similarityFunction, dimension);
  }

  /**
   * Compute similarity between two byte multi-vectors.
   *
   * <p>Expects all component vector values as a single packed float[] for each multi-vector. Uses
   * configured aggregation function and vector similarity.
   */
  public float compare(byte[] t1, byte[] t2, int dimension) {
    return aggregation.aggregate(t1, t2, similarityFunction, dimension);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MultiVectorSimilarityFunction == false) {
      return false;
    }
    MultiVectorSimilarityFunction o = (MultiVectorSimilarityFunction) obj;
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
    return "MultiVectorSimilarityFunction(similarity="
        + similarityFunction
        + ", aggregation="
        + aggregation
        + ")";
  }
}
