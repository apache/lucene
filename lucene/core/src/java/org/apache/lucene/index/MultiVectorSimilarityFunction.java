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
 * Multi-vector similarity function; used in search to return top K most similar multi-vectors to a
 * target multi-vector. This method is used during indexing and searching of the multi-vectors in
 * order to determine the nearest neighbors.
 */
// no commit
public class MultiVectorSimilarityFunction implements MultiVectorSimilarity {

  /** Aggregation function to combine similarity across multiple vector values */
  public enum Aggregation {
    /** Placeholder aggregation that is not intended to be used. */
    NONE {
      @Override
      public float aggregate(
          float[] outer,
          float[] inner,
          VectorSimilarityFunction vectorSimilarityFunction,
          int dimension) {
        throw new UnsupportedOperationException();
      }

      @Override
      public float aggregate(
          byte[] outer,
          byte[] inner,
          VectorSimilarityFunction vectorSimilarityFunction,
          int dimension) {
        throw new UnsupportedOperationException();
      }
    },

    /**
     * SumMaxSimilarity between two multi-vectors. Aggregates using the sum of maximum similarity
     * found for each vector in the first multi-vector against all vectors in the second
     * multi-vector.
     */
    SUM_MAX {
      @Override
      public float aggregate(
          float[] outer,
          float[] inner,
          VectorSimilarityFunction vectorSimilarityFunction,
          int dimension) {
        if (outer.length % dimension != 0 || inner.length % dimension != 0) {
          throw new IllegalArgumentException("Multi vectors do not match provided dimensions");
        }
        // TODO: can we avoid making vector copies?
        List<float[]> outerList = new ArrayList<>();
        List<float[]> innerList = new ArrayList<>();
        for (int i = 0; i < outer.length; i += dimension) {
//          System.out.println("copy subArray - " + i + ":" + i+dimension);
          outerList.add(ArrayUtil.copyOfSubArray(outer, i, i + dimension));
        }
        for (int i = 0; i < inner.length; i += dimension) {
//          System.out.println("copy subArray - " + i + ":" + i+dimension);
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
          throw new IllegalArgumentException("Multi vectors do not match provided dimensions");
        }
        List<byte[]> outerList = new ArrayList<>();
        List<byte[]> innerList = new ArrayList<>();
//        System.out.println("...handling outer list");
        for (int i = 0; i < outer.length; i += dimension) {
//          System.out.println("copy subArray - " + i + ":" + dimension);
          outerList.add(ArrayUtil.copyOfSubArray(outer, i, i + dimension));
        }
//        System.out.println("...handling inner list");
        for (int i = 0; i < inner.length; i += dimension) {
//          System.out.println("copy subArray - " + i + ":" + dimension);
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
     * Computes and aggregates similarity over multiple vector values
     *
     * @param outer first multi-vector
     * @param inner second multi-vector
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector value in the multi-vector
     * @return similarity between the two multi-vectors
     */
    public abstract float aggregate(
        float[] outer,
        float[] inner,
        VectorSimilarityFunction vectorSimilarityFunction,
        int dimension);

    /**
     * Computes and aggregates similarity over multiple vector values
     *
     * @param outer first multi-vector
     * @param inner second multi-vector
     * @param vectorSimilarityFunction distance function for vector proximity
     * @param dimension dimension for each vector value in the multi-vector
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
