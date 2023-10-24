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
package org.apache.lucene.util;

import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Calculates and adjust the scores correctly for quantized vectors given the scalar quantization
 * parameters
 */
public interface ScalarQuantizedVectorSimilarity {

  /**
   * Creates a {@link ScalarQuantizedVectorSimilarity} from a {@link VectorSimilarityFunction} and
   * the constant multiplier used for quantization.
   *
   * @param sim similarity function
   * @param constMultiplier constant multiplier used for quantization
   * @return a {@link ScalarQuantizedVectorSimilarity} that applies the appropriate corrections
   */
  static ScalarQuantizedVectorSimilarity fromVectorSimilarity(
      VectorSimilarityFunction sim, float constMultiplier) {
    switch (sim) {
      case EUCLIDEAN:
        return new Euclidean(constMultiplier);
      case COSINE:
      case DOT_PRODUCT:
        return new DotProduct(constMultiplier);
      case MAXIMUM_INNER_PRODUCT:
        return new MaximumInnerProduct(constMultiplier);
      default:
        throw new IllegalArgumentException("Unsupported similarity function: " + sim);
    }
  }

  float score(byte[] queryVector, float queryVectorOffset, byte[] storedVector, float vectorOffset);

  /** Calculates euclidean distance on quantized vectors, applying the appropriate corrections */
  class Euclidean implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;

    public Euclidean(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(
        byte[] queryVector, float queryVectorOffset, byte[] storedVector, float vectorOffset) {
      int squareDistance = VectorUtil.squareDistance(storedVector, queryVector);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  class DotProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;

    public DotProduct(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(
        byte[] queryVector, float queryOffset, byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return (1 + adjustedDistance) / 2;
    }
  }

  /** Calculates max inner product on quantized vectors, applying the appropriate corrections */
  class MaximumInnerProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;

    public MaximumInnerProduct(float constMultiplier) {
      this.constMultiplier = constMultiplier;
    }

    @Override
    public float score(
        byte[] queryVector, float queryOffset, byte[] storedVector, float vectorOffset) {
      int dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }
}
