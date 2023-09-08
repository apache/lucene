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
  static float scoreCorrectiveOffset(
      VectorSimilarityFunction sim, byte[] quantizedVector, float alpha, float scalarOffset) {
    if (sim == VectorSimilarityFunction.EUCLIDEAN) {
      return 0f;
    }
    int sum = 0;
    for (byte b : quantizedVector) {
      sum += b;
    }
    return sum * alpha * scalarOffset;
  }

  static ScalarQuantizedVectorSimilarity fromVectorSimilarity(
      VectorSimilarityFunction sim, float constMultiplier) {
    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(constMultiplier);
      case COSINE, DOT_PRODUCT -> new DotProduct(constMultiplier);
      case MAXIMUM_INNER_PRODUCT -> new MaximumInnerProduct(constMultiplier);
    };
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
