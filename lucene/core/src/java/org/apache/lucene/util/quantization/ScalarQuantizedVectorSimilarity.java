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
package org.apache.lucene.util.quantization;

import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

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
   * @param values the quantized byte vector values
   * @return a {@link ScalarQuantizedVectorSimilarity} that applies the appropriate corrections
   */
  static ScalarQuantizedVectorSimilarity fromVectorSimilarity(
      VectorSimilarityFunction sim,
      float constMultiplier,
      RandomAccessQuantizedByteVectorValues values) {
    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(constMultiplier, values);
      case COSINE, DOT_PRODUCT -> new DotProduct(constMultiplier, values);
      case MAXIMUM_INNER_PRODUCT -> new MaximumInnerProduct(constMultiplier, values);
    };
  }

  float score(byte[] queryVector, float queryVectorOffset, int vectorOrdinal) throws IOException;

  /** Calculates euclidean distance on quantized vectors, applying the appropriate corrections */
  class Euclidean implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;

    public Euclidean(float constMultiplier, RandomAccessQuantizedByteVectorValues values) {
      this.constMultiplier = constMultiplier;
      this.values = values;
    }

    @Override
    public float score(byte[] queryVector, float queryVectorOffset, int vectorOrdinal)
        throws IOException {
      byte[] storedVector = values.vectorValue(vectorOrdinal);
      int squareDistance = VectorUtil.squareDistance(storedVector, queryVector);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  class DotProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;

    public DotProduct(float constMultiplier, RandomAccessQuantizedByteVectorValues values) {
      this.constMultiplier = constMultiplier;
      this.values = values;
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, int vectorOrdinal)
        throws IOException {
      float adjustedDistance =
          optimizedDotProduct(queryVector, queryOffset, vectorOrdinal, values, constMultiplier);
      return (1 + adjustedDistance) / 2;
    }
  }

  /** Calculates max inner product on quantized vectors, applying the appropriate corrections */
  class MaximumInnerProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;

    public MaximumInnerProduct(
        float constMultiplier, RandomAccessQuantizedByteVectorValues values) {
      this.constMultiplier = constMultiplier;
      this.values = values;
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, int vectorOrdinal)
        throws IOException {
      float adjustedDistance =
          optimizedDotProduct(queryVector, queryOffset, vectorOrdinal, values, constMultiplier);
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }

  private static float optimizedDotProduct(
      byte[] queryVector,
      float queryOffset,
      int vectorOrdinal,
      RandomAccessQuantizedByteVectorValues values,
      float constMultiplier)
      throws IOException {
    final byte[] storedVector = values.vectorValue(vectorOrdinal);
    final float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
    final int dotProduct =
        values.getScalarQuantizer().getBits() <= 4
            ?
            // For 4-bit quantization, we use a specialized dot product implementation
            VectorUtil.int4DotProduct(storedVector, queryVector)
            : VectorUtil.dotProduct(storedVector, queryVector);
    return dotProduct * constMultiplier + queryOffset + vectorOffset;
  }
}
