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
    private final byte[] compressedVector;

    public DotProduct(float constMultiplier, RandomAccessQuantizedByteVectorValues values) {
      this.constMultiplier = constMultiplier;
      this.values = values;
      if (values.getScalarQuantizer().getBits() <= 4
          && values.getVectorByteLength() != values.dimension() + Float.BYTES
          && values.getSlice() != null) {
        this.compressedVector = new byte[values.getVectorByteLength() - Float.BYTES];
      } else {
        this.compressedVector = null;
      }
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, int vectorOrdinal)
        throws IOException {
      final int dotProduct;
      final float vectorOffset;
      if (values.getScalarQuantizer().getBits() <= 4) {
        if (values.getSlice() != null
            && values.getVectorByteLength() != queryVector.length + Float.BYTES) {
          // get compressed vector
          values.getSlice().seek((long) vectorOrdinal * values.getVectorByteLength());
          values.getSlice().readBytes(compressedVector, 0, compressedVector.length);
          vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
          dotProduct = VectorUtil.int4DotProductPacked(queryVector, compressedVector);
        } else {
          byte[] storedVector = values.vectorValue(vectorOrdinal);
          vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
          // For 4-bit quantization, we use a specialized dot product implementation
          dotProduct = VectorUtil.int4DotProduct(storedVector, queryVector);
        }
      } else {
        byte[] storedVector = values.vectorValue(vectorOrdinal);
        vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
        dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      }
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return (1 + adjustedDistance) / 2;
    }
  }

  /** Calculates max inner product on quantized vectors, applying the appropriate corrections */
  class MaximumInnerProduct implements ScalarQuantizedVectorSimilarity {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;
    private final byte[] compressedVector;

    public MaximumInnerProduct(
        float constMultiplier, RandomAccessQuantizedByteVectorValues values) {
      this.constMultiplier = constMultiplier;
      this.values = values;
      if (values.getScalarQuantizer().getBits() <= 4
          && values.getVectorByteLength() != values.dimension() + Float.BYTES
          && values.getSlice() != null) {
        this.compressedVector = new byte[values.getVectorByteLength() - Float.BYTES];
      } else {
        this.compressedVector = null;
      }
    }

    @Override
    public float score(byte[] queryVector, float queryOffset, int vectorOrdinal)
        throws IOException {
      final int dotProduct;
      final float vectorOffset;
      if (values.getScalarQuantizer().getBits() <= 4) {
        if (values.getSlice() != null
            && values.getVectorByteLength() != queryVector.length + Float.BYTES) {
          // get compressed vector
          values.getSlice().seek((long) vectorOrdinal * values.getVectorByteLength());
          values.getSlice().readBytes(compressedVector, 0, compressedVector.length);
          vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
          dotProduct = VectorUtil.int4DotProductPacked(queryVector, compressedVector);
        } else {
          byte[] storedVector = values.vectorValue(vectorOrdinal);
          vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
          // For 4-bit quantization, we use a specialized dot product implementation
          dotProduct = VectorUtil.int4DotProduct(storedVector, queryVector);
        }
      } else {
        byte[] storedVector = values.vectorValue(vectorOrdinal);
        vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
        dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
      }
      float adjustedDistance = dotProduct * constMultiplier + queryOffset + vectorOffset;
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }

  /** Compares two byte vectors */
  interface ByteVectorComparator {
    int compare(byte[] v1, byte[] v2);
  }
}
