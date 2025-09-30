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

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * Common utility functions for computing similarity scores between two optimized scalar quantized
 * vectors.
 */
public class OptimizedScalarQuantizedVectorSimilarity {
  // Precomputed scale factors for each quantization bit count (1 to 8 bits).
  private static final float[] SCALE_LUT =
      new float[] {
        1f,
        1f / ((1 << 2) - 1),
        1f / ((1 << 3) - 1),
        1f / ((1 << 4) - 1),
        1f / ((1 << 5) - 1),
        1f / ((1 << 6) - 1),
        1f / ((1 << 7) - 1),
        1f / ((1 << 8) - 1),
      };

  private final VectorSimilarityFunction similarityFunction;
  private final int dimensions;
  private final float centroidDotProduct;
  private final float queryScale;
  private final float indexScale;

  /**
   * Create a new vector similarity computer for optimized scalar quantized vectors.
   *
   * @param similarityFunction - the similarity function to use.
   * @param dimensions - the number of dimensions in each vector.
   * @param centroidDotProduct - the dot product of the segment centroid with itself.
   * @param bits - the number of bits used for each dimension in [1,8].
   */
  public OptimizedScalarQuantizedVectorSimilarity(
      VectorSimilarityFunction similarityFunction,
      int dimensions,
      float centroidDotProduct,
      int bits) {
    this(similarityFunction, dimensions, centroidDotProduct, bits, bits);
  }

  /**
   * Create a new vector similarity computer for optimized scalar quantized vectors.
   *
   * @param similarityFunction - the similarity function to use.
   * @param dimensions - the number of dimensions in each vector.
   * @param centroidDotProduct - the dot product of the segment centroid with itself.
   * @param queryBits - the number of bits used in the query vector for each dimension in [1,8].
   * @param indexBits - the number of bits used in the query vector for each dimension in [1,8].
   */
  public OptimizedScalarQuantizedVectorSimilarity(
      VectorSimilarityFunction similarityFunction,
      int dimensions,
      float centroidDotProduct,
      int queryBits,
      int indexBits) {
    this.similarityFunction = similarityFunction;
    this.dimensions = dimensions;
    this.centroidDotProduct = centroidDotProduct;
    this.queryScale = SCALE_LUT[queryBits - 1];
    this.indexScale = SCALE_LUT[indexBits - 1];
  }

  /**
   * Computes the similarity score between a 'query' and an 'index' quantized vector, given the dot
   * product of the two vectors and their corrective factors.
   *
   * @param dotProduct - dot product of the two quantized vectors.
   * @param queryCorrections - corrective factors for vector 'y'.
   * @param indexCorrections - corrective factors for vector 'x'.
   * @return - a similarity score value between 0 and 1; higher values are better.
   */
  public float score(
      float dotProduct,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections,
      OptimizedScalarQuantizer.QuantizationResult indexCorrections) {
    float x1 = indexCorrections.quantizedComponentSum();
    float ax = indexCorrections.lowerInterval();
    // Here we must scale according to the bits
    float lx = (indexCorrections.upperInterval() - ax) * indexScale;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * queryScale;
    float y1 = queryCorrections.quantizedComponentSum();
    float score = ax * ay * dimensions + ay * lx * x1 + ax * ly * y1 + lx * ly * dotProduct;
    // For euclidean, we need to invert the score and apply the additional
    // correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    if (similarityFunction == EUCLIDEAN) {
      score =
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - 2 * score;
      return Math.max(1 / (1f + score), 0);
    } else {
      // For cosine and max inner product, we need to apply the additional correction,
      // which is
      // assumed to be the non-centered dot-product between the vector and the
      // centroid
      score +=
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - centroidDotProduct;
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      return Math.max((1f + score) / 2f, 0);
    }
  }

  // XXX DO NOT MERGE duplication with above.
  /**
   * Computes the similarity score between a 'query' and an 'index' quantized vector, given the dot
   * product of the two vectors and their corrective factors.
   *
   * @param dotProduct - dot product of the two quantized vectors.
   * @param queryCorrections - corrective factors for vector 'y'.
   * @param indexLowerInterval - corrective factors for vector 'x'.
   * @param indexUpperInterval - corrective factors for vector 'x'.
   * @param indexAdditionalCorrection - corrective factors for vector 'x'.
   * @param indexQuantizedComponentSum - corrective factors for vector 'x'.
   * @return - a similarity score value between 0 and 1; higher values are better.
   */
  public float score(
      float dotProduct,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections,
      float indexLowerInterval,
      float indexUpperInterval,
      float indexAdditionalCorrection,
      int indexQuantizedComponentSum) {
    float x1 = indexQuantizedComponentSum;
    float ax = indexLowerInterval;
    // Here we must scale according to the bits
    float lx = (indexUpperInterval - ax) * indexScale;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * queryScale;
    float y1 = queryCorrections.quantizedComponentSum();
    float score = ax * ay * dimensions + ay * lx * x1 + ax * ly * y1 + lx * ly * dotProduct;
    // For euclidean, we need to invert the score and apply the additional
    // correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    if (similarityFunction == EUCLIDEAN) {
      score = queryCorrections.additionalCorrection() + indexAdditionalCorrection - 2 * score;
      return Math.max(1 / (1f + score), 0);
    } else {
      // For cosine and max inner product, we need to apply the additional correction,
      // which is
      // assumed to be the non-centered dot-product between the vector and the
      // centroid
      score +=
          queryCorrections.additionalCorrection() + indexAdditionalCorrection - centroidDotProduct;
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      return Math.max((1f + score) / 2f, 0);
    }
  }
}
