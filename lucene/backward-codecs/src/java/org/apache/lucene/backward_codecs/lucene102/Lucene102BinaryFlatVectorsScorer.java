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
package org.apache.lucene.backward_codecs.lucene102;

import static org.apache.lucene.backward_codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;

/** Vector scorer over binarized vector values */
public class Lucene102BinaryFlatVectorsScorer implements FlatVectorsScorer {
  /** The delegate scorer for non-quantized vectors */
  protected final FlatVectorsScorer nonQuantizedDelegate;

  /** Scaling factor for 4-bit quantization */
  protected static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

  /**
   * Construct a new scorer
   *
   * @param nonQuantizedDelegate the delegate scorer for non-quantized vectors
   */
  public Lucene102BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    throw new UnsupportedOperationException("Old codecs may only be used for reading");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof BinarizedByteVectorValues binarizedVectors) {
      OptimizedScalarQuantizer quantizer = binarizedVectors.getQuantizer();
      float[] centroid = binarizedVectors.getCentroid();
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      byte[] initial = new byte[target.length];
      byte[] quantized = new byte[QUERY_BITS * binarizedVectors.discretizedDimensions() / 8];
      QuantizationResult queryCorrections =
          quantizer.scalarQuantize(target, initial, (byte) 4, centroid);
      transposeHalfByte(initial, quantized);
      return new RandomVectorScorer.AbstractRandomVectorScorer(binarizedVectors) {
        @Override
        public float score(int node) throws IOException {
          return quantizedScore(
              quantized, queryCorrections, binarizedVectors, node, similarityFunction);
        }
      };
    }
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "Lucene102BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

  static float quantizedScore(
      byte[] quantizedQuery,
      QuantizationResult queryCorrections,
      BinarizedByteVectorValues targetVectors,
      int targetOrd,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    byte[] binaryCode = targetVectors.vectorValue(targetOrd);
    float qcDist = VectorUtil.int4BitDotProduct(quantizedQuery, binaryCode);
    QuantizationResult indexCorrections = targetVectors.getCorrectiveTerms(targetOrd);
    float x1 = indexCorrections.quantizedComponentSum();
    float ax = indexCorrections.lowerInterval();
    // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
    float lx = indexCorrections.upperInterval() - ax;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
    float y1 = queryCorrections.quantizedComponentSum();
    float score =
        ax * ay * targetVectors.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
    // For euclidean, we need to invert the score and apply the additional correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    if (similarityFunction == EUCLIDEAN) {
      score =
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - 2 * score;
      return Math.max(1 / (1f + score), 0);
    } else {
      // For cosine and max inner product, we need to apply the additional correction, which is
      // assumed to be the non-centered dot-product between the vector and the centroid
      score +=
          queryCorrections.additionalCorrection()
              + indexCorrections.additionalCorrection()
              - targetVectors.getCentroidDP();
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      return Math.max((1f + score) / 2f, 0);
    }
  }
}
