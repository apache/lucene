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

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;

/** Vector scorer over binarized vector values */
public class Lucene102RWBinaryFlatVectorsScorer extends Lucene102BinaryFlatVectorsScorer {

  /**
   * @param nonQuantizedDelegate the delegate scorer for non-quantized vectors
   */
  public Lucene102RWBinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    super(nonQuantizedDelegate);
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof BinarizedByteVectorValues) {
      throw new UnsupportedOperationException(
          "getRandomVectorScorerSupplier(VectorSimilarityFunction,RandomAccessVectorValues) not implemented for binarized format");
    }
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
      BinarizedByteVectorValues targetVectors) {
    return new BinarizedRandomVectorScorerSupplier(
        scoringVectors, targetVectors, similarityFunction);
  }

  @Override
  public String toString() {
    return "Lucene102BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

  /** Vector scorer supplier over binarized vector values */
  static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues
        queryVectors;
    private final BinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    BinarizedRandomVectorScorerSupplier(
        Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
        BinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction) {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      final BinarizedByteVectorValues targetVectors = this.targetVectors.copy();
      final Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors =
          this.queryVectors.copy();
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(targetVectors) {
        private QuantizationResult queryCorrections = null;
        private byte[] vector = null;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          queryCorrections = queryVectors.getCorrectiveTerms(node);
          vector = queryVectors.vectorValue(node);
        }

        @Override
        public float score(int node) throws IOException {
          if (vector == null || queryCorrections == null) {
            throw new IllegalStateException("setScoringOrdinal was not called");
          }
          return quantizedScore(vector, queryCorrections, targetVectors, node, similarityFunction);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BinarizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarityFunction);
    }
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
