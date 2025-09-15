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
package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Vector scorer over OptimizedScalarQuantized vectors */
public class Lucene104ScalarQuantizedVectorScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene104ScalarQuantizedVectorScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      return new ScalarQuantizedVectorScorerSupplier(qv, similarityFunction);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      OptimizedScalarQuantizer quantizer = qv.getQuantizer();
      byte[] targetQuantized =
          new byte
              [OptimizedScalarQuantizer.discretize(
                  target.length, qv.getScalarEncoding().getDimensionsPerByte())];
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      var targetCorrectiveTerms =
          quantizer.scalarQuantize(
              target, targetQuantized, qv.getScalarEncoding().getBits(), qv.getCentroid());
      return new RandomVectorScorer.AbstractRandomVectorScorer(qv) {
        @Override
        public float score(int node) throws IOException {
          return quantizedScore(
              targetQuantized, targetCorrectiveTerms, qv, node, similarityFunction);
        }
      };
    }
    // It is possible to get to this branch during initial indexing and flush
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
    return "Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate="
        + nonQuantizedDelegate
        + ")";
  }

  private static final class ScalarQuantizedVectorScorerSupplier
      implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues targetValues;
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityFunction similarity;

    public ScalarQuantizedVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction similarity) throws IOException {
      this.targetValues = values.copy();
      this.values = values;
      this.similarity = similarity;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private byte[] targetVector;
        private OptimizedScalarQuantizer.QuantizationResult targetCorrectiveTerms;

        @Override
        public float score(int node) throws IOException {
          return quantizedScore(targetVector, targetCorrectiveTerms, values, node, similarity);
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          var rawTargetVector = targetValues.vectorValue(node);
          switch (values.getScalarEncoding()) {
            case UNSIGNED_BYTE -> targetVector = rawTargetVector;
            case PACKED_NIBBLE -> {
              if (targetVector == null) {
                targetVector = new byte[OptimizedScalarQuantizer.discretize(values.dimension(), 2)];
              }
              OffHeapScalarQuantizedVectorValues.unpackNibbles(rawTargetVector, targetVector);
            }
          }
          targetCorrectiveTerms = targetValues.getCorrectiveTerms(node);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedVectorScorerSupplier(values.copy(), similarity);
    }
  }

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

  private static float quantizedScore(
      byte[] quantizedQuery,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections,
      QuantizedByteVectorValues targetVectors,
      int targetOrd,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    var scalarEncoding = targetVectors.getScalarEncoding();
    byte[] quantizedDoc = targetVectors.vectorValue(targetOrd);
    float qcDist =
        switch (scalarEncoding) {
          case UNSIGNED_BYTE -> VectorUtil.uint8DotProduct(quantizedQuery, quantizedDoc);
          case PACKED_NIBBLE -> VectorUtil.int4DotProductPacked(quantizedQuery, quantizedDoc);
        };
    OptimizedScalarQuantizer.QuantizationResult indexCorrections =
        targetVectors.getCorrectiveTerms(targetOrd);
    float scale = SCALE_LUT[scalarEncoding.getBits() - 1];
    float x1 = indexCorrections.quantizedComponentSum();
    float ax = indexCorrections.lowerInterval();
    // Here we must scale according to the bits
    float lx = (indexCorrections.upperInterval() - ax) * scale;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * scale;
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
