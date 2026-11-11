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
import org.apache.lucene.codecs.hnsw.AsymmetricFlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Vector scorer over OptimizedScalarQuantized vectors
 *
 * @lucene.experimental
 */
public class Lucene104ScalarQuantizedVectorScorer implements AsymmetricFlatVectorsScorer {
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
      FlatVectorsScorer.checkDimensions(target.length, qv.dimension());
      // We make a copy as the quantization process mutates the input
      return getRandomQuantizedVectorScorer(similarityFunction, qv, ArrayUtil.copyArray(target));
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    FlatVectorsScorer.checkDimensions(target.length, vectorValues.dimension());
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, short[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      FlatVectorsScorer.checkDimensions(target.length, qv.dimension());
      // Quantization operates on fp32, so inflate the fp16 query first
      // TODO: quantize fp16 directly, see https://github.com/apache/lucene/issues/16533
      float[] inflated = new float[target.length];
      for (int i = 0; i < target.length; i++) {
        inflated[i] = Float.float16ToFloat(target[i]);
      }
      return getRandomQuantizedVectorScorer(similarityFunction, qv, inflated);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  /**
   * Quantizes {@code query} against the centroid of {@code qv} and returns a scorer over the
   * quantized vectors. {@code query} is modified in place.
   */
  private RandomVectorScorer getRandomQuantizedVectorScorer(
      VectorSimilarityFunction similarityFunction, QuantizedByteVectorValues qv, float[] query)
      throws IOException {
    OptimizedScalarQuantizer quantizer = qv.getQuantizer();
    ScalarEncoding scalarEncoding = qv.getScalarEncoding();
    byte[] scratch = new byte[scalarEncoding.getDiscreteDimensions(qv.dimension())];
    final byte[] targetQuantized;
    if (scalarEncoding.isAsymmetric() == false) {
      targetQuantized = scratch;
    } else {
      // This is asymmetric quantization, we will pack the vector
      targetQuantized = new byte[scalarEncoding.getQueryPackedLength(scratch.length)];
    }
    if (similarityFunction == COSINE) {
      VectorUtil.l2normalize(query);
    }
    var targetCorrectiveTerms =
        quantizer.scalarQuantize(query, scratch, scalarEncoding.getQueryBits(), qv.getCentroid());
    // for asymmetric encodings with 4-bit query, we need to transpose the nibbles for fast
    // scoring comparisons
    if (scalarEncoding == ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE
        || scalarEncoding == ScalarEncoding.DIBIT_QUERY_NIBBLE) {
      OptimizedScalarQuantizer.transposeHalfByte(scratch, targetQuantized);
    }
    return getQuantizedQueryScorer(similarityFunction, qv, targetQuantized, targetCorrectiveTerms);
  }

  /**
   * Returns a scorer of a quantized query against the vectors of {@code qv}. Subclasses may
   * override this to score the vectors another way.
   *
   * @param quantizedQuery the query, quantized to {@link ScalarEncoding#getQueryBits()} and packed
   *     like the query side of the encoding
   * @param queryCorrections the corrective terms of the quantized query
   * @lucene.internal
   */
  protected RandomVectorScorer getQuantizedQueryScorer(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues qv,
      byte[] quantizedQuery,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections)
      throws IOException {
    return new RandomVectorScorer.AbstractRandomVectorScorer(qv) {
      @Override
      public float score(int node) throws IOException {
        return quantizedScore(quantizedQuery, queryCorrections, qv, node, similarityFunction);
      }
    };
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues scoringVectors,
      QuantizedByteVectorValues targetVectors)
      throws IOException {
    return new AsymmetricQuantizedRandomVectorScorerSupplier(
        scoringVectors, targetVectors, similarityFunction);
  }

  @Override
  public String toString() {
    return "Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate="
        + nonQuantizedDelegate
        + ")";
  }

  static class AsymmetricQuantizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues queryVectors;
    private final QuantizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    AsymmetricQuantizedRandomVectorScorerSupplier(
        QuantizedByteVectorValues queryVectors,
        QuantizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction) {
      assert targetVectors.getScalarEncoding().isAsymmetric();
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      final QuantizedByteVectorValues targetVectors = this.targetVectors.copy();
      final QuantizedByteVectorValues queryVectors = this.queryVectors.copy();
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(targetVectors) {
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections = null;
        private byte[] vector = null;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          vector = queryVectors.vectorValue(node);
          queryCorrections = queryVectors.getCorrectiveTerms(node);
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
      return new AsymmetricQuantizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarityFunction);
    }
  }

  private static final class ScalarQuantizedVectorScorerSupplier
      implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues targetValues;
    private final QuantizedByteVectorValues values;
    private final VectorSimilarityFunction similarity;

    public ScalarQuantizedVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction similarity) throws IOException {
      assert values.getScalarEncoding().isAsymmetric() == false;
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
            case UNSIGNED_BYTE, SEVEN_BIT -> targetVector = rawTargetVector;
            case PACKED_NIBBLE -> {
              if (targetVector == null) {
                targetVector = new byte[OptimizedScalarQuantizer.discretize(values.dimension(), 2)];
              }
              OffHeapScalarQuantizedVectorValues.unpackNibbles(rawTargetVector, targetVector);
            }
            case SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE -> {
              throw new IllegalStateException(
                  values.getScalarEncoding().name()
                      + " encoding is not supported for symmetric quantization");
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

  /**
   * Scores the quantized query against the vector at {@code targetOrd} of {@code targetVectors},
   * reading the vector on heap.
   *
   * @lucene.internal
   */
  protected static float quantizedScore(
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
          case SEVEN_BIT -> VectorUtil.dotProduct(quantizedQuery, quantizedDoc);
          case PACKED_NIBBLE -> VectorUtil.int4DotProductSinglePacked(quantizedQuery, quantizedDoc);
          case SINGLE_BIT_QUERY_NIBBLE ->
              VectorUtil.int4BitDotProduct(quantizedQuery, quantizedDoc);
          case DIBIT_QUERY_NIBBLE -> VectorUtil.int4DibitDotProduct(quantizedQuery, quantizedDoc);
        };
    OptimizedScalarQuantizer.QuantizationResult indexCorrections =
        targetVectors.getCorrectiveTerms(targetOrd);
    return quantizedScore(
        qcDist,
        queryCorrections,
        indexCorrections.lowerInterval(),
        indexCorrections.upperInterval(),
        indexCorrections.additionalCorrection(),
        indexCorrections.quantizedComponentSum(),
        scalarEncoding,
        targetVectors.dimension(),
        targetVectors.getCentroidDP(),
        similarityFunction);
  }

  /**
   * Turns {@code qcDist}, the dot product of a quantized query and a quantized vector, into a
   * similarity score using the corrective terms of both. The corrective terms of the vector and the
   * properties of its field are passed as values, so that a caller that reads them from the index
   * allocates nothing and needs no {@link QuantizedByteVectorValues}.
   *
   * @param scalarEncoding the encoding of the vectors
   * @param dimension the dimension of the vectors
   * @param centroidDp the dot product of the centroid with itself, see {@link
   *     QuantizedByteVectorValues#getCentroidDP}
   * @lucene.internal
   */
  protected static float quantizedScore(
      float qcDist,
      OptimizedScalarQuantizer.QuantizationResult queryCorrections,
      float lowerInterval,
      float upperInterval,
      float additionalCorrection,
      int quantizedComponentSum,
      ScalarEncoding scalarEncoding,
      int dimension,
      float centroidDp,
      VectorSimilarityFunction similarityFunction) {
    float queryScale = SCALE_LUT[scalarEncoding.getQueryBits() - 1];
    float scale = SCALE_LUT[scalarEncoding.getBits() - 1];
    float x1 = quantizedComponentSum;
    float ax = lowerInterval;
    // Here we must scale according to the bits
    float lx = (upperInterval - ax) * scale;
    float ay = queryCorrections.lowerInterval();
    float ly = (queryCorrections.upperInterval() - ay) * queryScale;
    float y1 = queryCorrections.quantizedComponentSum();
    float score = ax * ay * dimension + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
    // For euclidean, we need to invert the score and apply the additional correction, which is
    // assumed to be the squared l2norm of the centroid centered vectors.
    if (similarityFunction == EUCLIDEAN) {
      score = queryCorrections.additionalCorrection() + additionalCorrection - 2 * score;
      // Ensure that 'score' (the squared euclidean distance) is non-negative. The computed value
      // may be negative as a result of quantization loss.
      return 1 / (1f + Math.max(score, 0f));
    } else {
      // For cosine and max inner product, we need to apply the additional correction, which is
      // assumed to be the non-centered dot-product between the vector and the centroid
      score += queryCorrections.additionalCorrection() + additionalCorrection - centroidDp;
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      // Ensure that 'score' (a normalized dot product) is in [-1,1]. The computed value may be out
      // of bounds as a result of quantization loss.
      score = Math.clamp(score, -1, 1);
      return (1f + score) / 2f;
    }
  }
}
