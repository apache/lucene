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
package org.apache.lucene.codecs.lucene101;

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
import org.apache.lucene.util.quantization.BQSpaceUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/** Vector scorer over binarized vector values */
public class Lucene101BinaryFlatVectorsScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene101BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
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

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof BinarizedByteVectorValues binarizedVectors) {
      BinaryQuantizer quantizer = binarizedVectors.getQuantizer();
      float[] centroid = binarizedVectors.getCentroid();
      if (similarityFunction == COSINE) {
        float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
        VectorUtil.l2normalize(copy);
        target = copy;
      }
      byte[] quantized =
          new byte[BQSpaceUtils.B_QUERY * binarizedVectors.discretizedDimensions() / 8];
      float[] queryCorrections = quantizer.quantizeForQuery(target, quantized, centroid);
      BinaryQueryVector queryVector = new BinaryQueryVector(quantized, queryCorrections);
      return new BinarizedRandomVectorScorer(queryVector, binarizedVectors, similarityFunction);
    }
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      Lucene101BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
      BinarizedByteVectorValues targetVectors) {
    return new BinarizedRandomVectorScorerSupplier(
        scoringVectors, targetVectors, similarityFunction);
  }

  @Override
  public String toString() {
    return "Lucene101BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

  /** Vector scorer supplier over binarized vector values */
  static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final Lucene101BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues
        queryVectors;
    private final BinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    BinarizedRandomVectorScorerSupplier(
        Lucene101BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
        BinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction) {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      byte[] vector = queryVectors.vectorValue(ord);
      float[] correctiveTerms = queryVectors.getCorrectiveTerms(ord);
      BinaryQueryVector binaryQueryVector = new BinaryQueryVector(vector, correctiveTerms);
      return new BinarizedRandomVectorScorer(binaryQueryVector, targetVectors, similarityFunction);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BinarizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarityFunction);
    }
  }

  /** A binarized query representing its quantized form along with factors */
  public record BinaryQueryVector(byte[] vector, float[] factors) {}

  /** Vector scorer over binarized vector values */
  public static class BinarizedRandomVectorScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final BinaryQueryVector queryVector;
    private final BinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    private final float sqrtDimensions;
    private final float maxX1;

    public BinarizedRandomVectorScorer(
        BinaryQueryVector queryVectors,
        BinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction) {
      super(targetVectors);
      this.queryVector = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
      this.sqrtDimensions = targetVectors.sqrtDimensions();
      this.maxX1 = targetVectors.maxX1();
    }

    @Override
    public float score(int targetOrd) throws IOException {
      byte[] quantizedQuery = queryVector.vector();
      byte[] binaryCode = targetVectors.vectorValue(targetOrd);
      float qcDist = VectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
      float xbSum = (float) VectorUtil.popCount(binaryCode);
      float[] correctiveTerms = targetVectors.getCorrectiveTerms(targetOrd);
      if (similarityFunction == EUCLIDEAN) {
        return euclideanScore(xbSum, qcDist, correctiveTerms, queryVector.factors);
      }
      return dotProductScore(
          xbSum, qcDist, targetVectors.getCentroidDP(), correctiveTerms, queryVector.factors);
    }

    private float dotProductScore(
        float xbSum,
        float qcDist,
        float cDotC,
        float[] vectorCorrectiveTerms,
        float[] queryCorrectiveTerms) {
      assert vectorCorrectiveTerms.length == 3;
      assert queryCorrectiveTerms.length == 5;
      float lower = queryCorrectiveTerms[0] / sqrtDimensions;
      float width = queryCorrectiveTerms[1] / sqrtDimensions;
      float vmC = queryCorrectiveTerms[2];
      float vDotC = queryCorrectiveTerms[3];
      float quantizedSum = queryCorrectiveTerms[4];
      float ooq = vectorCorrectiveTerms[0];
      float vmcNormOC = vectorCorrectiveTerms[1] * vmC;
      float oDotC = vectorCorrectiveTerms[2];

      final float dist;
      // If ||o-c|| == 0, so, it's ok to throw the rest of the equation away
      // and simply use `oDotC + vDotC - cDotC` as centroid == doc vector
      if (vmcNormOC == 0 || ooq == 0) {
        dist = oDotC + vDotC - cDotC;
      } else {
        // If ||o-c|| != 0, we should assume that `ooq` is finite
        assert Float.isFinite(ooq);
        float estimatedDot =
            (2 * width * qcDist
                    + 2 * lower * xbSum
                    - width * quantizedSum
                    - targetVectors.dimension() * lower)
                / ooq;
        dist = vmcNormOC * estimatedDot + oDotC + vDotC - cDotC;
      }
      assert Float.isFinite(dist);

      float ooqSqr = ooq * ooq;
      float errorBound = (float) (vmcNormOC * (maxX1 * Math.sqrt((1 - ooqSqr) / ooqSqr)));
      float score = Float.isFinite(errorBound) ? dist - errorBound : dist;
      if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
        return VectorUtil.scaleMaxInnerProductScore(score);
      }
      return Math.max((1f + score) / 2f, 0);
    }

    private float euclideanScore(
        float xbSum, float qcDist, float[] vectorCorrectiveTerms, float[] queryCorrectiveTerms) {
      assert vectorCorrectiveTerms.length == 2;
      assert queryCorrectiveTerms.length == 4;
      float distanceToCentroid = queryCorrectiveTerms[0];
      float lower = queryCorrectiveTerms[1];
      float width = queryCorrectiveTerms[2];
      float quantizedSum = queryCorrectiveTerms[3];

      float targetDistToC = vectorCorrectiveTerms[0];
      float x0 = vectorCorrectiveTerms[1];
      float sqrX = targetDistToC * targetDistToC;
      double xX0 = targetDistToC / x0;

      float factorPPC =
          (float) (-2.0 / sqrtDimensions * xX0 * (xbSum * 2.0 - targetVectors.dimension()));
      float factorIP = (float) (-2.0 / sqrtDimensions * xX0);

      float score =
          sqrX
              + distanceToCentroid
              + factorPPC * lower
              + (qcDist * 2 - quantizedSum) * factorIP * width;
      float projectionDist = (float) Math.sqrt(xX0 * xX0 - targetDistToC * targetDistToC);
      float error = 2.0f * maxX1 * projectionDist;
      float y = (float) Math.sqrt(distanceToCentroid);
      float errorBound = y * error;
      if (Float.isFinite(errorBound)) {
        score = score + errorBound;
      }
      return Math.max(1 / (1f + score), 0);
    }
  }
}
