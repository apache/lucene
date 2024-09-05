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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.BQSpaceUtils;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/** Vector scorer over binarized vector values */
public class Lucene912BinaryFlatVectorsScorer implements BinaryFlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene912BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    // FIXME: write me ... presumably we can create a supplier here without a set of query vectors;
    // need to do that and figure out what that instantiation looks like for the Supplier
    if (vectorValues instanceof RandomAccessBinarizedByteVectorValues binarizedQueryVectors) {
      return new BinarizedRandomVectorScorerSupplier(
          null, binarizedQueryVectors.copy(), similarityFunction);
    }
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    if (vectorValues instanceof RandomAccessBinarizedByteVectorValues binarizedVectors) {
      BinaryQuantizer quantizer = binarizedVectors.getQuantizer();
      float[][] centroids = binarizedVectors.getCentroids();
      // FIXME: precompute this once?
      int discretizedDimensions = BQVectorUtils.discretize(target.length, 64);
      BinaryQueryVector[] queryVectors = new BinaryQueryVector[centroids.length];
      for (int i = 0; i < centroids.length; i++) {
        // TODO: if there are many clusters, do quantizing of query lazily
        byte[] quantized = new byte[BQSpaceUtils.B_QUERY * discretizedDimensions / 8];
        BinaryQuantizer.QueryFactors factors =
            quantizer.quantizeForQuery(target, quantized, centroids[i]);
        queryVectors[i] = new BinaryQueryVector(quantized, factors);
      }
      return new BinarizedRandomVectorScorer(
          queryVectors, binarizedVectors, similarityFunction, discretizedDimensions);
    }
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      RandomAccessBinarizedQueryByteVectorValues scoringVectors,
      RandomAccessBinarizedByteVectorValues targetVectors)
      throws IOException {
    return new BinarizedRandomVectorScorerSupplier(
        scoringVectors, targetVectors, similarityFunction);
  }

  @Override
  public String toString() {
    return "Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

  /** Vector scorer supplier over binarized vector values */
  public static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final RandomAccessBinarizedQueryByteVectorValues queryVectors;
    private final RandomAccessBinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    private final int discretizedDimensions;

    public BinarizedRandomVectorScorerSupplier(
        RandomAccessBinarizedQueryByteVectorValues queryVectors,
        RandomAccessBinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
      this.discretizedDimensions = BQVectorUtils.discretize(this.queryVectors.dimension(), 64);
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      BinaryQueryVector[] bQueryVectors = new BinaryQueryVector[queryVectors.centroidsCount()];
      for (int i = 0; i < queryVectors.centroidsCount(); i++) {
        int adjOrd = ord * queryVectors.centroidsCount() + i;
        byte[] queryVector = queryVectors.vectorValue(adjOrd);
        int quantizedSum = queryVectors.sumQuantizedValues(adjOrd, 0);
        float distanceToCentroid = queryVectors.getCentroidDistance(adjOrd, 0);
        float lower = queryVectors.getLower(adjOrd, 0);
        float width = queryVectors.getWidth(adjOrd, 0);
        float normVmC = queryVectors.getNormVmC(adjOrd, 0);
        float vDotC = queryVectors.getVDotC(adjOrd, 0);
        float cDotC = queryVectors.getCDotC(adjOrd, 0);

        bQueryVectors[i] =
            new BinaryQueryVector(
                queryVector,
                new BinaryQuantizer.QueryFactors(
                    quantizedSum, distanceToCentroid, lower, width, normVmC, vDotC, cDotC));
      }

      return new BinarizedRandomVectorScorer(
          bQueryVectors, targetVectors, similarityFunction, discretizedDimensions);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BinarizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarityFunction);
    }
  }

  /** A binarized query representing its quantized form along with factors */
  public record BinaryQueryVector(byte[] vector, BinaryQuantizer.QueryFactors factors) {}

  /** Vector scorer over binarized vector values */
  public static class BinarizedRandomVectorScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final BinaryQueryVector[] queryVectors;
    private final RandomAccessBinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    private final int discretizedDimensions;
    private final float sqrtDimensions;
    private final float maxX1;

    public BinarizedRandomVectorScorer(
        BinaryQueryVector[] queryVectors,
        RandomAccessBinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction,
        int discretizedDimensions) {
      super(targetVectors);
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;
      this.discretizedDimensions = discretizedDimensions;
      // FIXME: precompute this once?
      this.sqrtDimensions = (float) Utils.constSqrt(discretizedDimensions);
      this.maxX1 = (float) (1.9 / Utils.constSqrt(discretizedDimensions - 1.0));
    }

    // FIXME: utils class; pull this out
    private class Utils {
      public static double sqrtNewtonRaphson(double x, double curr, double prev) {
        return (curr == prev) ? curr : sqrtNewtonRaphson(x, 0.5 * (curr + x / curr), curr);
      }

      public static double constSqrt(double x) {
        return x >= 0 && !Double.isInfinite(x) ? sqrtNewtonRaphson(x, x, 0) : Double.NaN;
      }
    }

    @Override
    public float score(int targetOrd) throws IOException {
      // FIXME: implement fastscan in the future?

      short clusterId = targetVectors.getClusterId(targetOrd);
      BinaryQueryVector queryVector = queryVectors[clusterId];

      byte[] quantizedQuery = queryVector.vector();
      int quantizedSum = queryVector.factors().quantizedSum();
      float lower = queryVector.factors().lower();
      float width = queryVector.factors().width();
      float distanceToCentroid = queryVector.factors().distToC();

      return switch (similarityFunction) {
        case VectorSimilarityFunction.EUCLIDEAN:
        case VectorSimilarityFunction.COSINE:
        case VectorSimilarityFunction.DOT_PRODUCT:
          yield score(
              targetOrd,
              maxX1,
              sqrtDimensions,
              quantizedQuery,
              distanceToCentroid,
              lower,
              quantizedSum,
              width);
        case MAXIMUM_INNER_PRODUCT:
          float vmC = queryVector.factors().normVmC();
          float vDotC = queryVector.factors().vDotC();
          float cDotC = queryVector.factors().cDotC();
          yield scoreMIP(targetOrd, quantizedQuery, width, lower, quantizedSum, vmC, vDotC, cDotC);
      };
    }

    private float scoreMIP(
        int targetOrd,
        byte[] quantizedQuery,
        float width,
        float lower,
        int sumQ,
        float normVmC,
        float vDotC,
        float cDotC)
        throws IOException {
      byte[] binaryCode = targetVectors.vectorValue(targetOrd);
      float ooq = targetVectors.getOOQ(targetOrd);
      float normOC = targetVectors.getNormOC(targetOrd);
      float oDotC = targetVectors.getODotC(targetOrd);

      float qcDist = VectorUtil.ipByteBinByte(quantizedQuery, binaryCode);

      // FIXME: pre-compute these only once for each target vector
      //  ... pull this out or use a similar cache mechanism as do in score
      float xbSum = (float) BQVectorUtils.popcount(binaryCode);

      float estimatedDot =
          (2 * width / sqrtDimensions * qcDist
                  + 2 * lower / sqrtDimensions * xbSum
                  - width / sqrtDimensions * sumQ
                  - sqrtDimensions * lower)
              / ooq;

      float dist = normVmC * normOC * estimatedDot + oDotC + vDotC - cDotC;
      // FIXME: need a true error computation here; don't know what that is for MIP
      float errorBound = 0.0f; // FIXME: prior error bound computation: y * error;
      float score = dist + errorBound;
      return score > 0 ? score : 0f;
    }

    private float score(
        int targetOrd,
        float maxX1,
        float sqrtDimensions,
        byte[] quantizedQuery,
        float distanceToCentroid,
        float lower,
        int quantizedSum,
        float width)
        throws IOException {
      byte[] binaryCode = targetVectors.vectorValue(targetOrd);

      // FIXME: pre-compute these only once for each target vector
      // .. not sure how to enumerate the target ordinals but that's what we did in PoC
      float targetDistToC = targetVectors.getCentroidDistance(targetOrd);
      float x0 = targetVectors.getVectorMagnitude(targetOrd);
      float sqrX = targetDistToC * targetDistToC;
      double xX0 = targetDistToC / x0;
      float projectionDist = (float) Math.sqrt(xX0 * xX0 - targetDistToC * targetDistToC);
      float error = 2.0f * maxX1 * projectionDist;
      float xbSum = (float) BQVectorUtils.popcount(binaryCode);
      float factorPPC =
          (float) (-2.0 / sqrtDimensions * xX0 * (xbSum * 2.0 - discretizedDimensions));
      float factorIP = (float) (-2.0 / sqrtDimensions * xX0);

      long qcDist = VectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
      float y = (float) Math.sqrt(distanceToCentroid);
      float dist =
          sqrX
              + distanceToCentroid
              + factorPPC * lower
              + (qcDist * 2 - quantizedSum) * factorIP * width;
      float errorBound = y * error;
      float score = dist - errorBound;
      score = score > 0 ? score : 0f;
      return 1 / (1f + score);
    }
  }
}
