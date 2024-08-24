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
    if (vectorValues instanceof RandomAccessBinarizedByteVectorValues binarizedQueryVectors) {
      // TODO, implement & handle more than one coarse grained cluster
      BinaryQuantizer quantizer = binarizedQueryVectors.getQuantizer();
      float[][] centroids = binarizedQueryVectors.getCentroids();
      int discretizedDimensions = BQVectorUtils.discretize(target.length, 64);
      byte[] quantized = new byte[BQSpaceUtils.B_QUERY * discretizedDimensions / 8];
      float distToCentroid = VectorUtil.squareDistance(target, centroids[0]);
      BinaryQuantizer.QueryFactors factors =
          quantizer.quantizeForQuery(target, quantized, centroids[0]);
      return new BinarizedRandomVectorScorer(
          new BinaryQueryVector[] {new BinaryQueryVector(quantized, distToCentroid, factors)},
          binarizedQueryVectors,
          similarityFunction,
          discretizedDimensions);
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
    return null;
  }

  @Override
  public String toString() {
    return "Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

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
      byte[] queryVector = queryVectors.vectorValue(ord);

      short quantizedSum = queryVectors.sumQuantizedValues(ord, 0);

      float distanceToCentroid = queryVectors.getCentroidDistance(ord, 0);
      float vl = queryVectors.getLower(ord, 0);
      float width = queryVectors.getWidth(ord, 0);

      float normVmC = queryVectors.getNormVmC(ord, 0);
      float vDotC = queryVectors.getVDotC(ord, 0);
      float cDotC = queryVectors.getCDotC(ord, 0);

      return new BinarizedRandomVectorScorer(
          new BinaryQueryVector[] {
            new BinaryQueryVector(
                queryVector,
                distanceToCentroid,
                new BinaryQuantizer.QueryFactors(quantizedSum, vl, width, normVmC, vDotC, cDotC))
          },
          targetVectors,
          similarityFunction,
          discretizedDimensions);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BinarizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarityFunction);
    }
  }

  public record BinaryQueryVector(
      byte[] vector, float distanceToCentroid, BinaryQuantizer.QueryFactors factors) {}

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

    //    record Factors(float sqrX, float error, float PPC, float IP) {}
    // FIXME: use a weak map instead -- limit the size .. LRU cache .. thread safe
    //    private final HashMap<Integer, Factors> factorsCache = new WeakHashMap<>();

    @Override
    public float score(int targetOrd) throws IOException {
      // FIXME: implement fastscan in the future?

      // FIXME: handle multiple query vectors
      BinaryQueryVector queryVector = queryVectors[0];

      byte[] quantizedQuery = queryVector.vector();
      short quantizedSum = queryVector.factors().quantizedSum();
      float lower = queryVector.factors().lower();
      float width = queryVector.factors().width();
      float distanceToCentroid = queryVector.distanceToCentroid();

      switch (similarityFunction) {
        case VectorSimilarityFunction.EUCLIDEAN:
        case VectorSimilarityFunction.COSINE:
        case VectorSimilarityFunction.DOT_PRODUCT:
          return score(
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
          return scoreMIP(targetOrd, quantizedQuery, width, lower, quantizedSum, vmC, vDotC, cDotC);
        default:
          throw new UnsupportedOperationException(
              "Unsupported similarity function: " + similarityFunction);
      }
    }

    private float scoreMIP(
        int targetOrd,
        byte[] quantizedQuery,
        float width,
        float lower,
        short sumQ,
        float normVmC,
        float vDotC,
        float cDotC)
        throws IOException {
      byte[] binaryCode = targetVectors.vectorValue(targetOrd);
      float ooq = targetVectors.getOOQ(targetOrd);
      float normOC = targetVectors.getNormOC(targetOrd);
      float oDotC = targetVectors.getODotC(targetOrd);

      float qcDist = VectorUtil.ipByteBinByte(quantizedQuery, binaryCode);

      // FIXME: cache this value or pull it out to the scorer creation
      float sqrtD = (float) Math.sqrt(discretizedDimensions);

      // FIXME: pre-compute these only once for each target vector ... pull this out ...
      float xbSum = (float) BQVectorUtils.popcount(binaryCode, discretizedDimensions);

      float estimatedDot =
          (2 * width / sqrtD * qcDist + 2 * lower / sqrtD * xbSum - width / sqrtD * sumQ - sqrtD * lower)
              / ooq;

      float dist = normVmC * normOC * estimatedDot + oDotC + vDotC - cDotC;
      // FIXME: need a true error computation here; don't know what that is for MIP
      float errorBound = 0.0f; // FIXME: prior error bound computation: y * error;
      return dist + errorBound;
    }

    private float score(
        int targetOrd,
        float maxX1,
        float sqrtDimensions,
        byte[] quantizedQuery,
        float distanceToCentroid,
        float lower,
        short quantizedSum,
        float width)
        throws IOException {
      byte[] binaryCode;
      long qcDist;
      float errorBound;
      float dist;
      float targetDistToC = targetVectors.getCentroidDistance(targetOrd);
      binaryCode = targetVectors.vectorValue(targetOrd);
      float x0 = targetVectors.getVectorMagnitude(targetOrd);

      // FIXME: pre-compute these only once for each target vector ... pull this out ...
      // .. not sure how to enumerate the target ordinals but it seems expensive to this here
      // ... could cache these as well on each call to score using targetOrd as the lookup
      float sqrX;
      float error;
      float factorPPC;
      float factorIP;
      //          if (factorsCache.containsKey(targetOrd)) {
      //            Factors factors = factorsCache.get(targetOrd);
      //            sqrX = factors.sqrX();
      //            error = factors.error();
      //            factorPPC = factors.PPC();
      //            factorIP = factors.IP();
      //          } else {
      sqrX = targetDistToC * targetDistToC;
      double xX0 = targetDistToC / x0;
      float projectionDist = (float) Math.sqrt(xX0 * xX0 - targetDistToC * targetDistToC);
      error = 2.0f * maxX1 * projectionDist;
      float xbSum = (float) BQVectorUtils.popcount(binaryCode, discretizedDimensions);
      factorPPC = (float) (-2.0 / sqrtDimensions * xX0 * (xbSum * 2.0 - discretizedDimensions));
      factorIP = (float) (-2.0 / sqrtDimensions * xX0);
      //            factorsCache.put(targetOrd, new Factors(sqrX, error, factorPPC, factorIP));
      //          }
      ////////

      qcDist = VectorUtil.ipByteBinByte(quantizedQuery, binaryCode);
      float y = (float) Math.sqrt(distanceToCentroid);
      dist =
          sqrX
              + distanceToCentroid
              + factorPPC * lower
              + (qcDist * 2 - quantizedSum) * factorIP * width;
      errorBound = y * error;
      return dist - errorBound;
    }
  }
}
