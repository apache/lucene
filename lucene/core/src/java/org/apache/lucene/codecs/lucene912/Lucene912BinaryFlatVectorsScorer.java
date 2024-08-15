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
import java.util.BitSet;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
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
    if (vectorValues instanceof RandomAccessBinarizedByteVectorValues binarizedQueryVectors) {}
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
      byte[] quantized = new byte[(target.length + 1) / 2];
      quantizer.quantizeForQuery(target, quantized, centroids[0]);
      int discretizedDimensions = (target.length + 63) / 64 * 64;
      return new BinarizedRandomVectorScorer(
          new BinaryQueryVector[] {new BinaryQueryVector(quantized, 0, 0, 0, 0)},
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

    // FIXME: can we pull this out further?
    private final int discretizedDimensions;

    public BinarizedRandomVectorScorerSupplier(
        RandomAccessBinarizedQueryByteVectorValues queryVectors,
        RandomAccessBinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarityFunction = similarityFunction;

      this.discretizedDimensions = (this.queryVectors.dimension() + 63) / 64 * 64;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      byte[] queryVector = queryVectors.vectorValue(ord);
      float distanceToCentroid = queryVectors.getCentroidDistance(ord, 0);
      float vl = queryVectors.getVl(ord, 0);
      float width = queryVectors.getWidth(ord, 0);
      int quantizedSum = queryVectors.sumQuantizedValues(ord, 0);
      return new BinarizedRandomVectorScorer(
          new BinaryQueryVector[] {
            new BinaryQueryVector(queryVector, distanceToCentroid, vl, width, quantizedSum)
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

  // TODO, eh, maybe not a record, just add the fields to scorer?
  public record BinaryQueryVector(
      byte[] vector, float distanceToCentroid, float vl, float width, int quantizedSum) {}

  public static class BinarizedRandomVectorScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final BinaryQueryVector[] queryVectors;
    private final RandomAccessBinarizedByteVectorValues targetVectors;
    private final VectorSimilarityFunction similarityFunction;

    // FIXME: does it make sense to have this here or should be higher or lower?  not sure ...
    // computed on score?
    private final int discretizedDimensions;

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
    }

    // FIXME: pull this out
    private static final int B_QUERY = 4;

    private static long ipByteBinByte(byte[] q, byte[] d) {
      long ret = 0;
      int size = d.length;
      for (int i = 0; i < B_QUERY; i++) {
        int r = 0;
        long subRet = 0;
        for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
          subRet +=
              Integer.bitCount(
                  (int) BitUtil.VH_NATIVE_INT.get(q, i * size + r)
                      & (int) BitUtil.VH_NATIVE_INT.get(d, r));
        }
        for (; r < d.length; r++) {
          subRet += Integer.bitCount((q[i * size + r] & d[i]) & 0xFF);
        }
        ret += subRet << i;
      }
      return ret;
    }

    // FIXME: utils class and duplicate from BinaryQuantizer
    private static int popcount(byte[] d, int dimensions) {
      return BitSet.valueOf(d).cardinality();
    }

    // FIXME: utils class; pull this out
    public class Utils {
      public static double sqrtNewtonRaphson(double x, double curr, double prev) {
        return (curr == prev) ? curr : sqrtNewtonRaphson(x, 0.5 * (curr + x / curr), curr);
      }

      public static double constSqrt(double x) {
        return x >= 0 && !Double.isInfinite(x) ? sqrtNewtonRaphson(x, x, 0) : Double.NaN;
      }
    }

    // FIXME: write tests to validate how this flow works and that a score is realized
    @Override
    public float score(int targetOrd) {
      // FIXME: write me
      // FIXME: deal with similarity function .. for now assuming euclidean
      // FIXME: implement fastscan in the future
      // FIXME: clean up and rename the variables

      switch (similarityFunction) {
        case VectorSimilarityFunction.EUCLIDEAN:
        case VectorSimilarityFunction.COSINE:
        case VectorSimilarityFunction.DOT_PRODUCT:
          // FIXME: why is this an array of query vectors? ... how to return more than a single
          // score
          //      for(int i = 0; i < queryVectors.length; i++) {
          BinaryQueryVector queryVector = queryVectors[0];

          // FIXME: throw exception with these I'm assuming??
          float targetDistToCentroid;
          byte[] binaryCode;
          float x0;
          try {
            targetDistToCentroid = targetVectors.getCentroidDistance(targetOrd);
            binaryCode = targetVectors.vectorValue(targetOrd);
            x0 = targetVectors.getVectorMagnitude(targetOrd);
          } catch (IOException e) {
            // FIXME: bad
            throw new RuntimeException(e);
          }

          byte[] quantizedQuery = queryVector.vector();
          float sumQ = queryVector.quantizedSum();
          float vl = queryVector.vl();
          float width = queryVector.width();
          float distanceToCentroid = queryVector.distanceToCentroid();

          // FIXME: pre-compute these only once for each target vector ... pull this out ...
          // somewhere?
          // .. not sure how to enumerate the target ordinals but it seems expensive to do all of
          // this here
          float sqrX = targetDistToCentroid * targetDistToCentroid;
          double xX0 = targetDistToCentroid / x0;
          float sqrtDimensions = (float) Utils.constSqrt(discretizedDimensions);
          float maxX1 = (float) (1.9 / Utils.constSqrt(discretizedDimensions - 1.0));
          float error =
              (float)
                  (2.0
                      * maxX1
                      * Math.sqrt(xX0 * xX0 - targetDistToCentroid * targetDistToCentroid));
          float factorPPC =
              (float)
                  (-2.0
                      / sqrtDimensions
                      * xX0
                      * ((float) popcount(binaryCode, discretizedDimensions) * 2.0
                          - discretizedDimensions));
          float factorIP = (float) (-2.0 / sqrtDimensions * xX0);
          ////////

          long qcDist = ipByteBinByte(quantizedQuery, binaryCode);
          float y = (float) Math.sqrt(distanceToCentroid);
          float tmpDist =
              sqrX + distanceToCentroid + factorPPC * vl + (qcDist * 2 - sumQ) * factorIP * width;
          float errorBound = y * error;
          return tmpDist - errorBound;
        case MAXIMUM_INNER_PRODUCT:
          // FIXME: write me ... have impl waiting to reconcile some of the above questions before
          // including
          // FIXME: need a way to pull through the different corrective factors
          // FIXME: revisit how we pass through the correct factors with a class that's just about
          // looking up those factors
          return 0.0f;
        default:
          throw new UnsupportedOperationException(
              "Unsupported similarity function: " + similarityFunction);
      }
    }
  }
}
