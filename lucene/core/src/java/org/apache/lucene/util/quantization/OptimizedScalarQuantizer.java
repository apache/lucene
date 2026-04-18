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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * This is a scalar quantizer that optimizes the quantization intervals for a given vector. This is
 * done by optimizing the quantiles of the vector centered on a provided centroid. The optimization
 * is done by minimizing the quantization loss via coordinate descent.
 *
 * <p>Local vector quantization parameters was originally proposed with LVQ in <a
 * href="https://arxiv.org/abs/2304.04759">Similarity search in the blink of an eye with compressed
 * indices</a> This technique builds on LVQ, but instead of taking the min/max values, a grid search
 * over the centered vector is done to find the optimal quantization intervals, taking into account
 * anisotropic loss.
 *
 * <p>Anisotropic loss is first discussed in depth by <a
 * href="https://arxiv.org/abs/1908.10396">Accelerating Large-Scale Inference with Anisotropic
 * Vector Quantization</a> by Ruiqi Guo, et al.
 *
 * @lucene.experimental
 */
public class OptimizedScalarQuantizer {
  // The initial interval is set to the minimum MSE grid for each number of bits
  // these starting points are derived from the optimal MSE grid for a uniform distribution
  static final float[][] MINIMUM_MSE_GRID =
      new float[][] {
        {-0.798f, 0.798f},
        {-1.493f, 1.493f},
        {-2.051f, 2.051f},
        {-2.514f, 2.514f},
        {-2.916f, 2.916f},
        {-3.278f, 3.278f},
        {-3.611f, 3.611f},
        {-3.922f, 3.922f}
      };
  // the default lambda value
  private static final float DEFAULT_LAMBDA = 0.1f;
  // the default optimization iterations allowed
  private static final int DEFAULT_ITERS = 5;
  private final VectorSimilarityFunction similarityFunction;
  // This determines how much emphasis we place on quantization errors perpendicular to the
  // embedding
  // as opposed to parallel to it.
  // The smaller the value the more we will allow the overall error to increase if it allows us to
  // reduce error parallel to the vector.
  // Parallel errors are important for nearest neighbor queries because the closest document vectors
  // tend to be parallel to the query
  private final float lambda;
  // the number of iterations to optimize the quantization intervals
  private final int iters;

  /**
   * Create a new scalar quantizer with the given similarity function, lambda, and number of
   * iterations.
   *
   * @param similarityFunction similarity function to use
   * @param lambda lambda value to use
   * @param iters number of iterations to use
   */
  public OptimizedScalarQuantizer(
      VectorSimilarityFunction similarityFunction, float lambda, int iters) {
    this.similarityFunction = similarityFunction;
    this.lambda = lambda;
    this.iters = iters;
  }

  /**
   * Create a new scalar quantizer with the default lambda and number of iterations.
   *
   * @param similarityFunction similarity function to use
   */
  public OptimizedScalarQuantizer(VectorSimilarityFunction similarityFunction) {
    this(similarityFunction, DEFAULT_LAMBDA, DEFAULT_ITERS);
  }

  /**
   * Quantization result containing the lower and upper interval bounds, the additional correction
   *
   * @param lowerInterval the lower interval bound
   * @param upperInterval the upper interval bound
   * @param additionalCorrection the additional correction
   * @param quantizedComponentSum the sum of the quantized components
   */
  public record QuantizationResult(
      float lowerInterval,
      float upperInterval,
      float additionalCorrection,
      int quantizedComponentSum) {}

  /**
   * Quantize the vector to the multiple bit levels.
   *
   * @param vector raw vector
   * @param destinations array of destinations to store the quantized vector
   * @param bits array of bits to quantize the vector
   * @param centroid centroid to center the vector
   * @return array of quantization results
   */
  public QuantizationResult[] multiScalarQuantize(
      float[] vector, byte[][] destinations, byte[] bits, float[] centroid) {
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
    assert bits.length == destinations.length;
    float[] intervalScratch = new float[2];
    double vecMean = 0;
    double vecVar = 0;
    float norm2 = 0;
    float centroidDot = 0;
    float min = Float.MAX_VALUE;
    float max = -Float.MAX_VALUE;
    for (int i = 0; i < vector.length; ++i) {
      if (similarityFunction != EUCLIDEAN) {
        centroidDot += vector[i] * centroid[i];
      }
      vector[i] = vector[i] - centroid[i];
      min = Math.min(min, vector[i]);
      max = Math.max(max, vector[i]);
      norm2 += (vector[i] * vector[i]);
      double delta = vector[i] - vecMean;
      vecMean += delta / (i + 1);
      vecVar += delta * (vector[i] - vecMean);
    }
    vecVar /= vector.length;
    double vecStd = Math.sqrt(vecVar);
    QuantizationResult[] results = new QuantizationResult[bits.length];
    for (int i = 0; i < bits.length; ++i) {
      assert bits[i] > 0 && bits[i] <= 8;
      int points = (1 << bits[i]);
      // Linearly scale the interval to the standard deviation of the vector, ensuring we are within
      // the min/max bounds
      intervalScratch[0] =
          (float) clamp(MINIMUM_MSE_GRID[bits[i] - 1][0] * vecStd + vecMean, min, max);
      intervalScratch[1] =
          (float) clamp(MINIMUM_MSE_GRID[bits[i] - 1][1] * vecStd + vecMean, min, max);
      optimizeIntervals(intervalScratch, vector, norm2, points);
      float nSteps = ((1 << bits[i]) - 1);
      float a = intervalScratch[0];
      float b = intervalScratch[1];
      float step = (b - a) / nSteps;
      int sumQuery = 0;
      // Now we have the optimized intervals, quantize the vector
      for (int h = 0; h < vector.length; h++) {
        float xi = (float) clamp(vector[h], a, b);
        int assignment = Math.round((xi - a) / step);
        sumQuery += assignment;
        destinations[i][h] = (byte) assignment;
      }
      results[i] =
          new QuantizationResult(
              intervalScratch[0],
              intervalScratch[1],
              similarityFunction == EUCLIDEAN ? norm2 : centroidDot,
              sumQuery);
    }
    return results;
  }

  /**
   * Quantize the vector to the given bit level.
   *
   * @param vector raw vector
   * @param destination destination to store the quantized vector
   * @param bits number of bits to quantize the vector
   * @param centroid centroid to center the vector
   * @return quantization result
   */
  public QuantizationResult scalarQuantize(
      float[] vector, byte[] destination, byte bits, float[] centroid) {
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
    assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
    assert vector.length <= destination.length;
    assert bits > 0 && bits <= 8;
    float[] intervalScratch = new float[2];
    int points = 1 << bits;
    double vecMean = 0;
    double vecVar = 0;
    float norm2 = 0;
    float centroidDot = 0;
    float min = Float.MAX_VALUE;
    float max = -Float.MAX_VALUE;
    for (int i = 0; i < vector.length; ++i) {
      if (similarityFunction != EUCLIDEAN) {
        centroidDot += vector[i] * centroid[i];
      }
      vector[i] = vector[i] - centroid[i];
      min = Math.min(min, vector[i]);
      max = Math.max(max, vector[i]);
      norm2 += (vector[i] * vector[i]);
      double delta = vector[i] - vecMean;
      vecMean += delta / (i + 1);
      vecVar += delta * (vector[i] - vecMean);
    }
    vecVar /= vector.length;
    double vecStd = Math.sqrt(vecVar);
    // Linearly scale the interval to the standard deviation of the vector, ensuring we are within
    // the min/max bounds
    intervalScratch[0] = (float) clamp(MINIMUM_MSE_GRID[bits - 1][0] * vecStd + vecMean, min, max);
    intervalScratch[1] = (float) clamp(MINIMUM_MSE_GRID[bits - 1][1] * vecStd + vecMean, min, max);
    optimizeIntervals(intervalScratch, vector, norm2, points);
    float nSteps = ((1 << bits) - 1);
    // Now we have the optimized intervals, quantize the vector
    float a = intervalScratch[0];
    float b = intervalScratch[1];
    float step = (b - a) / nSteps;
    int sumQuery = 0;
    for (int h = 0; h < vector.length; h++) {
      float xi = (float) clamp(vector[h], a, b);
      int assignment = Math.round((xi - a) / step);
      sumQuery += assignment;
      destination[h] = (byte) assignment;
    }
    return new QuantizationResult(
        intervalScratch[0],
        intervalScratch[1],
        similarityFunction == EUCLIDEAN ? norm2 : centroidDot,
        sumQuery);
  }

  /**
   * Dequantizes a quantized byte vector back to float values.
   *
   * <p>This method reconstructs float vectors from their quantized byte representation using linear
   * interpolation between the corrective range [a, b] and adding back the centroid offset.
   *
   * @param quantized the quantized byte vector to dequantize
   * @param dequantized the output array to store dequantized float values
   * @param bits the number of bits used for quantization
   * @param lowerInterval lower value of quantization range
   * @param upperInterval upper value of quantization range
   * @param centroid the centroid vector that was subtracted during quantization
   * @return the dequantized float array (same as dequantized parameter)
   */
  public static float[] deQuantize(
      byte[] quantized,
      float[] dequantized,
      byte bits,
      float lowerInterval,
      float upperInterval,
      float[] centroid) {
    int nSteps = (1 << bits) - 1;
    double step = (upperInterval - lowerInterval) / nSteps;
    for (int h = 0; h < quantized.length; h++) {
      double xi = (double) (quantized[h] & 0xFF) * step + lowerInterval;
      dequantized[h] = (float) (xi + centroid[h]);
    }
    return dequantized;
  }

  /**
   * Compute the loss of the vector given the interval. Effectively, we are computing the MSE of a
   * dequantized vector with the raw vector.
   *
   * @param vector raw vector
   * @param interval interval to quantize the vector
   * @param points number of quantization points
   * @param norm2 squared norm of the vector
   * @return the loss
   */
  private double loss(float[] vector, float[] interval, int points, float norm2) {
    double a = interval[0];
    double b = interval[1];
    double step = ((b - a) / (points - 1.0F));
    double stepInv = 1.0 / step;
    double xe = 0.0;
    double e = 0.0;
    for (double xi : vector) {
      // this is quantizing and then dequantizing the vector
      double xiq = (a + step * Math.round((clamp(xi, a, b) - a) * stepInv));
      // how much does the de-quantized value differ from the original value
      xe += xi * (xi - xiq);
      e += (xi - xiq) * (xi - xiq);
    }
    return (1.0 - lambda) * xe * xe / norm2 + lambda * e;
  }

  /**
   * Optimize the quantization interval for the given vector. This is done via a coordinate descent
   * trying to minimize the quantization loss. Note, the loss is not always guaranteed to decrease,
   * so we have a maximum number of iterations and will exit early if the loss increases.
   *
   * @param initInterval initial interval, the optimized interval will be stored here
   * @param vector raw vector
   * @param norm2 squared norm of the vector
   * @param points number of quantization points
   */
  private void optimizeIntervals(float[] initInterval, float[] vector, float norm2, int points) {
    double initialLoss = loss(vector, initInterval, points, norm2);
    final float scale = (1.0f - lambda) / norm2;
    if (Float.isFinite(scale) == false) {
      return;
    }
    for (int i = 0; i < iters; ++i) {
      float a = initInterval[0];
      float b = initInterval[1];
      float stepInv = (points - 1.0f) / (b - a);
      // calculate the grid points for coordinate descent
      double daa = 0;
      double dab = 0;
      double dbb = 0;
      double dax = 0;
      double dbx = 0;
      for (float xi : vector) {
        float k = Math.round((clamp(xi, a, b) - a) * stepInv);
        float s = k / (points - 1);
        daa += (1.0 - s) * (1.0 - s);
        dab += (1.0 - s) * s;
        dbb += s * s;
        dax += xi * (1.0 - s);
        dbx += xi * s;
      }
      double m0 = scale * dax * dax + lambda * daa;
      double m1 = scale * dax * dbx + lambda * dab;
      double m2 = scale * dbx * dbx + lambda * dbb;
      // its possible that the determinant is 0, in which case we can't update the interval
      double det = m0 * m2 - m1 * m1;
      if (det == 0) {
        return;
      }
      float aOpt = (float) ((m2 * dax - m1 * dbx) / det);
      float bOpt = (float) ((m0 * dbx - m1 * dax) / det);
      // If there is no change in the interval, we can stop
      if ((Math.abs(initInterval[0] - aOpt) < 1e-8 && Math.abs(initInterval[1] - bOpt) < 1e-8)) {
        return;
      }
      double newLoss = loss(vector, new float[] {aOpt, bOpt}, points, norm2);
      // If the new loss is worse, don't update the interval and exit
      // This optimization, unlike kMeans, does not always converge to better loss
      // So exit if we are getting worse
      if (newLoss > initialLoss) {
        return;
      }
      // Update the interval and go again
      initInterval[0] = aOpt;
      initInterval[1] = bOpt;
      initialLoss = newLoss;
    }
  }

  public static int discretize(int value, int bucket) {
    return ((value + (bucket - 1)) / bucket) * bucket;
  }

  /**
   * Transpose the query vector into a byte array allowing for efficient bitwise operations with the
   * index bit vectors. The idea here is to organize the query vector bits such that the first bit
   * of every dimension is in the first set dimensions bits, or (dimensions/8) bytes. The second,
   * third, and fourth bits are in the second, third, and fourth set of dimensions bits,
   * respectively. This allows for direct bitwise comparisons with the stored index vectors through
   * summing the bitwise results with the relative required bit shifts.
   *
   * <p>This bit decomposition for fast bitwise SIMD operations was first proposed in:
   *
   * <pre><code class="language-java">
   *   Gao, Jianyang, and Cheng Long. "RaBitQ: Quantizing High-
   *   Dimensional Vectors with a Theoretical Error Bound for Approximate Nearest Neighbor Search."
   *   Proceedings of the ACM on Management of Data 2, no. 3 (2024): 1-27.
   *   </code></pre>
   *
   * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
   * @param quantQueryByte the byte array to store the transposed query vector
   */
  public static void transposeHalfByte(byte[] q, byte[] quantQueryByte) {
    for (int i = 0; i < q.length; ) {
      assert q[i] >= 0 && q[i] <= 15;
      int lowerByte = 0;
      int lowerMiddleByte = 0;
      int upperMiddleByte = 0;
      int upperByte = 0;
      for (int j = 7; j >= 0 && i < q.length; j--) {
        lowerByte |= (q[i] & 1) << j;
        lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
        upperMiddleByte |= ((q[i] >> 2) & 1) << j;
        upperByte |= ((q[i] >> 3) & 1) << j;
        i++;
      }
      int index = ((i + 7) / 8) - 1;
      quantQueryByte[index] = (byte) lowerByte;
      quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
      quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
      quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
    }
  }

  /**
   * Pack the vector as a binary array.
   *
   * @param vector the vector to pack
   * @param packed the packed vector
   */
  public static void packAsBinary(byte[] vector, byte[] packed) {
    for (int i = 0; i < vector.length; ) {
      byte result = 0;
      for (int j = 7; j >= 0 && i < vector.length; j--) {
        assert vector[i] == 0 || vector[i] == 1;
        result |= (byte) ((vector[i] & 1) << j);
        ++i;
      }
      int index = ((i + 7) / 8) - 1;
      assert index < packed.length;
      packed[index] = result;
    }
  }

  /**
   * Unpack a binary array back to a vector.
   *
   * @param packed the packed binary array
   * @param vector the unpacked vector (each byte will be 0 or 1)
   */
  public static void unpackBinary(byte[] packed, byte[] vector) {
    int vectorIndex = 0;
    for (int packedIndex = 0;
        packedIndex < packed.length && vectorIndex < vector.length;
        packedIndex++) {
      byte packedByte = packed[packedIndex];
      for (int j = 7; j >= 0 && vectorIndex < vector.length; j--) {
        vector[vectorIndex] = (byte) ((packedByte >> j) & 1);
        vectorIndex++;
      }
    }
  }

  /**
   * Transpose a 2-bit (dibit) quantized vector into a byte array for efficient bitwise operations.
   * The result has 2 stripes: similar to {@link #transposeHalfByte}, but only for 2 bits
   *
   * @param vector the 2-bit quantized vector (values 0-3)
   * @param packed the byte array to store the transposed vector
   */
  public static void transposeDibit(byte[] vector, byte[] packed) {
    int limit = vector.length - 7;
    int i = 0;
    int index = 0;
    for (; i < limit; i += 8, index++) {
      int lowerByte =
          (vector[i] & 1) << 7
              | (vector[i + 1] & 1) << 6
              | (vector[i + 2] & 1) << 5
              | (vector[i + 3] & 1) << 4
              | (vector[i + 4] & 1) << 3
              | (vector[i + 5] & 1) << 2
              | (vector[i + 6] & 1) << 1
              | (vector[i + 7] & 1);
      int upperByte =
          ((vector[i] >> 1) & 1) << 7
              | ((vector[i + 1] >> 1) & 1) << 6
              | ((vector[i + 2] >> 1) & 1) << 5
              | ((vector[i + 3] >> 1) & 1) << 4
              | ((vector[i + 4] >> 1) & 1) << 3
              | ((vector[i + 5] >> 1) & 1) << 2
              | ((vector[i + 6] >> 1) & 1) << 1
              | ((vector[i + 7] >> 1) & 1);
      packed[index] = (byte) lowerByte;
      packed[index + packed.length / 2] = (byte) upperByte;
    }
    if (i == vector.length) {
      return;
    }
    int lowerByte = 0;
    int upperByte = 0;
    for (int j = 7; i < vector.length; j--, i++) {
      assert vector[i] >= 0 && vector[i] <= 3;
      lowerByte |= (vector[i] & 1) << j;
      upperByte |= ((vector[i] >> 1) & 1) << j;
    }
    packed[index] = (byte) lowerByte;
    packed[index + packed.length / 2] = (byte) upperByte;
  }

  /**
   * Untranspose a packed 2-bit (dibit) vector back to its original form. This is the reverse of
   * {@link #transposeDibit}.
   *
   * @param packed the packed/transposed byte array
   * @param vector the output vector where each byte will contain a 2-bit value (0-3)
   */
  public static void untransposeDibit(byte[] packed, byte[] vector) {
    int stripeSize = packed.length / 2;
    int limit = vector.length - 7;
    int i = 0;
    int index = 0;
    for (; i < limit; i += 8, index++) {
      byte lowerByte = packed[index];
      byte upperByte = packed[index + stripeSize];
      vector[i] = (byte) (((lowerByte >> 7) & 1) | (((upperByte >> 7) & 1) << 1));
      vector[i + 1] = (byte) (((lowerByte >> 6) & 1) | (((upperByte >> 6) & 1) << 1));
      vector[i + 2] = (byte) (((lowerByte >> 5) & 1) | (((upperByte >> 5) & 1) << 1));
      vector[i + 3] = (byte) (((lowerByte >> 4) & 1) | (((upperByte >> 4) & 1) << 1));
      vector[i + 4] = (byte) (((lowerByte >> 3) & 1) | (((upperByte >> 3) & 1) << 1));
      vector[i + 5] = (byte) (((lowerByte >> 2) & 1) | (((upperByte >> 2) & 1) << 1));
      vector[i + 6] = (byte) (((lowerByte >> 1) & 1) | (((upperByte >> 1) & 1) << 1));
      vector[i + 7] = (byte) ((lowerByte & 1) | ((upperByte & 1) << 1));
    }
    if (i < vector.length) {
      byte lowerByte = packed[index];
      byte upperByte = packed[index + stripeSize];
      for (int j = 7; i < vector.length; j--, i++) {
        vector[i] = (byte) (((lowerByte >> j) & 1) | (((upperByte >> j) & 1) << 1));
      }
    }
  }

  private static double clamp(double x, double a, double b) {
    return Math.min(Math.max(x, a), b);
  }
}
