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
package org.apache.lucene.util;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Will scalar quantize float vectors into `int8` byte values. This is a lossy transformation.
 * Scalar quantization works by first calculating the quantiles of the float vector values. The
 * quantiles are calculated using the configured confidence interval. The [minQuantile, maxQuantile]
 * are then used to scale the values into the range [0, 127] and bucketed into the nearest byte
 * values.
 *
 * <h2>How Scalar Quantization Works</h2>
 *
 * <p>The basic mathematical equations behind this are fairly straight forward and based on min/max
 * normalization. Given a float vector `v` and a confidenceInterval `q` we can calculate the
 * quantiles of the vector values [minQuantile, maxQuantile].
 *
 * <pre class="prettyprint">
 *   byte = (float - minQuantile) * 127/(maxQuantile - minQuantile)
 *   float = (maxQuantile - minQuantile)/127 * byte + minQuantile
 * </pre>
 *
 * <p>This then means to multiply two float values together (e.g. dot_product) we can do the
 * following:
 *
 * <pre class="prettyprint">
 *   float1 * float2 ~= (byte1 * (maxQuantile - minQuantile)/127 + minQuantile) * (byte2 * (maxQuantile - minQuantile)/127 + minQuantile)
 *   float1 * float2 ~= (byte1 * byte2 * (maxQuantile - minQuantile)^2)/(127^2) + (byte1 * minQuantile * (maxQuantile - minQuantile)/127) + (byte2 * minQuantile * (maxQuantile - minQuantile)/127) + minQuantile^2
 *   let alpha = (maxQuantile - minQuantile)/127
 *   float1 * float2 ~= (byte1 * byte2 * alpha^2) + (byte1 * minQuantile * alpha) + (byte2 * minQuantile * alpha) + minQuantile^2
 * </pre>
 *
 * <p>The expansion for square distance is much simpler:
 *
 * <pre class="prettyprint">
 *  square_distance = (float1 - float2)^2
 *  (float1 - float2)^2 ~= (byte1 * alpha + minQuantile - byte2 * alpha - minQuantile)^2
 *  = (alpha*byte1 + minQuantile)^2 + (alpha*byte2 + minQuantile)^2 - 2*(alpha*byte1 + minQuantile)(alpha*byte2 + minQuantile)
 *  this can be simplified to:
 *  = alpha^2 (byte1 - byte2)^2
 * </pre>
 */
public class ScalarQuantizer {

  public static final int SCALAR_QUANTIZATION_SAMPLE_SIZE = 25_000;

  private final float alpha;
  private final float scale;
  private final float minQuantile, maxQuantile, confidenceInterval;

  /**
   * @param minQuantile the lower quantile of the distribution
   * @param maxQuantile the upper quantile of the distribution
   * @param confidenceInterval The configured confidence interval used to calculate the quantiles.
   */
  public ScalarQuantizer(float minQuantile, float maxQuantile, float confidenceInterval) {
    assert maxQuantile >= minQuantile;
    this.minQuantile = minQuantile;
    this.maxQuantile = maxQuantile;
    this.scale = 127f / (maxQuantile - minQuantile);
    this.alpha = (maxQuantile - minQuantile) / 127f;
    this.confidenceInterval = confidenceInterval;
  }

  /**
   * Quantize a float vector into a byte vector
   *
   * @param src the source vector
   * @param dest the destination vector
   * @param similarityFunction the similarity function used to calculate the quantile
   * @return the corrective offset that needs to be applied to the score
   */
  public float quantize(float[] src, byte[] dest, VectorSimilarityFunction similarityFunction) {
    assert src.length == dest.length;
    float correctiveOffset = 0f;
    for (int i = 0; i < src.length; i++) {
      float v = src[i];
      // Make sure the value is within the quantile range, cutting off the tails
      // see first parenthesis in equation: byte = (float - minQuantile) * 127/(maxQuantile -
      // minQuantile)
      float dx = Math.max(minQuantile, Math.min(maxQuantile, src[i])) - minQuantile;
      // Scale the value to the range [0, 127], this is our quantized value
      // scale = 127/(maxQuantile - minQuantile)
      float dxs = scale * dx;
      // We multiply by `alpha` here to get the quantized value back into the original range
      // to aid in calculating the corrective offset
      float dxq = Math.round(dxs) * alpha;
      // Calculate the corrective offset that needs to be applied to the score
      // in addition to the `byte * minQuantile * alpha` term in the equation
      // we add the `(dx - dxq) * dxq` term to account for the fact that the quantized value
      // will be rounded to the nearest whole number and lose some accuracy
      // Additionally, we account for the global correction of `minQuantile^2` in the equation
      correctiveOffset += minQuantile * (v - minQuantile / 2.0F) + (dx - dxq) * dxq;
      dest[i] = (byte) Math.round(dxs);
    }
    if (similarityFunction.equals(VectorSimilarityFunction.EUCLIDEAN)) {
      return 0;
    }
    return correctiveOffset;
  }

  /**
   * Recalculate the old score corrective value given new current quantiles
   *
   * @param quantizedVector the old vector
   * @param oldQuantizer the old quantizer
   * @param similarityFunction the similarity function used to calculate the quantile
   * @return the new offset
   */
  public float recalculateCorrectiveOffset(
      byte[] quantizedVector,
      ScalarQuantizer oldQuantizer,
      VectorSimilarityFunction similarityFunction) {
    if (similarityFunction.equals(VectorSimilarityFunction.EUCLIDEAN)) {
      return 0f;
    }
    float correctiveOffset = 0f;
    for (int i = 0; i < quantizedVector.length; i++) {
      // dequantize the old value in order to recalculate the corrective offset
      float v = (oldQuantizer.alpha * quantizedVector[i]) + oldQuantizer.minQuantile;
      float dx = Math.max(minQuantile, Math.min(maxQuantile, v)) - minQuantile;
      float dxs = scale * dx;
      float dxq = Math.round(dxs) * alpha;
      correctiveOffset += minQuantile * (v - minQuantile / 2.0F) + (dx - dxq) * dxq;
    }
    return correctiveOffset;
  }

  /**
   * Dequantize a byte vector into a float vector
   *
   * @param src the source vector
   * @param dest the destination vector
   */
  public void deQuantize(byte[] src, float[] dest) {
    assert src.length == dest.length;
    for (int i = 0; i < src.length; i++) {
      dest[i] = (alpha * src[i]) + minQuantile;
    }
  }

  public float getLowerQuantile() {
    return minQuantile;
  }

  public float getUpperQuantile() {
    return maxQuantile;
  }

  public float getConfidenceInterval() {
    return confidenceInterval;
  }

  public float getConstantMultiplier() {
    return alpha * alpha;
  }

  @Override
  public String toString() {
    return "ScalarQuantizer{"
        + "minQuantile="
        + minQuantile
        + ", maxQuantile="
        + maxQuantile
        + ", confidenceInterval="
        + confidenceInterval
        + '}';
  }

  private static final Random random = new Random(42);

  /**
   * This will read the float vector values and calculate the quantiles. If the number of float
   * vectors is less than {@link #SCALAR_QUANTIZATION_SAMPLE_SIZE} then all the values will be read
   * and the quantiles calculated. If the number of float vectors is greater than {@link
   * #SCALAR_QUANTIZATION_SAMPLE_SIZE} then a random sample of {@link
   * #SCALAR_QUANTIZATION_SAMPLE_SIZE} will be read and the quantiles calculated.
   *
   * @param floatVectorValues the float vector values from which to calculate the quantiles
   * @param confidenceInterval the confidence interval used to calculate the quantiles
   * @return A new {@link ScalarQuantizer} instance
   * @throws IOException if there is an error reading the float vector values
   */
  public static ScalarQuantizer fromVectors(
      FloatVectorValues floatVectorValues, float confidenceInterval) throws IOException {
    assert 0.9f <= confidenceInterval && confidenceInterval <= 1f;
    if (floatVectorValues.size() == 0) {
      return new ScalarQuantizer(0f, 0f, confidenceInterval);
    }
    if (confidenceInterval == 1f) {
      float min = Float.POSITIVE_INFINITY;
      float max = Float.NEGATIVE_INFINITY;
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        for (float v : floatVectorValues.vectorValue()) {
          min = Math.min(min, v);
          max = Math.max(max, v);
        }
      }
      return new ScalarQuantizer(min, max, confidenceInterval);
    }
    int dim = floatVectorValues.dimension();
    if (floatVectorValues.size() < SCALAR_QUANTIZATION_SAMPLE_SIZE) {
      int copyOffset = 0;
      float[] values = new float[floatVectorValues.size() * dim];
      while (floatVectorValues.nextDoc() != NO_MORE_DOCS) {
        float[] floatVector = floatVectorValues.vectorValue();
        System.arraycopy(floatVector, 0, values, copyOffset, floatVector.length);
        copyOffset += dim;
      }
      float[] upperAndLower = getUpperAndLowerQuantile(values, confidenceInterval);
      return new ScalarQuantizer(upperAndLower[0], upperAndLower[1], confidenceInterval);
    }
    int numFloatVecs = floatVectorValues.size();
    // Reservoir sample the vector ordinals we want to read
    float[] values = new float[SCALAR_QUANTIZATION_SAMPLE_SIZE * dim];
    int[] vectorsToTake = IntStream.range(0, SCALAR_QUANTIZATION_SAMPLE_SIZE).toArray();
    for (int i = SCALAR_QUANTIZATION_SAMPLE_SIZE; i < numFloatVecs; i++) {
      int j = random.nextInt(i + 1);
      if (j < SCALAR_QUANTIZATION_SAMPLE_SIZE) {
        vectorsToTake[j] = i;
      }
    }
    Arrays.sort(vectorsToTake);
    int copyOffset = 0;
    int index = 0;
    for (int i : vectorsToTake) {
      while (index <= i) {
        // We cannot use `advance(docId)` as MergedVectorValues does not support it
        floatVectorValues.nextDoc();
        index++;
      }
      assert floatVectorValues.docID() != NO_MORE_DOCS;
      float[] floatVector = floatVectorValues.vectorValue();
      System.arraycopy(floatVector, 0, values, copyOffset, floatVector.length);
      copyOffset += dim;
    }
    float[] upperAndLower = getUpperAndLowerQuantile(values, confidenceInterval);
    return new ScalarQuantizer(upperAndLower[0], upperAndLower[1], confidenceInterval);
  }

  /**
   * Takes an array of floats, sorted or not, and returns a minimum and maximum value. These values
   * are such that they reside on the `(1 - confidenceInterval)/2` and `confidenceInterval/2`
   * percentiles. Example: providing floats `[0..100]` and asking for `90` quantiles will return `5`
   * and `95`.
   *
   * @param arr array of floats
   * @param confidenceInterval the configured confidence interval
   * @return lower and upper quantile values
   */
  static float[] getUpperAndLowerQuantile(float[] arr, float confidenceInterval) {
    assert 0.9f <= confidenceInterval && confidenceInterval <= 1f;
    int selectorIndex = (int) (arr.length * (1f - confidenceInterval) / 2f + 0.5f);
    if (selectorIndex > 0) {
      Selector selector = new FloatSelector(arr);
      selector.select(0, arr.length, arr.length - selectorIndex);
      selector.select(0, arr.length - selectorIndex, selectorIndex);
    }
    float min = Float.POSITIVE_INFINITY;
    float max = Float.NEGATIVE_INFINITY;
    for (int i = selectorIndex; i < arr.length - selectorIndex; i++) {
      min = Math.min(arr[i], min);
      max = Math.max(arr[i], max);
    }
    return new float[] {min, max};
  }

  private static class FloatSelector extends IntroSelector {
    float pivot = Float.NaN;

    private final float[] arr;

    private FloatSelector(float[] arr) {
      this.arr = arr;
    }

    @Override
    protected void setPivot(int i) {
      pivot = arr[i];
    }

    @Override
    protected int comparePivot(int j) {
      return Float.compare(pivot, arr[j]);
    }

    @Override
    protected void swap(int i, int j) {
      final float tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
  }
}
