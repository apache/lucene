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

/** Utilities for computations with numeric arrays */
public final class VectorUtil {

  private VectorUtil() {}

  /**
   * Returns the vector dot product of the two vectors.
   *
   * @throws IllegalArgumentException if the vectors' dimensions differ.
   */
  public static float dotProduct(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    float res = 0f;
    /*
     * If length of vector is larger than 8, we use unrolled dot product to accelerate the
     * calculation.
     */
    int i;
    for (i = 0; i < a.length % 8; i++) {
      res += b[i] * a[i];
    }
    if (a.length < 8) {
      return res;
    }
    for (; i + 31 < a.length; i += 32) {
      res +=
          b[i + 0] * a[i + 0]
              + b[i + 1] * a[i + 1]
              + b[i + 2] * a[i + 2]
              + b[i + 3] * a[i + 3]
              + b[i + 4] * a[i + 4]
              + b[i + 5] * a[i + 5]
              + b[i + 6] * a[i + 6]
              + b[i + 7] * a[i + 7];
      res +=
          b[i + 8] * a[i + 8]
              + b[i + 9] * a[i + 9]
              + b[i + 10] * a[i + 10]
              + b[i + 11] * a[i + 11]
              + b[i + 12] * a[i + 12]
              + b[i + 13] * a[i + 13]
              + b[i + 14] * a[i + 14]
              + b[i + 15] * a[i + 15];
      res +=
          b[i + 16] * a[i + 16]
              + b[i + 17] * a[i + 17]
              + b[i + 18] * a[i + 18]
              + b[i + 19] * a[i + 19]
              + b[i + 20] * a[i + 20]
              + b[i + 21] * a[i + 21]
              + b[i + 22] * a[i + 22]
              + b[i + 23] * a[i + 23];
      res +=
          b[i + 24] * a[i + 24]
              + b[i + 25] * a[i + 25]
              + b[i + 26] * a[i + 26]
              + b[i + 27] * a[i + 27]
              + b[i + 28] * a[i + 28]
              + b[i + 29] * a[i + 29]
              + b[i + 30] * a[i + 30]
              + b[i + 31] * a[i + 31];
    }
    for (; i + 7 < a.length; i += 8) {
      res +=
          b[i + 0] * a[i + 0]
              + b[i + 1] * a[i + 1]
              + b[i + 2] * a[i + 2]
              + b[i + 3] * a[i + 3]
              + b[i + 4] * a[i + 4]
              + b[i + 5] * a[i + 5]
              + b[i + 6] * a[i + 6]
              + b[i + 7] * a[i + 7];
    }
    return res;
  }

  /**
   * Returns the cosine similarity between the two vectors.
   *
   * @throws IllegalArgumentException if the vectors' dimensions differ.
   */
  public static float cosine(float[] v1, float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "vector dimensions differ: " + v1.length + "!=" + v2.length);
    }

    float sum = 0.0f;
    float norm1 = 0.0f;
    float norm2 = 0.0f;
    int dim = v1.length;

    for (int i = 0; i < dim; i++) {
      float elem1 = v1[i];
      float elem2 = v2[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt(norm1 * norm2));
  }

  /** Returns the cosine similarity between the two vectors. */
  public static float cosine(byte[] a, byte[] b) {
    // Note: this will not overflow if dim < 2^18, since max(byte * byte) = 2^14.
    int sum = 0;
    int norm1 = 0;
    int norm2 = 0;

    for (int i = 0; i < a.length; i++) {
      byte elem1 = a[i];
      byte elem2 = b[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  /**
   * Returns the sum of squared differences of the two vectors.
   *
   * @throws IllegalArgumentException if the vectors' dimensions differ.
   */
  public static float squareDistance(float[] v1, float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "vector dimensions differ: " + v1.length + "!=" + v2.length);
    }
    float squareSum = 0.0f;
    int dim = v1.length;
    int i;
    for (i = 0; i + 8 <= dim; i += 8) {
      squareSum += squareDistanceUnrolled(v1, v2, i);
    }
    for (; i < dim; i++) {
      float diff = v1[i] - v2[i];
      squareSum += diff * diff;
    }
    return squareSum;
  }

  private static float squareDistanceUnrolled(float[] v1, float[] v2, int index) {
    float diff0 = v1[index + 0] - v2[index + 0];
    float diff1 = v1[index + 1] - v2[index + 1];
    float diff2 = v1[index + 2] - v2[index + 2];
    float diff3 = v1[index + 3] - v2[index + 3];
    float diff4 = v1[index + 4] - v2[index + 4];
    float diff5 = v1[index + 5] - v2[index + 5];
    float diff6 = v1[index + 6] - v2[index + 6];
    float diff7 = v1[index + 7] - v2[index + 7];
    return diff0 * diff0
        + diff1 * diff1
        + diff2 * diff2
        + diff3 * diff3
        + diff4 * diff4
        + diff5 * diff5
        + diff6 * diff6
        + diff7 * diff7;
  }

  /** Returns the sum of squared differences of the two vectors. */
  public static int squareDistance(byte[] a, byte[] b) {
    // Note: this will not overflow if dim < 2^18, since max(byte * byte) = 2^14.
    int squareSum = 0;
    for (int i = 0; i < a.length; i++) {
      int diff = a[i] - b[i];
      squareSum += diff * diff;
    }
    return squareSum;
  }

  /**
   * Modifies the argument to be unit length, dividing by its l2-norm. IllegalArgumentException is
   * thrown for zero vectors.
   *
   * @return the input array after normalization
   */
  public static float[] l2normalize(float[] v) {
    l2normalize(v, true);
    return v;
  }

  /**
   * Modifies the argument to be unit length, dividing by its l2-norm.
   *
   * @param v the vector to normalize
   * @param throwOnZero whether to throw an exception when <code>v</code> has all zeros
   * @return the input array after normalization
   * @throws IllegalArgumentException when the vector is all zero and throwOnZero is true
   */
  public static float[] l2normalize(float[] v, boolean throwOnZero) {
    double squareSum = 0.0f;
    int dim = v.length;
    for (float x : v) {
      squareSum += x * x;
    }
    if (squareSum == 0) {
      if (throwOnZero) {
        throw new IllegalArgumentException("Cannot normalize a zero-length vector");
      } else {
        return v;
      }
    }
    double length = Math.sqrt(squareSum);
    for (int i = 0; i < dim; i++) {
      v[i] /= length;
    }
    return v;
  }

  /**
   * Adds the second argument to the first
   *
   * @param u the destination
   * @param v the vector to add to the destination
   */
  public static void add(float[] u, float[] v) {
    for (int i = 0; i < u.length; i++) {
      u[i] += v[i];
    }
  }

  /**
   * Dot product computed over signed bytes.
   *
   * @param a bytes containing a vector
   * @param b bytes containing another vector, of the same dimension
   * @return the value of the dot product of the two vectors
   */
  public static int dotProduct(byte[] a, byte[] b) {
    assert a.length == b.length;
    int total = 0;
    for (int i = 0; i < a.length; i++) {
      total += a[i] * b[i];
    }
    return total;
  }

  /**
   * Dot product score computed over signed bytes, scaled to be in [0, 1].
   *
   * @param a bytes containing a vector
   * @param b bytes containing another vector, of the same dimension
   * @return the value of the similarity function applied to the two vectors
   */
  public static float dotProductScore(byte[] a, byte[] b) {
    // divide by 2 * 2^14 (maximum absolute value of product of 2 signed bytes) * len
    float denom = (float) (a.length * (1 << 15));
    return 0.5f + dotProduct(a, b) / denom;
  }
}
