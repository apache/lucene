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

/** The default VectorUtil provider implementation. */
final class VectorUtilDefaultProvider implements VectorUtilProvider {

  VectorUtilDefaultProvider() {}

  @Override
  public float dotProduct(float[] a, float[] b) {
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

  @Override
  public float cosine(float[] a, float[] b) {
    float sum = 0.0f;
    float norm1 = 0.0f;
    float norm2 = 0.0f;
    int dim = a.length;

    for (int i = 0; i < dim; i++) {
      float elem1 = a[i];
      float elem2 = b[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    float squareSum = 0.0f;
    int dim = a.length;
    int i;
    for (i = 0; i + 8 <= dim; i += 8) {
      squareSum += squareDistanceUnrolled(a, b, i);
    }
    for (; i < dim; i++) {
      float diff = a[i] - b[i];
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

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    int total = 0;
    for (int i = 0; i < a.length; i++) {
      total += a[i] * b[i];
    }
    return total;
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
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

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    // Note: this will not overflow if dim < 2^18, since max(byte * byte) = 2^14.
    int squareSum = 0;
    for (int i = 0; i < a.length; i++) {
      int diff = a[i] - b[i];
      squareSum += diff * diff;
    }
    return squareSum;
  }
}
