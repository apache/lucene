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

package org.apache.lucene.internal.vectorization;

import static org.apache.lucene.util.VectorUtil.EPSILON;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

final class DefaultVectorUtilSupport implements VectorUtilSupport {

  DefaultVectorUtilSupport() {}

  // the way FMA should work! if available use it, otherwise fall back to mul/add
  @SuppressForbidden(reason = "Uses FMA only where fast and carefully contained")
  private static float fma(float a, float b, float c) {
    if (Constants.HAS_FAST_SCALAR_FMA) {
      return Math.fma(a, b, c);
    } else {
      return a * b + c;
    }
  }

  @Override
  public float dotProduct(float[] a, float[] b) {
    float res = 0f;
    int i = 0;

    // if the array is big, unroll it
    if (a.length > 32) {
      float acc1 = 0;
      float acc2 = 0;
      float acc3 = 0;
      float acc4 = 0;
      int upperBound = a.length & ~(4 - 1);
      for (; i < upperBound; i += 4) {
        acc1 = fma(a[i], b[i], acc1);
        acc2 = fma(a[i + 1], b[i + 1], acc2);
        acc3 = fma(a[i + 2], b[i + 2], acc3);
        acc4 = fma(a[i + 3], b[i + 3], acc4);
      }
      res += acc1 + acc2 + acc3 + acc4;
    }

    for (; i < a.length; i++) {
      res = fma(a[i], b[i], res);
    }
    return res;
  }

  @Override
  public float cosine(float[] a, float[] b) {
    float sum = 0.0f;
    float norm1 = 0.0f;
    float norm2 = 0.0f;
    int i = 0;

    // if the array is big, unroll it
    if (a.length > 32) {
      float sum1 = 0;
      float sum2 = 0;
      float norm1_1 = 0;
      float norm1_2 = 0;
      float norm2_1 = 0;
      float norm2_2 = 0;

      int upperBound = a.length & ~(2 - 1);
      for (; i < upperBound; i += 2) {
        // one
        sum1 = fma(a[i], b[i], sum1);
        norm1_1 = fma(a[i], a[i], norm1_1);
        norm2_1 = fma(b[i], b[i], norm2_1);

        // two
        sum2 = fma(a[i + 1], b[i + 1], sum2);
        norm1_2 = fma(a[i + 1], a[i + 1], norm1_2);
        norm2_2 = fma(b[i + 1], b[i + 1], norm2_2);
      }
      sum += sum1 + sum2;
      norm1 += norm1_1 + norm1_2;
      norm2 += norm2_1 + norm2_2;
    }

    for (; i < a.length; i++) {
      sum = fma(a[i], b[i], sum);
      norm1 = fma(a[i], a[i], norm1);
      norm2 = fma(b[i], b[i], norm2);
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    float res = 0;
    int i = 0;

    // if the array is big, unroll it
    if (a.length > 32) {
      float acc1 = 0;
      float acc2 = 0;
      float acc3 = 0;
      float acc4 = 0;

      int upperBound = a.length & ~(4 - 1);
      for (; i < upperBound; i += 4) {
        // one
        float diff1 = a[i] - b[i];
        acc1 = fma(diff1, diff1, acc1);

        // two
        float diff2 = a[i + 1] - b[i + 1];
        acc2 = fma(diff2, diff2, acc2);

        // three
        float diff3 = a[i + 2] - b[i + 2];
        acc3 = fma(diff3, diff3, acc3);

        // four
        float diff4 = a[i + 3] - b[i + 3];
        acc4 = fma(diff4, diff4, acc4);
      }
      res += acc1 + acc2 + acc3 + acc4;
    }

    for (; i < a.length; i++) {
      float diff = a[i] - b[i];
      res = fma(diff, diff, res);
    }
    return res;
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
  public int uint8DotProduct(byte[] a, byte[] b) {
    int total = 0;
    for (int i = 0; i < a.length; i++) {
      total += Byte.toUnsignedInt(a[i]) * Byte.toUnsignedInt(b[i]);
    }
    return total;
  }

  @Override
  public int int4DotProduct(byte[] a, byte[] b) {
    return dotProduct(a, b);
  }

  @Override
  public int int4DotProductSinglePacked(byte[] unpacked, byte[] packed) {
    int total = 0;
    for (int i = 0; i < packed.length; i++) {
      byte packedByte = packed[i];
      byte unpacked1 = unpacked[i];
      byte unpacked2 = unpacked[i + packed.length];
      total += (packedByte & 0x0F) * unpacked2;
      total += ((packedByte & 0xFF) >> 4) * unpacked1;
    }
    return total;
  }

  @Override
  public int int4DotProductBothPacked(byte[] a, byte[] b) {
    int total = 0;
    for (int i = 0; i < a.length; i++) {
      byte aByte = a[i];
      byte bByte = b[i];
      total += (aByte & 0x0F) * (bByte & 0x0F);
      total += ((aByte & 0xFF) >> 4) * ((bByte & 0xFF) >> 4);
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

  @Override
  public int int4SquareDistance(byte[] a, byte[] b) {
    return squareDistance(a, b);
  }

  @Override
  public int int4SquareDistanceSinglePacked(byte[] unpacked, byte[] packed) {
    int total = 0;
    for (int i = 0; i < packed.length; i++) {
      byte packedByte = packed[i];
      byte unpacked1 = unpacked[i];
      byte unpacked2 = unpacked[i + packed.length];

      int diff1 = (packedByte & 0x0F) - unpacked2;
      int diff2 = ((packedByte & 0xFF) >> 4) - unpacked1;

      total += diff1 * diff1 + diff2 * diff2;
    }
    return total;
  }

  @Override
  public int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    int total = 0;
    for (int i = 0; i < a.length; i++) {
      byte aByte = a[i];
      byte bByte = b[i];

      int diff1 = (aByte & 0x0F) - (bByte & 0x0F);
      int diff2 = ((aByte & 0xFF) >> 4) - ((bByte & 0xFF) >> 4);

      total += diff1 * diff1 + diff2 * diff2;
    }
    return total;
  }

  @Override
  public int uint8SquareDistance(byte[] a, byte[] b) {
    // Note: this will not overflow if dim < 2^16, since max(ubyte * ubyte) = 2^16.
    int squareSum = 0;
    for (int i = 0; i < a.length; i++) {
      int diff = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
      squareSum += diff * diff;
    }
    return squareSum;
  }

  @Override
  public int findNextGEQ(int[] buffer, int target, int from, int to) {
    for (int i = from; i < to; ++i) {
      if (buffer[i] >= target) {
        return i;
      }
    }
    return to;
  }

  @Override
  public long int4BitDotProduct(byte[] int4Quantized, byte[] binaryQuantized) {
    return int4BitDotProductImpl(int4Quantized, binaryQuantized);
  }

  public static long int4BitDotProductImpl(byte[] q, byte[] d) {
    assert q.length == d.length * 4;
    long ret = 0;
    int size = d.length;
    for (int i = 0; i < 4; i++) {
      int r = 0;
      long subRet = 0;
      for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
        subRet +=
            Integer.bitCount(
                (int) BitUtil.VH_NATIVE_INT.get(q, i * size + r)
                    & (int) BitUtil.VH_NATIVE_INT.get(d, r));
      }
      for (; r < d.length; r++) {
        subRet += Integer.bitCount((q[i * size + r] & d[r]) & 0xFF);
      }
      ret += subRet << i;
    }
    return ret;
  }

  @Override
  public float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile) {
    return new ScalarQuantizer(alpha, scale, minQuantile, maxQuantile).quantize(vector, dest, 0);
  }

  @Override
  public float recalculateScalarQuantizationOffset(
      byte[] vector,
      float oldAlpha,
      float oldMinQuantile,
      float scale,
      float alpha,
      float minQuantile,
      float maxQuantile) {
    return new ScalarQuantizer(alpha, scale, minQuantile, maxQuantile)
        .recalculateOffset(vector, 0, oldAlpha, oldMinQuantile);
  }

  static class ScalarQuantizer {
    private final float alpha;
    private final float scale;
    private final float minQuantile, maxQuantile;

    ScalarQuantizer(float alpha, float scale, float minQuantile, float maxQuantile) {
      this.alpha = alpha;
      this.scale = scale;
      this.minQuantile = minQuantile;
      this.maxQuantile = maxQuantile;
    }

    float quantize(float[] vector, byte[] dest, int start) {
      assert vector.length == dest.length;
      float correction = 0;
      for (int i = start; i < vector.length; i++) {
        correction += quantizeFloat(vector[i], dest, i);
      }
      return correction;
    }

    float recalculateOffset(byte[] vector, int start, float oldAlpha, float oldMinQuantile) {
      float correction = 0;
      for (int i = start; i < vector.length; i++) {
        // undo the old quantization
        float v = (oldAlpha * Byte.toUnsignedInt(vector[i])) + oldMinQuantile;
        correction += quantizeFloat(v, null, 0);
      }
      return correction;
    }

    private float quantizeFloat(float v, byte[] dest, int destIndex) {
      assert dest == null || destIndex < dest.length;
      // Make sure the value is within the quantile range, cutting off the tails
      // see first parenthesis in equation: byte = (float - minQuantile) * 127/(maxQuantile -
      // minQuantile)
      float dx = v - minQuantile;
      float dxc = Math.max(minQuantile, Math.min(maxQuantile, v)) - minQuantile;
      // Scale the value to the range [0, 127], this is our quantized value
      // scale = 127/(maxQuantile - minQuantile)
      int roundedDxs = Math.round(scale * dxc);
      // We multiply by `alpha` here to get the quantized value back into the original range
      // to aid in calculating the corrective offset
      float dxq = roundedDxs * alpha;
      if (dest != null) {
        dest[destIndex] = (byte) roundedDxs;
      }
      // Calculate the corrective offset that needs to be applied to the score
      // in addition to the `byte * minQuantile * alpha` term in the equation
      // we add the `(dx - dxq) * dxq` term to account for the fact that the quantized value
      // will be rounded to the nearest whole number and lose some accuracy
      // Additionally, we account for the global correction of `minQuantile^2` in the equation
      return minQuantile * (v - minQuantile / 2.0F) + (dx - dxq) * dxq;
    }
  }

  @Override
  public int filterByScore(
      int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo) {
    int newSize = 0;
    for (int i = 0; i < upTo; ++i) {
      int doc = docBuffer[i];
      double score = scoreBuffer[i];
      docBuffer[newSize] = doc;
      scoreBuffer[newSize] = score;
      if (score >= minScoreInclusive) {
        newSize++;
      }
    }
    return newSize;
  }

  @Override
  public float[] l2normalize(float[] v, boolean throwOnZero) {
    double l1norm = this.dotProduct(v, v);
    if (l1norm == 0) {
      if (throwOnZero) {
        throw new IllegalArgumentException("Cannot normalize a zero-length vector");
      } else {
        return v;
      }
    }
    if (Math.abs(l1norm - 1.0d) <= EPSILON) {
      return v;
    }
    int dim = v.length;
    double l2norm = Math.sqrt(l1norm);
    for (int i = 0; i < dim; i++) {
      v[i] /= (float) l2norm;
    }
    return v;
  }

  @Override
  public void expand8(int[] arr) {
    // BLOCK_SIZE is 256
    for (int i = 0; i < 64; ++i) {
      int l = arr[i];
      arr[i] = (l >>> 24) & 0xFF;
      arr[64 + i] = (l >>> 16) & 0xFF;
      arr[128 + i] = (l >>> 8) & 0xFF;
      arr[192 + i] = l & 0xFF;
    }
  }
}
