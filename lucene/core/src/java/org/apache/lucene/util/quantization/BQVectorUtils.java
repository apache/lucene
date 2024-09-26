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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/** Utility class for vector quantization calculations */

// FIXME: move these to VectorUtils?
public class BQVectorUtils {

  public static int discretize(int value, int bucket) {
    return ((value + (bucket - 1)) / bucket) * bucket;
  }

  public static float[] pad(float[] vector, int dimensions) {
    if (vector.length >= dimensions) {
      return vector;
    }
    return ArrayUtil.growExact(vector, dimensions);
  }

  public static byte[] pad(byte[] vector, int dimensions) {
    if (vector.length >= dimensions) {
      return vector;
    }
    return ArrayUtil.growExact(vector, dimensions);
  }

  public static int popcount(byte[] d) {
    // TODO: can this be vectorized even better?
    int r = 0;
    int cnt = 0;
    for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
      cnt += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(d, r));
    }
    for (; r < d.length; r++) {
      cnt += Integer.bitCount(d[r] & 0xFF);
    }
    return cnt;
  }

  // TODO: move to VectorUtil & vectorize?
  public static void divideInPlace(float[] a, float b) {
    for (int j = 0; j < a.length; j++) {
      a[j] /= b;
    }
  }

  public static float[] subtract(float[] a, float[] b) {
    float[] result = new float[a.length];
    subtract(a, b, result);
    return result;
  }

  public static void subtractInPlace(float[] target, float[] other) {
    subtract(target, other, target);
  }

  private static float[] subtract(float[] a, float[] b, float[] result) {
    for (int j = 0; j < a.length; j++) {
      result[j] = a[j] - b[j];
    }
    return result;
  }

  public static float norm(float[] vector) {
    float magnitude = VectorUtil.dotProduct(vector, vector);
    return (float) Math.sqrt(magnitude);
  }
}
