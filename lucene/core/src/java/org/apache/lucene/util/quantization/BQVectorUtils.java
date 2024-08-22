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

import java.util.BitSet;
import org.apache.lucene.util.VectorUtil;

// FIXME: move these to VectorUtils?
public class BQVectorUtils {
  public static int popcount(byte[] d, int dimensions) {
    return BitSet.valueOf(d).cardinality();
  }

  // FIXME: divide by the norm is a pretty common operation may be able to provide a combination
  // function that's faster
  public static float[] divide(float[] a, float b) {
    float[] c = new float[a.length];
    for (int j = 0; j < a.length; j++) {
      c[j] = a[j] / b;
    }
    return c;
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
    magnitude = (float) Math.sqrt(magnitude);

    // FIXME: FUTURE - not good; sometimes this needs to be 0
    //            if (magnitude == 0) {
    //                throw new IllegalArgumentException("Cannot normalize a vector of length
    // zero.");
    //            }

    return magnitude;
  }

  // FIXME: move this out to vector utils
  //  public static float norm(float[] vector) {
  //    float normalized = 0f;
  //    // Calculate magnitude/length of the vector
  //    double magnitude = 0;
  //
  //    int size = vector.length / FLOAT_SPECIES.length();
  //    for (int r = 0; r < size; r++) {
  //      int offset = FLOAT_SPECIES.length() * r;
  //      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, vector, offset);
  //      magnitude += va.mul(va).reduceLanes(VectorOperators.ADD);
  //    }
  //
  //    // tail
  //    int remainder = vector.length % FLOAT_SPECIES.length();
  //    if (remainder != 0) {
  //      for (int i = vector.length - remainder; i < vector.length; i++) {
  //        magnitude = Math.fma(vector[i], vector[i], magnitude);
  //      }
  //    }
  //
  //    // FIXME: evaluate for small dimensions whether this is faster
  //    //            for (int i = 0; i < vector.length; i++) {
  //    //                magnitude = Math.fma(vector[i], vector[i], magnitude);
  //    //            }
  //
  //    magnitude = Math.sqrt(magnitude);
  //
  //    // FIXME: FUTURE - not good; sometimes this needs to be 0
  //    //            if (magnitude == 0) {
  //    //                throw new IllegalArgumentException("Cannot normalize a vector of length
  //    // zero.");
  //    //            }
  //
  //    normalized = (float) magnitude;
  //
  //    return normalized;
  //  }
}
