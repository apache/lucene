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

import org.apache.lucene.internal.vectorization.VectorUtilSupport;
import org.apache.lucene.internal.vectorization.VectorizationProvider;

/**
 * Utilities for computations with numeric arrays, especially algebraic operations like vector dot
 * products. This class uses SIMD vectorization if the corresponding Java module is available and
 * enabled. To enable vectorized code, pass {@code --add-modules jdk.incubator.vector} to Java's
 * command line.
 *
 * <p>It will use CPU's <a href="https://en.wikipedia.org/wiki/Fused_multiply%E2%80%93add">FMA
 * instructions</a> if it is known to perform faster than separate multiply+add. This requires at
 * least Hotspot C2 enabled, which is the default for OpenJDK based JVMs.
 *
 * <p>To explicitly disable or enable FMA usage, pass the following system properties:
 *
 * <ul>
 *   <li>{@code -Dlucene.useScalarFMA=(auto|true|false)} for scalar operations
 *   <li>{@code -Dlucene.useVectorFMA=(auto|true|false)} for vectorized operations (with vector
 *       incubator module)
 * </ul>
 *
 * <p>The default is {@code auto}, which enables this for known CPU types and JVM settings. If
 * Hotspot C2 is disabled, FMA and vectorization are <strong>not</strong> used.
 *
 * <p>Vectorization and FMA is only supported for Hotspot-based JVMs; it won't work on OpenJ9-based
 * JVMs unless they provide {@link com.sun.management.HotSpotDiagnosticMXBean}. Please also make
 * sure that you have the {@code jdk.management} module enabled in modularized applications.
 */
public final class VectorUtil {

  private static final VectorUtilSupport IMPL =
      VectorizationProvider.getInstance().getVectorUtilSupport();

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
    float r = IMPL.dotProduct(a, b);
    assert Float.isFinite(r);
    return r;
  }

  /**
   * Returns the cosine similarity between the two vectors.
   *
   * @throws IllegalArgumentException if the vectors' dimensions differ.
   */
  public static float cosine(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    float r = IMPL.cosine(a, b);
    assert Float.isFinite(r);
    return r;
  }

  /** Returns the cosine similarity between the two vectors. */
  public static float cosine(byte[] a, byte[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    return IMPL.cosine(a, b);
  }

  /**
   * Returns the sum of squared differences of the two vectors.
   *
   * @throws IllegalArgumentException if the vectors' dimensions differ.
   */
  public static float squareDistance(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    float r = IMPL.squareDistance(a, b);
    assert Float.isFinite(r);
    return r;
  }

  /** Returns the sum of squared differences of the two vectors. */
  public static int squareDistance(byte[] a, byte[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    return IMPL.squareDistance(a, b);
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
    double l1norm = IMPL.dotProduct(v, v);
    if (l1norm == 0) {
      if (throwOnZero) {
        throw new IllegalArgumentException("Cannot normalize a zero-length vector");
      } else {
        return v;
      }
    }
    if (Math.abs(l1norm - 1.0d) <= 1e-5) {
      return v;
    }
    int dim = v.length;
    double l2norm = Math.sqrt(l1norm);
    for (int i = 0; i < dim; i++) {
      v[i] /= (float) l2norm;
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
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    return IMPL.dotProduct(a, b);
  }

  public static int int4DotProduct(byte[] a, byte[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    return IMPL.int4DotProduct(a, false, b, false);
  }

  /**
   * Dot product computed over int4 (values between [0,15]) bytes. The second vector is considered
   * "packed" (i.e. every byte representing two values). The following packing is assumed:
   *
   * <pre class="prettyprint lang-java">
   *   packed[0] = (raw[0] * 16) | raw[packed.length];
   *   packed[1] = (raw[1] * 16) | raw[packed.length + 1];
   *   ...
   *   packed[packed.length - 1] = (raw[packed.length - 1] * 16) | raw[2 * packed.length - 1];
   * </pre>
   *
   * @param unpacked the unpacked vector, of even length
   * @param packed the packed vector, of length {@code (unpacked.length + 1) / 2}
   * @return the value of the dot product of the two vectors
   */
  public static int int4DotProductPacked(byte[] unpacked, byte[] packed) {
    if (packed.length != ((unpacked.length + 1) >> 1)) {
      throw new IllegalArgumentException(
          "vector dimensions differ: " + unpacked.length + "!= 2 * " + packed.length);
    }
    return IMPL.int4DotProduct(unpacked, false, packed, true);
  }

  /**
   * XOR bit count computed over signed bytes.
   *
   * @param a bytes containing a vector
   * @param b bytes containing another vector, of the same dimension
   * @return the value of the XOR bit count of the two vectors
   */
  public static int xorBitCount(byte[] a, byte[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    int distance = 0, i = 0;
    for (final int upperBound = a.length & ~(Long.BYTES - 1); i < upperBound; i += Long.BYTES) {
      distance +=
          Long.bitCount(
              (long) BitUtil.VH_NATIVE_LONG.get(a, i) ^ (long) BitUtil.VH_NATIVE_LONG.get(b, i));
    }
    // tail:
    for (; i < a.length; i++) {
      distance += Integer.bitCount((a[i] ^ b[i]) & 0xFF);
    }
    return distance;
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

  /**
   * @param vectorDotProductSimilarity the raw similarity between two vectors
   * @return A scaled score preventing negative scores for maximum-inner-product
   */
  public static float scaleMaxInnerProductScore(float vectorDotProductSimilarity) {
    if (vectorDotProductSimilarity < 0) {
      return 1 / (1 + -1 * vectorDotProductSimilarity);
    }
    return vectorDotProductSimilarity + 1;
  }

  /**
   * Checks if a float vector only has finite components.
   *
   * @param v bytes containing a vector
   * @return the vector for call-chaining
   * @throws IllegalArgumentException if any component of vector is not finite
   */
  public static float[] checkFinite(float[] v) {
    for (int i = 0; i < v.length; i++) {
      if (!Float.isFinite(v[i])) {
        throw new IllegalArgumentException("non-finite value at vector[" + i + "]=" + v[i]);
      }
    }
    return v;
  }
}
