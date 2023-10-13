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

/** Utilities for computations with numeric arrays */
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
    double squareSum = IMPL.dotProduct(v, v);
    int dim = v.length;
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
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
    return IMPL.dotProduct(a, b);
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
