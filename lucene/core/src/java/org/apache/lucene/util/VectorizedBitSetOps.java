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

/**
 * SIMD-accelerated operations for {@code FixedBitSet}.
 *
 * <p>The Vector API is only used when the incubator module is resolved at runtime.
 */
final class VectorizedBitSetOps {
  // Small-bitset fallback: for very small arrays, Vector API setup/tails can cost more than scalar.
  // Length is expressed in 64-bit words.
  // Conservative default for core acceptance: avoid vectorizing small bitsets where scalar
  // Long.bitCount() intrinsics can be hard to beat and results can be noisy.
  private static final int MIN_WORDS_FOR_VECTORIZATION = 64;

  private static final VectorHelper VECTOR_HELPER = loadVectorHelper();
  private static final boolean VECTORIZED = VECTOR_HELPER.isVectorized();

  private static VectorHelper loadVectorHelper() {
    try {
      if (Constants.IS_HOTSPOT_VM == false || Constants.IS_JVMCI_VM || Constants.IS_CLIENT_VM) {
        return ScalarVectorHelper.INSTANCE;
      }
      // If the vector module is not resolved (missing --add-modules), we must not even attempt
      // to load a class that links against it.
      var layer = VectorizedBitSetOps.class.getModule().getLayer();
      var vectorModule =
          (layer != null ? layer : ModuleLayer.boot()).findModule("jdk.incubator.vector");
      if (vectorModule.isEmpty()) {
        return ScalarVectorHelper.INSTANCE;
      }
      // Ensure readability before linking any Vector API types.
      VectorizedBitSetOps.class.getModule().addReads(vectorModule.get());

      Class<?> helperClass = Class.forName("org.apache.lucene.util.VectorizedBitSetOpsHelper");
      return (VectorHelper) helperClass.getDeclaredConstructor().newInstance();
    } catch (Throwable t) {
      assert t != null;
      return ScalarVectorHelper.INSTANCE;
    }
  }

  static boolean shouldUseVectorized(int length) {
    return VECTORIZED && length >= MIN_WORDS_FOR_VECTORIZATION;
  }

  static long popCount(long[] a, int len) {
    return popCount(a, 0, len);
  }

  static long popCount(long[] a, int start, int len) {
    if (shouldUseVectorized(len)) {
      return VECTOR_HELPER.popCount(a, start, len);
    } else {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[start + i]);
      }
      return count;
    }
  }

  static long intersectionCount(long[] a, long[] b, int len) {
    if (shouldUseVectorized(len)) {
      return VECTOR_HELPER.intersectionCount(a, b, len);
    } else {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] & b[i]);
      }
      return count;
    }
  }

  static long unionCount(long[] a, long[] b, int len) {
    if (shouldUseVectorized(len)) {
      return VECTOR_HELPER.unionCount(a, b, len);
    } else {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] | b[i]);
      }
      return count;
    }
  }

  static long andNotCount(long[] a, long[] b, int len) {
    if (shouldUseVectorized(len)) {
      return VECTOR_HELPER.andNotCount(a, b, len);
    } else {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] & ~b[i]);
      }
      return count;
    }
  }

  // Helper interfaces and implementations

  interface VectorHelper {
    boolean isVectorized();

    long popCount(long[] a, int start, int len);

    long intersectionCount(long[] a, long[] b, int len);

    long unionCount(long[] a, long[] b, int len);

    long andNotCount(long[] a, long[] b, int len);
  }

  static class ScalarVectorHelper implements VectorHelper {
    static final ScalarVectorHelper INSTANCE = new ScalarVectorHelper();

    private ScalarVectorHelper() {}

    @Override
    public boolean isVectorized() {
      return false;
    }

    @Override
    public long popCount(long[] a, int start, int len) {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[start + i]);
      }
      return count;
    }

    @Override
    public long intersectionCount(long[] a, long[] b, int len) {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] & b[i]);
      }
      return count;
    }

    @Override
    public long unionCount(long[] a, long[] b, int len) {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] | b[i]);
      }
      return count;
    }

    @Override
    public long andNotCount(long[] a, long[] b, int len) {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[i] & ~b[i]);
      }
      return count;
    }
  }
}
