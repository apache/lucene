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

import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.logging.Logger;
import jdk.incubator.vector.FloatVector;
import org.apache.lucene.util.Constants;

/**
 * VectorUtilSupport implementation that uses native bindings for optimized vector operations(using
 * Foreign Function and Memory API (FFM)) if available(optional) or else fallback to
 * PanamaVectorUtil implementations.
 *
 * <p>This class provides access to native C implementations of dot product operations from the
 * loaded shared/dynamic library(.so|.dylib|.dll) which is generated from C code and linked at
 * runtime. The native library contains multiple optimized implementations:
 *
 * <p>PanamaVectorUtilSupport#dotProduct use this Native C implementation for dot product
 * calculation if system property <b>lucene.useNativeDotProduct=true</b> is passed it always tries
 * to ensure binary is provided and required methods are implemented
 *
 * <p>It Uses <code>Linker.Option.critical(true)</code> for optimal performance by eliminating the
 * overhead of ensuring MemorySegments are allocated off-heap before native calls.
 */
@SuppressWarnings("restricted")
public final class NativeVectorUtilSupport implements VectorUtilSupport {

  private final VectorUtilSupport delegateVectorUtilSupport;

  public static final AddressLayout POINTER = ValueLayout.ADDRESS;

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup SYMBOL_LOOKUP;

  @SuppressWarnings("NonFinalStaticField")
  private static boolean isLibraryLoaded;

  public static final String NATIVE_VECTOR_LIBRARY_NAME = "dotProduct";

  public NativeVectorUtilSupport(VectorUtilSupport vectorUtilSupport) {
    this.delegateVectorUtilSupport = vectorUtilSupport;
  }

  static {
    try {
      // Attempt to load the library
      System.loadLibrary(NATIVE_VECTOR_LIBRARY_NAME);
      isLibraryLoaded = true; // If successful, set the flag to true
    } catch (UnsatisfiedLinkError e) {
      // If the library loading fails, set the flag to false
      isLibraryLoaded = false;
      Logger.getLogger(NativeVectorUtilSupport.class.getName())
          .warning("No native library" + NATIVE_VECTOR_LIBRARY_NAME + " found : " + e.getMessage());
    }
  }

  // Function descriptors
  private static final FunctionDescriptor dot8sDesc =
      FunctionDescriptor.of(JAVA_INT, POINTER, POINTER, JAVA_INT);

  // Method handles
  private static final MethodHandle dotProduct$MH;

  public static boolean isLibraryLoaded() {
    return isLibraryLoaded;
  }

  static {
    if (isLibraryLoaded) {
      SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
      SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));

      // Method handles for native functions
      dotProduct$MH = getMethodHandle("dot8s", dot8sDesc);
    } else if (Constants.NATIVE_DOT_PRODUCT_ENABLED) {
      throw new RuntimeException("Native library dotProduct missing!");
    } else {
      SYMBOL_LOOKUP = null;
      dotProduct$MH = null;
    }
  }

  private static MethodHandle getMethodHandle(String methodName, FunctionDescriptor descriptor) {
    MethodHandle mh =
        SYMBOL_LOOKUP
            .find(methodName)
            .map(addr -> LINKER.downcallHandle(addr, descriptor, Linker.Option.critical(true)))
            .orElse(null);
    if (Constants.NATIVE_DOT_PRODUCT_ENABLED && mh == null) {
      throw new RuntimeException("C code for " + methodName + " was not linked!");
    }
    return mh;
  }

  private static int invokedMethodHandle(MethodHandle mh, MemorySegment a, MemorySegment b) {
    try {
      int limit = (int) a.byteSize();
      return (int) mh.invokeExact(a, b, limit);
    } catch (Throwable ex$) {
      throw new AssertionError("should not reach here", ex$);
    }
  }

  static FloatVector fma(FloatVector a, FloatVector b, FloatVector c) {
    return PanamaVectorUtilSupport.fma(a, b, c);
  }

  static float fma(float a, float b, float c) {
    return PanamaVectorUtilSupport.fma(a, b, c);
  }

  public static float cosine(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.cosine(a, b);
  }

  public static float cosine(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.cosine(a, b);
  }

  public static int dotProduct(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int dotProduct(MemorySegment a, MemorySegment b) {
    if (dotProduct$MH != null) {
      // throw new RuntimeException("issue in native");
      return invokedMethodHandle(dotProduct$MH, a, b);
    }
    return PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int squareDistance(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int squareDistance(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int int4SquareDistance(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistance(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistanceSinglePacked(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4SquareDistanceSinglePacked(a, b);
  }

  public static int uint8SquareDistance(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8SquareDistance(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8DotProduct(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int uint8DotProduct(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int int4DotProduct(byte[] a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProduct(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProductSinglePacked(byte[] unpacked, MemorySegment packed) {
    return PanamaVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  public static int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  public static int int4DotProductBothPacked(MemorySegment a, MemorySegment b) {
    return PanamaVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public float dotProduct(float[] a, float[] b) {
    return delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public float cosine(float[] v1, float[] v2) {
    return delegateVectorUtilSupport.cosine(v1, v2);
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    return delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    if (dotProduct$MH != null) {
      return dotProduct(MemorySegment.ofArray(a), MemorySegment.ofArray(b));
    }
    return delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public int int4DotProduct(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.int4DotProduct(a, b);
  }

  @Override
  public int int4DotProductSinglePacked(byte[] unpacked, byte[] packed) {
    return delegateVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  @Override
  public int int4DotProductBothPacked(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public int uint8DotProduct(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.uint8DotProduct(a, b);
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.cosine(a, b);
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int int4SquareDistance(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.int4SquareDistance(a, b);
  }

  @Override
  public int int4SquareDistanceSinglePacked(byte[] unpacked, byte[] packed) {
    return delegateVectorUtilSupport.int4SquareDistanceSinglePacked(unpacked, packed);
  }

  @Override
  public int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  @Override
  public int uint8SquareDistance(byte[] a, byte[] b) {
    return delegateVectorUtilSupport.uint8SquareDistance(a, b);
  }

  @Override
  public int findNextGEQ(int[] buffer, int target, int from, int to) {
    return delegateVectorUtilSupport.findNextGEQ(buffer, target, from, to);
  }

  @Override
  public long int4BitDotProduct(byte[] int4Quantized, byte[] binaryQuantized) {
    return delegateVectorUtilSupport.int4BitDotProduct(int4Quantized, binaryQuantized);
  }

  @Override
  public float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile) {
    return delegateVectorUtilSupport.minMaxScalarQuantize(
        vector, dest, scale, alpha, minQuantile, maxQuantile);
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
    return delegateVectorUtilSupport.recalculateScalarQuantizationOffset(
        vector, oldAlpha, oldMinQuantile, scale, alpha, minQuantile, maxQuantile);
  }

  @Override
  public int filterByScore(
      int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo) {
    return delegateVectorUtilSupport.filterByScore(docBuffer, scoreBuffer, minScoreInclusive, upTo);
  }

  @Override
  public float[] l2normalize(float[] v, boolean throwOnZero) {
    return delegateVectorUtilSupport.l2normalize(v, throwOnZero);
  }

  @Override
  public void expand8(int[] arr) {
    delegateVectorUtilSupport.expand8(arr);
  }
}
