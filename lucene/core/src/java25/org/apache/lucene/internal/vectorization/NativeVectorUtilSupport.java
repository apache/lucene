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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.function.Supplier;
import java.util.logging.Logger;
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

  // TODO: Make this dynamic?
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
  // (POINTER, POINTER, INT) -> INT
  private static final FunctionDescriptor twoPointerIntToInt =
      FunctionDescriptor.of(JAVA_INT, POINTER, POINTER, JAVA_INT);

  // (POINTER, POINTER, INT) -> LONG
  private static final FunctionDescriptor twoPointerIntToLong =
      FunctionDescriptor.of(JAVA_LONG, POINTER, POINTER, JAVA_INT);

  // (POINTER, POINTER, INT) -> FLOAT
  private static final FunctionDescriptor twoPointerIntToFloat =
      FunctionDescriptor.of(JAVA_FLOAT, POINTER, POINTER, JAVA_INT);

  // (POINTER, POINTER, FLOAT, FLOAT, FLOAT, FLOAT, INT) -> FLOAT
  private static final FunctionDescriptor minMaxScalarQuantizeDesc =
      FunctionDescriptor.of(
          JAVA_FLOAT, POINTER, POINTER, JAVA_FLOAT, JAVA_FLOAT, JAVA_FLOAT, JAVA_FLOAT, JAVA_INT);

  // (POINTER, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, INT) -> FLOAT
  private static final FunctionDescriptor recalculateOffsetDesc =
      FunctionDescriptor.of(
          JAVA_FLOAT,
          POINTER,
          JAVA_FLOAT,
          JAVA_FLOAT,
          JAVA_FLOAT,
          JAVA_FLOAT,
          JAVA_FLOAT,
          JAVA_FLOAT,
          JAVA_INT);

  // (POINTER, POINTER, DOUBLE, INT) -> INT
  private static final FunctionDescriptor filterByScoreDesc =
      FunctionDescriptor.of(JAVA_INT, POINTER, POINTER, JAVA_DOUBLE, JAVA_INT);

  // (POINTER, BYTE, INT) -> POINTER
  private static final FunctionDescriptor l2normalizeDesc =
      FunctionDescriptor.of(POINTER, POINTER, JAVA_BYTE, JAVA_INT);

  // (POINTER, INT) -> void
  private static final FunctionDescriptor expand8Desc =
      FunctionDescriptor.ofVoid(POINTER, JAVA_INT);

  // (POINTER, INT, INT, INT) -> INT
  private static final FunctionDescriptor findNextGEQDesc =
      FunctionDescriptor.of(JAVA_INT, POINTER, JAVA_INT, JAVA_INT, JAVA_INT);

  // Method handles
  private static final MethodHandle dotProduct$MH;
  private static final MethodHandle squareDistance$MH;
  private static final MethodHandle cosine$MH;
  private static final MethodHandle dotProductFloat$MH;
  private static final MethodHandle squareDistanceFloat$MH;
  private static final MethodHandle cosineFloat$MH;
  private static final MethodHandle int4SquareDistance$MH;
  private static final MethodHandle int4SquareDistanceSinglePacked$MH;
  private static final MethodHandle int4SquareDistanceBothPacked$MH;
  private static final MethodHandle uint8SquareDistance$MH;
  private static final MethodHandle uint8DotProduct$MH;
  private static final MethodHandle int4DotProduct$MH;
  private static final MethodHandle int4DotProductSinglePacked$MH;
  private static final MethodHandle int4DotProductBothPacked$MH;
  private static final MethodHandle int4BitDotProduct$MH;
  private static final MethodHandle int4DibitDotProduct$MH;
  private static final MethodHandle minMaxScalarQuantize$MH;
  private static final MethodHandle recalculateScalarQuantizationOffset$MH;
  private static final MethodHandle filterByScore$MH;
  private static final MethodHandle l2normalize$MH;
  private static final MethodHandle expand8$MH;
  private static final MethodHandle findNextGEQ$MH;

  public static boolean isLibraryLoaded() {
    return isLibraryLoaded;
  }

  static {
    if (isLibraryLoaded) {
      SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
      SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));

      // Each method handle with a unique native method name
      dotProduct$MH = getMethodHandle("dotProduct", twoPointerIntToInt);
      squareDistance$MH = getMethodHandle("squareDistance", twoPointerIntToInt);
      cosine$MH = getMethodHandle("cosine", twoPointerIntToInt);
      dotProductFloat$MH = getMethodHandle("dotProductFloat", twoPointerIntToFloat);
      squareDistanceFloat$MH = getMethodHandle("squareDistanceFloat", twoPointerIntToFloat);
      cosineFloat$MH = getMethodHandle("cosineFloat", twoPointerIntToFloat);
      int4SquareDistance$MH = getMethodHandle("int4SquareDistance", twoPointerIntToInt);
      int4SquareDistanceSinglePacked$MH =
          getMethodHandle("int4SquareDistanceSinglePacked", twoPointerIntToInt);
      int4SquareDistanceBothPacked$MH =
          getMethodHandle("int4SquareDistanceBothPacked", twoPointerIntToInt);
      uint8SquareDistance$MH = getMethodHandle("uint8SquareDistance", twoPointerIntToInt);
      uint8DotProduct$MH = getMethodHandle("uint8DotProduct", twoPointerIntToInt);
      int4DotProduct$MH = getMethodHandle("int4DotProduct", twoPointerIntToInt);
      int4DotProductSinglePacked$MH =
          getMethodHandle("int4DotProductSinglePacked", twoPointerIntToInt);
      int4DotProductBothPacked$MH = getMethodHandle("int4DotProductBothPacked", twoPointerIntToInt);
      int4BitDotProduct$MH = getMethodHandle("int4BitDotProduct", twoPointerIntToLong);
      int4DibitDotProduct$MH = getMethodHandle("int4DibitDotProduct", twoPointerIntToLong);
      minMaxScalarQuantize$MH = getMethodHandle("minMaxScalarQuantize", minMaxScalarQuantizeDesc);
      recalculateScalarQuantizationOffset$MH =
          getMethodHandle("recalculateScalarQuantizationOffset", recalculateOffsetDesc);
      filterByScore$MH = getMethodHandle("filterByScore", filterByScoreDesc);
      l2normalize$MH = getMethodHandle("l2normalize", l2normalizeDesc);
      expand8$MH = getMethodHandle("expand8", expand8Desc);
      findNextGEQ$MH = getMethodHandle("findNextGEQ", findNextGEQDesc);
    } else if (Constants.NATIVE_DOT_PRODUCT_ENABLED) {
      throw new RuntimeException("Native library dotProduct missing!");
    } else {
      SYMBOL_LOOKUP = null;
      dotProduct$MH = null;
      squareDistance$MH = null;
      cosine$MH = null;
      dotProductFloat$MH = null;
      squareDistanceFloat$MH = null;
      cosineFloat$MH = null;
      int4SquareDistance$MH = null;
      int4SquareDistanceSinglePacked$MH = null;
      int4SquareDistanceBothPacked$MH = null;
      uint8SquareDistance$MH = null;
      uint8DotProduct$MH = null;
      int4DotProduct$MH = null;
      int4DotProductSinglePacked$MH = null;
      int4DotProductBothPacked$MH = null;
      int4BitDotProduct$MH = null;
      int4DibitDotProduct$MH = null;
      minMaxScalarQuantize$MH = null;
      recalculateScalarQuantizationOffset$MH = null;
      filterByScore$MH = null;
      l2normalize$MH = null;
      expand8$MH = null;
      findNextGEQ$MH = null;
    }
  }

  private static MethodHandle getMethodHandle(String methodName, FunctionDescriptor descriptor) {
    MethodHandle mh =
        SYMBOL_LOOKUP
            .find(methodName)
            .map(addr -> LINKER.downcallHandle(addr, descriptor, Linker.Option.critical(true)))
            .orElse(null);
    if (mh == null && Constants.NATIVE_STRICT_MODE) {
      throw new RuntimeException("C code for " + methodName + " was not linked!");
    }
    return mh;
  }

  // Reusable invoke helpers for signatures used multiple times
  private static int invokeIntMethodHandle(MethodHandle mh, MemorySegment a, MemorySegment b) {
    try {
      return (int) mh.invokeExact(a, b, (int) a.byteSize());
    } catch (Throwable ex) {
      throw new AssertionError("should not reach here", ex);
    }
  }

  private static long invokeLongMethodHandle(MethodHandle mh, MemorySegment a, MemorySegment b) {
    try {
      return (long) mh.invokeExact(a, b, (int) a.byteSize());
    } catch (Throwable ex) {
      throw new AssertionError("should not reach here", ex);
    }
  }

  private static float invokeFloatMethodHandle(MethodHandle mh, MemorySegment a, MemorySegment b) {
    try {
      return (float) mh.invokeExact(a, b, (int) a.byteSize());
    } catch (Throwable ex) {
      throw new AssertionError("should not reach here", ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T invokeOrDelegate(MethodHandle mh, Supplier<T> delegate, Object... args) {
    if (mh != null) {
      try {
        return (T) mh.invokeExact(args);
      } catch (Throwable ex) {
        throw new AssertionError("should not reach here", ex);
      }
    }
    return delegate.get();
  }

  public static float cosine(byte[] a, MemorySegment b) {
    return (cosine$MH != null)
        ? cosine(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.cosine(a, b);
  }

  public static float cosine(MemorySegment a, MemorySegment b) {
    return (cosine$MH != null)
        ? invokeIntMethodHandle(cosine$MH, a, b)
        : PanamaVectorUtilSupport.cosine(a, b);
  }

  public static int dotProduct(byte[] a, MemorySegment b) {
    return (dotProduct$MH != null)
        ? dotProduct(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int dotProduct(MemorySegment a, MemorySegment b) {
    return (dotProduct$MH != null)
        ? invokeIntMethodHandle(dotProduct$MH, a, b)
        : PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int squareDistance(byte[] a, MemorySegment b) {
    return (squareDistance$MH != null)
        ? squareDistance(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int squareDistance(MemorySegment a, MemorySegment b) {
    return (squareDistance$MH != null)
        ? invokeIntMethodHandle(squareDistance$MH, a, b)
        : PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int int4SquareDistance(byte[] a, MemorySegment b) {
    return (int4SquareDistance$MH != null)
        ? int4SquareDistance(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistance(MemorySegment a, MemorySegment b) {
    return (int4SquareDistance$MH != null)
        ? invokeIntMethodHandle(int4SquareDistance$MH, a, b)
        : PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistanceSinglePacked(byte[] a, MemorySegment b) {
    return (int4SquareDistanceSinglePacked$MH != null)
        ? invokeIntMethodHandle(int4SquareDistanceSinglePacked$MH, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4SquareDistanceSinglePacked(a, b);
  }

  public static int uint8SquareDistance(byte[] a, MemorySegment b) {
    return (uint8SquareDistance$MH != null)
        ? uint8SquareDistance(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8SquareDistance(MemorySegment a, MemorySegment b) {
    return (uint8SquareDistance$MH != null)
        ? invokeIntMethodHandle(uint8SquareDistance$MH, a, b)
        : PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8DotProduct(byte[] a, MemorySegment b) {
    return (uint8DotProduct$MH != null)
        ? uint8DotProduct(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int uint8DotProduct(MemorySegment a, MemorySegment b) {
    return (uint8DotProduct$MH != null)
        ? invokeIntMethodHandle(uint8DotProduct$MH, a, b)
        : PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int int4DotProduct(byte[] a, MemorySegment b) {
    return (int4DotProduct$MH != null)
        ? int4DotProduct(MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProduct(MemorySegment a, MemorySegment b) {
    return (int4DotProduct$MH != null)
        ? invokeIntMethodHandle(int4DotProduct$MH, a, b)
        : PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProductSinglePacked(byte[] unpacked, MemorySegment packed) {
    return (int4DotProductSinglePacked$MH != null)
        ? invokeIntMethodHandle(
            int4DotProductSinglePacked$MH, MemorySegment.ofArray(unpacked), packed)
        : PanamaVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  public static int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b) {
    return (int4SquareDistanceBothPacked$MH != null)
        ? invokeIntMethodHandle(int4SquareDistanceBothPacked$MH, a, b)
        : PanamaVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  public static int int4DotProductBothPacked(MemorySegment a, MemorySegment b) {
    return (int4DotProductBothPacked$MH != null)
        ? invokeIntMethodHandle(int4DotProductBothPacked$MH, a, b)
        : PanamaVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public float dotProduct(float[] a, float[] b) {
    return (dotProductFloat$MH != null)
        ? invokeFloatMethodHandle(
            dotProductFloat$MH, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public float cosine(float[] v1, float[] v2) {
    return (cosineFloat$MH != null)
        ? invokeFloatMethodHandle(
            cosineFloat$MH, MemorySegment.ofArray(v1), MemorySegment.ofArray(v2))
        : delegateVectorUtilSupport.cosine(v1, v2);
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    return (squareDistanceFloat$MH != null)
        ? invokeFloatMethodHandle(
            squareDistanceFloat$MH, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    return (dotProduct$MH != null)
        ? dotProduct(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public int int4DotProduct(byte[] a, byte[] b) {
    return (int4DotProduct$MH != null)
        ? int4DotProduct(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4DotProduct(a, b);
  }

  @Override
  public int int4DotProductSinglePacked(byte[] unpacked, byte[] packed) {
    return int4DotProductSinglePacked$MH != null
        ? int4DotProductSinglePacked(unpacked, MemorySegment.ofArray(packed))
        : delegateVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  @Override
  public int int4DotProductBothPacked(byte[] a, byte[] b) {
    return (int4DotProductBothPacked$MH != null)
        ? int4DotProductBothPacked(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public int uint8DotProduct(byte[] a, byte[] b) {
    return (uint8DotProduct$MH != null)
        ? uint8DotProduct(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.uint8DotProduct(a, b);
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    return (cosine$MH != null)
        ? cosine(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.cosine(a, b);
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    return (squareDistance$MH != null)
        ? squareDistance(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int int4SquareDistance(byte[] a, byte[] b) {
    return (int4SquareDistance$MH != null)
        ? int4SquareDistance(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4SquareDistance(a, b);
  }

  @Override
  public int int4SquareDistanceSinglePacked(byte[] unpacked, byte[] packed) {
    return (int4SquareDistanceSinglePacked$MH != null)
        ? int4SquareDistanceSinglePacked(unpacked, MemorySegment.ofArray(packed))
        : delegateVectorUtilSupport.int4SquareDistanceSinglePacked(unpacked, packed);
  }

  @Override
  public int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    return (int4SquareDistanceBothPacked$MH != null)
        ? int4SquareDistanceBothPacked(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  @Override
  public int uint8SquareDistance(byte[] a, byte[] b) {
    return (uint8SquareDistance$MH != null)
        ? uint8SquareDistance(MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.uint8SquareDistance(a, b);
  }

  @Override
  public long int4BitDotProduct(byte[] int4Quantized, byte[] binaryQuantized) {
    if (int4BitDotProduct$MH != null) {
      return invokeLongMethodHandle(
          int4BitDotProduct$MH,
          MemorySegment.ofArray(int4Quantized),
          MemorySegment.ofArray(binaryQuantized));
    }
    return delegateVectorUtilSupport.int4BitDotProduct(int4Quantized, binaryQuantized);
  }

  @Override
  public long int4DibitDotProduct(byte[] int4Quantized, byte[] dibitQuantized) {
    if (int4DibitDotProduct$MH != null) {
      return invokeLongMethodHandle(
          int4DibitDotProduct$MH,
          MemorySegment.ofArray(int4Quantized),
          MemorySegment.ofArray(dibitQuantized));
    }
    return delegateVectorUtilSupport.int4DibitDotProduct(int4Quantized, dibitQuantized);
  }

  @Override
  public int findNextGEQ(int[] buffer, int target, int from, int to) {
    return invokeOrDelegate(
        findNextGEQ$MH,
        () -> delegateVectorUtilSupport.findNextGEQ(buffer, target, from, to),
        MemorySegment.ofArray(buffer),
        target,
        from,
        to);
  }

  @Override
  public float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile) {
    return invokeOrDelegate(
        minMaxScalarQuantize$MH,
        () ->
            delegateVectorUtilSupport.minMaxScalarQuantize(
                vector, dest, scale, alpha, minQuantile, maxQuantile),
        MemorySegment.ofArray(vector),
        MemorySegment.ofArray(dest),
        scale,
        alpha,
        minQuantile,
        maxQuantile,
        vector.length);
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
    return invokeOrDelegate(
        recalculateScalarQuantizationOffset$MH,
        () ->
            delegateVectorUtilSupport.recalculateScalarQuantizationOffset(
                vector, oldAlpha, oldMinQuantile, scale, alpha, minQuantile, maxQuantile),
        MemorySegment.ofArray(vector),
        oldAlpha,
        oldMinQuantile,
        scale,
        alpha,
        minQuantile,
        maxQuantile,
        vector.length);
  }

  @Override
  public int filterByScore(
      int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo) {
    return invokeOrDelegate(
        filterByScore$MH,
        () ->
            delegateVectorUtilSupport.filterByScore(
                docBuffer, scoreBuffer, minScoreInclusive, upTo),
        MemorySegment.ofArray(docBuffer),
        MemorySegment.ofArray(scoreBuffer),
        minScoreInclusive,
        upTo);
  }

  @Override
  public float[] l2normalize(float[] v, boolean throwOnZero) {
    invokeOrDelegate(
        l2normalize$MH,
        () -> delegateVectorUtilSupport.l2normalize(v, throwOnZero),
        MemorySegment.ofArray(v),
        (byte) (throwOnZero ? 1 : 0),
        v.length);
    return v;
  }

  @Override
  public void expand8(int[] arr) {
    invokeOrDelegate(
        expand8$MH,
        () -> {
          delegateVectorUtilSupport.expand8(arr);
          return null;
        },
        MemorySegment.ofArray(arr),
        arr.length);
  }
}
