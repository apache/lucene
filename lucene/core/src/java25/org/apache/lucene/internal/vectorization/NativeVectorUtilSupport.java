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
import java.util.function.BiFunction;
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
 *
 * @lucene.experimental
 */
@SuppressWarnings("restricted")
final class NativeVectorUtilSupport implements VectorUtilSupport {

  private final VectorUtilSupport delegateVectorUtilSupport;
  // Null selects the static final production handles, which the JIT can inline. Tests supply
  // instance handles while exercising the same argument conversion, invocation and fallback code.
  private final NativeFunctions nativeFunctions;

  public static final AddressLayout POINTER = ValueLayout.ADDRESS;

  // TODO: Make this dynamic?
  public static final String NATIVE_VECTOR_LIBRARY_NAME = "dotProduct";

  public NativeVectorUtilSupport(VectorUtilSupport vectorUtilSupport) {
    this(vectorUtilSupport, null);
  }

  // Visible for tests so Java-backed method handles can exercise dispatch without a native library.
  NativeVectorUtilSupport(VectorUtilSupport vectorUtilSupport, NativeFunctions nativeFunctions) {
    this.delegateVectorUtilSupport = vectorUtilSupport;
    this.nativeFunctions = nativeFunctions;
  }

  public static boolean isLibraryLoaded() {
    return NativeLibrary.LOADED;
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

  /** The handles used by one support instance; null handles select the Java fallback. */
  static final class NativeFunctions {
    private final MethodHandle dotProduct;
    private final MethodHandle squareDistance;
    private final MethodHandle cosine;
    private final MethodHandle dotProductFloat;
    private final MethodHandle squareDistanceFloat;
    private final MethodHandle cosineFloat;
    private final MethodHandle int4SquareDistance;
    private final MethodHandle int4SquareDistanceSinglePacked;
    private final MethodHandle int4SquareDistanceBothPacked;
    private final MethodHandle uint8SquareDistance;
    private final MethodHandle uint8DotProduct;
    private final MethodHandle int4DotProduct;
    private final MethodHandle int4DotProductSinglePacked;
    private final MethodHandle int4DotProductBothPacked;
    private final MethodHandle int4BitDotProduct;
    private final MethodHandle int4DibitDotProduct;
    private final MethodHandle minMaxScalarQuantize;
    private final MethodHandle recalculateScalarQuantizationOffset;
    private final MethodHandle filterByScore;
    private final MethodHandle l2normalize;
    private final MethodHandle expand8;
    private final MethodHandle findNextGEQ;

    NativeFunctions(BiFunction<String, FunctionDescriptor, MethodHandle> handleProvider) {
      dotProduct = handleProvider.apply("dotProduct", twoPointerIntToInt);
      squareDistance = handleProvider.apply("squareDistance", twoPointerIntToInt);
      // Byte cosine returns a float, unlike byte dot product and square distance.
      cosine = handleProvider.apply("cosine", twoPointerIntToFloat);
      dotProductFloat = handleProvider.apply("dotProductFloat", twoPointerIntToFloat);
      squareDistanceFloat = handleProvider.apply("squareDistanceFloat", twoPointerIntToFloat);
      cosineFloat = handleProvider.apply("cosineFloat", twoPointerIntToFloat);
      int4SquareDistance = handleProvider.apply("int4SquareDistance", twoPointerIntToInt);
      int4SquareDistanceSinglePacked =
          handleProvider.apply("int4SquareDistanceSinglePacked", twoPointerIntToInt);
      int4SquareDistanceBothPacked =
          handleProvider.apply("int4SquareDistanceBothPacked", twoPointerIntToInt);
      uint8SquareDistance = handleProvider.apply("uint8SquareDistance", twoPointerIntToInt);
      uint8DotProduct = handleProvider.apply("uint8DotProduct", twoPointerIntToInt);
      int4DotProduct = handleProvider.apply("int4DotProduct", twoPointerIntToInt);
      int4DotProductSinglePacked =
          handleProvider.apply("int4DotProductSinglePacked", twoPointerIntToInt);
      int4DotProductBothPacked =
          handleProvider.apply("int4DotProductBothPacked", twoPointerIntToInt);
      int4BitDotProduct = handleProvider.apply("int4BitDotProduct", twoPointerIntToLong);
      int4DibitDotProduct = handleProvider.apply("int4DibitDotProduct", twoPointerIntToLong);
      minMaxScalarQuantize = handleProvider.apply("minMaxScalarQuantize", minMaxScalarQuantizeDesc);
      recalculateScalarQuantizationOffset =
          handleProvider.apply("recalculateScalarQuantizationOffset", recalculateOffsetDesc);
      filterByScore = handleProvider.apply("filterByScore", filterByScoreDesc);
      l2normalize = handleProvider.apply("l2normalize", l2normalizeDesc);
      expand8 = handleProvider.apply("expand8", expand8Desc);
      findNextGEQ = handleProvider.apply("findNextGEQ", findNextGEQDesc);
    }
  }

  // Keep native linking lazy so injected Java handles do not require a library or native access.
  private static final class NativeLibrary {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final boolean LOADED = loadLibrary();
    private static final NativeFunctions FUNCTIONS = loadFunctions();

    // Downcall handles must be constants at production call sites for the JIT to inline them.
    private static final MethodHandle dotProduct = FUNCTIONS.dotProduct;
    private static final MethodHandle squareDistance = FUNCTIONS.squareDistance;
    private static final MethodHandle cosine = FUNCTIONS.cosine;
    private static final MethodHandle dotProductFloat = FUNCTIONS.dotProductFloat;
    private static final MethodHandle squareDistanceFloat = FUNCTIONS.squareDistanceFloat;
    private static final MethodHandle cosineFloat = FUNCTIONS.cosineFloat;
    private static final MethodHandle int4SquareDistance = FUNCTIONS.int4SquareDistance;
    private static final MethodHandle int4SquareDistanceSinglePacked =
        FUNCTIONS.int4SquareDistanceSinglePacked;
    private static final MethodHandle int4SquareDistanceBothPacked =
        FUNCTIONS.int4SquareDistanceBothPacked;
    private static final MethodHandle uint8SquareDistance = FUNCTIONS.uint8SquareDistance;
    private static final MethodHandle uint8DotProduct = FUNCTIONS.uint8DotProduct;
    private static final MethodHandle int4DotProduct = FUNCTIONS.int4DotProduct;
    private static final MethodHandle int4DotProductSinglePacked =
        FUNCTIONS.int4DotProductSinglePacked;
    private static final MethodHandle int4DotProductBothPacked = FUNCTIONS.int4DotProductBothPacked;
    private static final MethodHandle int4BitDotProduct = FUNCTIONS.int4BitDotProduct;
    private static final MethodHandle int4DibitDotProduct = FUNCTIONS.int4DibitDotProduct;
    private static final MethodHandle minMaxScalarQuantize = FUNCTIONS.minMaxScalarQuantize;
    private static final MethodHandle recalculateScalarQuantizationOffset =
        FUNCTIONS.recalculateScalarQuantizationOffset;
    private static final MethodHandle filterByScore = FUNCTIONS.filterByScore;
    private static final MethodHandle l2normalize = FUNCTIONS.l2normalize;
    private static final MethodHandle expand8 = FUNCTIONS.expand8;
    private static final MethodHandle findNextGEQ = FUNCTIONS.findNextGEQ;

    private static boolean loadLibrary() {
      try {
        System.loadLibrary(NATIVE_VECTOR_LIBRARY_NAME);
        return true;
      } catch (UnsatisfiedLinkError e) {
        Logger.getLogger(NativeVectorUtilSupport.class.getName())
            .warning(
                "No native library" + NATIVE_VECTOR_LIBRARY_NAME + " found : " + e.getMessage());
        return false;
      }
    }

    private static NativeFunctions loadFunctions() {
      if (LOADED) {
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        SymbolLookup symbolLookup =
            name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
        return new NativeFunctions(
            (name, descriptor) -> getMethodHandle(symbolLookup, name, descriptor));
      } else if (Constants.NATIVE_DOT_PRODUCT_ENABLED) {
        throw new RuntimeException("Native library dotProduct missing!");
      }
      return new NativeFunctions((_, _) -> null);
    }

    private static MethodHandle getMethodHandle(
        SymbolLookup symbolLookup, String methodName, FunctionDescriptor descriptor) {
      MethodHandle mh =
          symbolLookup
              .find(methodName)
              .map(addr -> LINKER.downcallHandle(addr, descriptor, Linker.Option.critical(true)))
              .orElse(null);
      if (mh == null && Constants.NATIVE_STRICT_MODE) {
        throw new RuntimeException("C code for " + methodName + " was not linked!");
      }
      return mh;
    }
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
        // TODO: This is slow and we should avoid dynamic invocations and improve the test coverage
        // (https://github.com/apache/lucene/issues/15840)
        return (T) mh.invokeWithArguments(args);
      } catch (Throwable ex) {
        throw new AssertionError("should not reach here", ex);
      }
    }
    return delegate.get();
  }

  public static float cosine(byte[] a, MemorySegment b) {
    return (NativeLibrary.cosine != null)
        ? invokeFloatMethodHandle(NativeLibrary.cosine, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.cosine(a, b);
  }

  public static float cosine(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.cosine != null)
        ? invokeFloatMethodHandle(NativeLibrary.cosine, a, b)
        : PanamaVectorUtilSupport.cosine(a, b);
  }

  public static int dotProduct(byte[] a, MemorySegment b) {
    return (NativeLibrary.dotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.dotProduct, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int dotProduct(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.dotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.dotProduct, a, b)
        : PanamaVectorUtilSupport.dotProduct(a, b);
  }

  public static int squareDistance(byte[] a, MemorySegment b) {
    return (NativeLibrary.squareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.squareDistance, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int squareDistance(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.squareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.squareDistance, a, b)
        : PanamaVectorUtilSupport.squareDistance(a, b);
  }

  public static int int4SquareDistance(byte[] a, MemorySegment b) {
    return (NativeLibrary.int4SquareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.int4SquareDistance, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistance(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.int4SquareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.int4SquareDistance, a, b)
        : PanamaVectorUtilSupport.int4SquareDistance(a, b);
  }

  public static int int4SquareDistanceSinglePacked(byte[] a, MemorySegment b) {
    return (NativeLibrary.int4SquareDistanceSinglePacked != null)
        ? invokeIntMethodHandle(
            NativeLibrary.int4SquareDistanceSinglePacked, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4SquareDistanceSinglePacked(a, b);
  }

  public static int uint8SquareDistance(byte[] a, MemorySegment b) {
    return (NativeLibrary.uint8SquareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.uint8SquareDistance, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8SquareDistance(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.uint8SquareDistance != null)
        ? invokeIntMethodHandle(NativeLibrary.uint8SquareDistance, a, b)
        : PanamaVectorUtilSupport.uint8SquareDistance(a, b);
  }

  public static int uint8DotProduct(byte[] a, MemorySegment b) {
    return (NativeLibrary.uint8DotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.uint8DotProduct, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int uint8DotProduct(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.uint8DotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.uint8DotProduct, a, b)
        : PanamaVectorUtilSupport.uint8DotProduct(a, b);
  }

  public static int int4DotProduct(byte[] a, MemorySegment b) {
    return (NativeLibrary.int4DotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.int4DotProduct, MemorySegment.ofArray(a), b)
        : PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProduct(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.int4DotProduct != null)
        ? invokeIntMethodHandle(NativeLibrary.int4DotProduct, a, b)
        : PanamaVectorUtilSupport.int4DotProduct(a, b);
  }

  public static int int4DotProductSinglePacked(byte[] unpacked, MemorySegment packed) {
    return (NativeLibrary.int4DotProductSinglePacked != null)
        ? invokeIntMethodHandle(
            NativeLibrary.int4DotProductSinglePacked, MemorySegment.ofArray(unpacked), packed)
        : PanamaVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  public static int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.int4SquareDistanceBothPacked != null)
        ? invokeIntMethodHandle(NativeLibrary.int4SquareDistanceBothPacked, a, b)
        : PanamaVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  public static int int4DotProductBothPacked(MemorySegment a, MemorySegment b) {
    return (NativeLibrary.int4DotProductBothPacked != null)
        ? invokeIntMethodHandle(NativeLibrary.int4DotProductBothPacked, a, b)
        : PanamaVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public float dotProduct(float[] a, float[] b) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.dotProductFloat : nativeFunctions.dotProductFloat;
    return (mh != null)
        ? invokeFloatMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public float cosine(float[] v1, float[] v2) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.cosineFloat : nativeFunctions.cosineFloat;
    return (mh != null)
        ? invokeFloatMethodHandle(mh, MemorySegment.ofArray(v1), MemorySegment.ofArray(v2))
        : delegateVectorUtilSupport.cosine(v1, v2);
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.squareDistanceFloat
            : nativeFunctions.squareDistanceFloat;
    return (mh != null)
        ? invokeFloatMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.dotProduct : nativeFunctions.dotProduct;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public int int4DotProduct(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.int4DotProduct : nativeFunctions.int4DotProduct;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4DotProduct(a, b);
  }

  @Override
  public int int4DotProductSinglePacked(byte[] unpacked, byte[] packed) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4DotProductSinglePacked
            : nativeFunctions.int4DotProductSinglePacked;
    return mh != null
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(unpacked), MemorySegment.ofArray(packed))
        : delegateVectorUtilSupport.int4DotProductSinglePacked(unpacked, packed);
  }

  @Override
  public int int4DotProductBothPacked(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4DotProductBothPacked
            : nativeFunctions.int4DotProductBothPacked;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4DotProductBothPacked(a, b);
  }

  @Override
  public void int4Unpack(byte[] packed, byte[] unpacked) {
    delegateVectorUtilSupport.int4Unpack(packed, unpacked);
  }

  @Override
  public int uint8DotProduct(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.uint8DotProduct : nativeFunctions.uint8DotProduct;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.uint8DotProduct(a, b);
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    MethodHandle mh = nativeFunctions == null ? NativeLibrary.cosine : nativeFunctions.cosine;
    return (mh != null)
        ? invokeFloatMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.cosine(a, b);
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.squareDistance : nativeFunctions.squareDistance;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public int int4SquareDistance(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4SquareDistance
            : nativeFunctions.int4SquareDistance;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4SquareDistance(a, b);
  }

  @Override
  public int int4SquareDistanceSinglePacked(byte[] unpacked, byte[] packed) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4SquareDistanceSinglePacked
            : nativeFunctions.int4SquareDistanceSinglePacked;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(unpacked), MemorySegment.ofArray(packed))
        : delegateVectorUtilSupport.int4SquareDistanceSinglePacked(unpacked, packed);
  }

  @Override
  public int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4SquareDistanceBothPacked
            : nativeFunctions.int4SquareDistanceBothPacked;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.int4SquareDistanceBothPacked(a, b);
  }

  @Override
  public int uint8SquareDistance(byte[] a, byte[] b) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.uint8SquareDistance
            : nativeFunctions.uint8SquareDistance;
    return (mh != null)
        ? invokeIntMethodHandle(mh, MemorySegment.ofArray(a), MemorySegment.ofArray(b))
        : delegateVectorUtilSupport.uint8SquareDistance(a, b);
  }

  @Override
  public long int4BitDotProduct(byte[] int4Quantized, byte[] binaryQuantized) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4BitDotProduct
            : nativeFunctions.int4BitDotProduct;
    if (mh != null) {
      return invokeLongMethodHandle(
          mh, MemorySegment.ofArray(int4Quantized), MemorySegment.ofArray(binaryQuantized));
    }
    return delegateVectorUtilSupport.int4BitDotProduct(int4Quantized, binaryQuantized);
  }

  @Override
  public long int4DibitDotProduct(byte[] int4Quantized, byte[] dibitQuantized) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.int4DibitDotProduct
            : nativeFunctions.int4DibitDotProduct;
    if (mh != null) {
      return invokeLongMethodHandle(
          mh, MemorySegment.ofArray(int4Quantized), MemorySegment.ofArray(dibitQuantized));
    }
    return delegateVectorUtilSupport.int4DibitDotProduct(int4Quantized, dibitQuantized);
  }

  @Override
  public int findNextGEQ(int[] buffer, int target, int from, int to) {
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.findNextGEQ : nativeFunctions.findNextGEQ;
    return invokeOrDelegate(
        mh,
        () -> delegateVectorUtilSupport.findNextGEQ(buffer, target, from, to),
        MemorySegment.ofArray(buffer),
        target,
        from,
        to);
  }

  @Override
  public float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile) {
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.minMaxScalarQuantize
            : nativeFunctions.minMaxScalarQuantize;
    return invokeOrDelegate(
        mh,
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
    MethodHandle mh =
        nativeFunctions == null
            ? NativeLibrary.recalculateScalarQuantizationOffset
            : nativeFunctions.recalculateScalarQuantizationOffset;
    return invokeOrDelegate(
        mh,
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
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.filterByScore : nativeFunctions.filterByScore;
    return invokeOrDelegate(
        mh,
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
    MethodHandle mh =
        nativeFunctions == null ? NativeLibrary.l2normalize : nativeFunctions.l2normalize;
    invokeOrDelegate(
        mh,
        () -> delegateVectorUtilSupport.l2normalize(v, throwOnZero),
        MemorySegment.ofArray(v),
        (byte) (throwOnZero ? 1 : 0),
        v.length);
    return v;
  }

  @Override
  public float dotProduct(short[] a, short[] b) {
    return delegateVectorUtilSupport.dotProduct(a, b);
  }

  @Override
  public float cosine(short[] v1, short[] v2) {
    return delegateVectorUtilSupport.cosine(v1, v2);
  }

  @Override
  public float squareDistance(short[] a, short[] b) {
    return delegateVectorUtilSupport.squareDistance(a, b);
  }

  @Override
  public void expand8(int[] arr) {
    MethodHandle mh = nativeFunctions == null ? NativeLibrary.expand8 : nativeFunctions.expand8;
    invokeOrDelegate(
        mh,
        () -> {
          delegateVectorUtilSupport.expand8(arr);
          return null;
        },
        MemorySegment.ofArray(arr),
        arr.length);
  }
}
