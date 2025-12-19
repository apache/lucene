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
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.logging.Logger;

/**
 * Native method handles for optimized vector operations using Foreign Function and Memory API
 * (FFM).
 *
 * <p>This class provides access to native C implementations of dot product operations from the
 * loaded shared library(.so) which is generated from dotProduct.c with GCC (>= 10). The native
 * library contains multiple optimized implementations:
 *
 * <ul>
 *   <li><code>dot8s()</code>: Runtime-dispatched function that automatically selects the best
 *       available implementation (SVE > NEON > scalar) based on CPU capabilities
 *   <li><code>dot8s_scalar()</code>: Portable scalar implementation with compiler
 *       auto-vectorization
 * </ul>
 *
 * PanamaVectorUtilSupport#dotProduct use this Native C implementation for dot product calculation
 * if system property <b>lucene.useNativeDotProduct=true</b> is passed
 *
 * <p>It Uses <code>Linker.Option.critical(true)</code> for optimal performance by eliminating the
 * overhead of ensuring MemorySegments are allocated off-heap before native calls.
 */
@SuppressWarnings("restricted")
public final class NativeMethodHandles {

  private NativeMethodHandles() {}

  public static final AddressLayout POINTER = ValueLayout.ADDRESS;

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup SYMBOL_LOOKUP;

  @SuppressWarnings("NonFinalStaticField")
  private static boolean isLibraryLoaded;

  static {
    try {
      // Attempt to load the library
      System.loadLibrary("dotProduct");
      isLibraryLoaded = true; // If successful, set the flag to true
    } catch (UnsatisfiedLinkError _) {
      // If the library loading fails, set the flag to false
      isLibraryLoaded = false;
      Logger.getLogger(NativeMethodHandles.class.getName())
          .severe("Failed to load the native library: dotProduct");
    }
    SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
    SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
  }

  private static final FunctionDescriptor dot8sDesc =
      FunctionDescriptor.of(JAVA_INT, POINTER, POINTER, JAVA_INT);

  private static final MethodHandle dot8sMH =
      SYMBOL_LOOKUP
          .find("dot8s")
          .map(addr -> LINKER.downcallHandle(addr, dot8sDesc, Linker.Option.critical(true)))
          .orElse(null);

  private static final MethodHandle simpleDot8sMH =
      SYMBOL_LOOKUP
          .find("dot8s_scalar")
          .map(addr -> LINKER.downcallHandle(addr, dot8sDesc, Linker.Option.critical(true)))
          .orElse(null);

  static final MethodHandle DOT_PRODUCT_IMPL;

  static final MethodHandle SIMPLE_DOT_PRODUCT_IMPL;

  public static boolean isLibraryLoaded() {
    return isLibraryLoaded;
  }

  static {
    if (dot8sMH != null) {
      DOT_PRODUCT_IMPL = dot8sMH;
    } else {
      throw new RuntimeException("C code for dot8s was not linked!");
    }
    if (simpleDot8sMH != null) {
      SIMPLE_DOT_PRODUCT_IMPL = simpleDot8sMH;
    } else {
      throw new RuntimeException("C code for dot8s_scalar was not linked!");
    }
  }
}
