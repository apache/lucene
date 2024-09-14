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
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

public final class NativeMethodHandles {

  private NativeMethodHandles() {}

  public static final AddressLayout POINTER =
      ValueLayout.ADDRESS.withTargetLayout(MemoryLayout.sequenceLayout(JAVA_BYTE));

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup SYMBOL_LOOKUP;

  static {
    System.loadLibrary("dotProduct");
    SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
    SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
  }

  private static final FunctionDescriptor dot8sDesc =
      FunctionDescriptor.of(JAVA_INT, POINTER, POINTER, JAVA_INT);

  private static final MethodHandle dot8sMH =
      SYMBOL_LOOKUP.find("dot8s").map(addr -> LINKER.downcallHandle(addr, dot8sDesc)).orElse(null);

  private static final MethodHandle neonVdot8sMH =
      SYMBOL_LOOKUP
          .find("vdot8s_neon")
          .map(addr -> LINKER.downcallHandle(addr, dot8sDesc))
          .orElse(null);

  private static final MethodHandle sveVdot8sMH =
      SYMBOL_LOOKUP
          .find("vdot8s_sve")
          .map(addr -> LINKER.downcallHandle(addr, dot8sDesc))
          .orElse(null);

  /* chosen C implementation */
  static final MethodHandle DOT_PRODUCT_IMPL;

  static {
    if (sveVdot8sMH != null) {
      DOT_PRODUCT_IMPL = sveVdot8sMH;
    } else if (neonVdot8sMH != null) {
      DOT_PRODUCT_IMPL = neonVdot8sMH;
    } else if (dot8sMH != null) {
      DOT_PRODUCT_IMPL = dot8sMH;
    } else {
      throw new RuntimeException("c code was not linked!");
    }
  }
}
