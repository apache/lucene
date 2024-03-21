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
package org.apache.lucene.store;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;
import java.util.logging.Logger;

@SuppressWarnings("preview")
final class PosixNativeAccess extends NativeAccess {

  private static final Logger LOG = Logger.getLogger(PosixNativeAccess.class.getName());

  private static final MethodHandle mh$posix_madvise = lookupmadvise();

  private static final PosixNativeAccess INSTANCE = new PosixNativeAccess();

  private PosixNativeAccess() {}

  static PosixNativeAccess getInstance() {
    return INSTANCE;
  }

  private static MethodHandle lookupmadvise() {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup stdlib = linker.defaultLookup();
    final MethodHandle mh =
        findFunction(
            linker,
            stdlib,
            "posix_madvise",
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));
    LOG.info("posix_madvise() available on this platform");
    return mh;
  }

  private static MethodHandle findFunction(
      Linker linker, SymbolLookup lookup, String name, FunctionDescriptor desc) {
    final MemorySegment symbol =
        lookup
            .find(name)
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Platform has no symbol for '" + name + "' in libc."));
    return linker.downcallHandle(symbol, desc);
  }

  @Override
  public void madvise(MemorySegment segment, int advice) throws IOException {
    final int ret;
    try {
      ret = (int) mh$posix_madvise.invokeExact(segment, segment.byteSize(), advice);
    } catch (Throwable th) {
      throw new AssertionError(th);
    }
    if (ret != 0) {
      throw new IOException(
          String.format(
              Locale.ENGLISH,
              "Call to posix_madvise with address=0x%08X and byteSize=%d failed with return code %d.",
              segment.address(),
              segment.byteSize(),
              ret));
    }
  }
}
