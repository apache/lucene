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
import java.util.OptionalLong;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;

@SuppressWarnings("preview")
final class PosixNativeAccess extends NativeAccess {

  private static final Logger LOG = Logger.getLogger(NativeAccess.class.getName());

  private final int _SC_PAGESIZE = 30; // only valid on Linux, on MacOS it is 29 !?!

  private final MethodHandle mh$posix_madvise;
  private final OptionalLong pageSize;

  PosixNativeAccess() {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup stdlib = linker.defaultLookup();
    if (Constants.LINUX) {
      final MethodHandle mh$sysconf =
          findFunction(
              linker,
              stdlib,
              "sysconf",
              FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
      final long ps;
      try {
        ps = (long) mh$sysconf.invokeExact(_SC_PAGESIZE);
      } catch (Throwable th) {
        throw new AssertionError(th);
      }
      if (ps <= 0) {
        throw new UnsupportedOperationException(
            "Can't get the page size; madvise() is not possible.");
      }
      this.pageSize = OptionalLong.of(ps);
    } else {
      this.pageSize = OptionalLong.empty();
    }
    this.mh$posix_madvise =
        findFunction(
            linker,
            stdlib,
            "posix_madvise",
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));
    LOG.info("madvise() available on this platform");
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
  public void madvise(MemorySegment segment, int advise) throws IOException {
    if (pageSize.isPresent() && segment.address() % pageSize.getAsLong() != 0) {
      // for testing we have very small chunk sizes, so FileChannel#map does not return correctly
      // aligned
      // TODO: remove the warning after testing
      LOG.warning(
          String.format(
              Locale.ENGLISH,
              "Too small chunksize to have correctly aligned segments, address (address=0x%08X) is not aligned at pageSize=%d",
              segment.address(),
              pageSize.getAsLong()));
      return;
    }
    final int ret;
    try {
      ret = (int) mh$posix_madvise.invokeExact(segment, segment.byteSize(), advise);
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
