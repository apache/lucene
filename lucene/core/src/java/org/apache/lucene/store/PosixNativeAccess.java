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
import java.util.Optional;
import java.util.logging.Logger;

final class PosixNativeAccess extends NativeAccess {

  private static final Logger LOG = Logger.getLogger(PosixNativeAccess.class.getName());

  // these constants were extracted from glibc and macos header files - luckily they are the same:

  /** No further special treatment. */
  public static final int POSIX_MADV_NORMAL = 0;

  /** Expect random page references. */
  public static final int POSIX_MADV_RANDOM = 1;

  /** Expect sequential page references. */
  public static final int POSIX_MADV_SEQUENTIAL = 2;

  /** Will need these pages. */
  public static final int POSIX_MADV_WILLNEED = 3;

  /** Don't need these pages. */
  public static final int POSIX_MADV_DONTNEED = 4;

  private static final MethodHandle MH$posix_madvise;
  private static final int PAGE_SIZE;

  private static final Optional<NativeAccess> INSTANCE;

  private PosixNativeAccess() {}

  static Optional<NativeAccess> getInstance() {
    return INSTANCE;
  }

  static {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup stdlib = linker.defaultLookup();
    MethodHandle adviseHandle = null;
    int pagesize = -1;
    PosixNativeAccess instance = null;
    try {
      adviseHandle = lookupMadvise(linker, stdlib);
      pagesize = (int) lookupGetPageSize(linker, stdlib).invokeExact();
      instance = new PosixNativeAccess();
    } catch (UnsupportedOperationException uoe) {
      LOG.warning(uoe.getMessage());
    } catch (
        @SuppressWarnings("unused")
        IllegalCallerException ice) {
      LOG.warning(
          String.format(
              Locale.ENGLISH,
              "Lucene has no access to native functions. To enable access to native functions, "
                  + "pass the following on command line: --enable-native-access=%s",
              Optional.ofNullable(PosixNativeAccess.class.getModule().getName())
                  .orElse("ALL-UNNAMED")));
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new AssertionError(e);
    }
    MH$posix_madvise = adviseHandle;
    PAGE_SIZE = pagesize;
    INSTANCE = Optional.ofNullable(instance);
  }

  private static MethodHandle lookupMadvise(Linker linker, SymbolLookup stdlib) {
    return findFunction(
        linker,
        stdlib,
        "posix_madvise",
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_INT));
  }

  private static MethodHandle lookupGetPageSize(Linker linker, SymbolLookup stdlib) {
    return findFunction(linker, stdlib, "getpagesize", FunctionDescriptor.of(ValueLayout.JAVA_INT));
  }

  @SuppressWarnings("restricted") // unsafe functionality is used
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
  public void madvise(MemorySegment segment, ReadAdvice readAdvice) throws IOException {
    final int advice = mapReadAdvice(readAdvice);
    madvise(segment, advice);
  }

  @Override
  public void madviseWillNeed(MemorySegment segment) throws IOException {
    madvise(segment, POSIX_MADV_WILLNEED);
  }

  private void madvise(MemorySegment segment, int advice) throws IOException {
    // Note: madvise is bypassed if the segment should be preloaded via MemorySegment#load.
    if (segment.byteSize() == 0L) {
      return; // empty segments should be excluded, because they may have no address at all
    }
    final int ret;
    try {
      ret = (int) MH$posix_madvise.invokeExact(segment, segment.byteSize(), advice);
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

  private int mapReadAdvice(ReadAdvice readAdvice) {
    return switch (readAdvice) {
      case NORMAL -> POSIX_MADV_NORMAL;
      case RANDOM -> POSIX_MADV_RANDOM;
      case SEQUENTIAL -> POSIX_MADV_SEQUENTIAL;
    };
  }

  @Override
  public int getPageSize() {
    return PAGE_SIZE;
  }
}
