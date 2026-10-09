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
package org.apache.lucene.sandbox.store;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * FFM bindings for {@code liburing-ffi}, plus the few {@code libc} calls that go with them. No
 * pooling or policy.
 *
 * <p>The bindings target {@code liburing-ffi.so.2} rather than {@code liburing.so.2} because most
 * of liburing's API ({@code io_uring_get_sqe}, {@code io_uring_prep_read}, {@code
 * io_uring_cq_advance} and others) is {@code static inline} in {@code liburing.h} and has no symbol
 * in the regular shared object. The {@code -ffi} variant exports out-of-line copies.
 */
@SuppressWarnings("restricted") // FFM downcalls to libc + liburing-ffi
final class LibUring {

  private LibUring() {}

  /** {@code O_RDONLY}: zero on every Linux architecture. */
  static final int O_RDONLY = 0;

  /** {@code -EINTR}: a submit interrupted by a signal, which is retried. */
  static final int NEG_EINTR = -4;

  /**
   * {@code RWF_DONTCACHE} from {@code include/uapi/linux/fs.h} (Linux 6.14+): drop folios once the
   * read completes. Unlike O_DIRECT it has no alignment requirements. An unsupported flag fails
   * with {@code -EOPNOTSUPP} or {@code -EINVAL} rather than being ignored, which {@link #probe}
   * relies on.
   */
  static final int RWF_DONTCACHE = 0x00000080;

  /*
   * Struct sizes and field offsets, from liburing 2.5 on x86_64:
   *
   *   sizeof(struct io_uring)     = 216
   *   sizeof(struct io_uring_sqe) = 64
   *   sizeof(struct io_uring_cqe) = 16
   *   offsetof(sqe, len)          = 24
   *   offsetof(sqe, rw_flags)     = 28
   *   offsetof(sqe, user_data)    = 32
   *   offsetof(cqe, user_data)    = 0
   *   offsetof(cqe, res)          = 8
   *   offsetof(cqe, flags)        = 12
   */

  /**
   * Bytes to allocate for a {@code struct io_uring}. Over-sized, because the installed liburing is
   * bound at run time and the struct may grow.
   */
  static final int RING_BYTES = 512;

  /** {@code sizeof(struct io_uring_sqe)}, for reinterpreting a submission-entry pointer. */
  static final int SQE_BYTES = 64;

  /** {@code sizeof(struct io_uring_cqe)}, for reinterpreting a completion pointer. */
  static final int CQE_BYTES = 16;

  private static final int CQE_RES_OFFSET = 8;

  /**
   * {@code offsetof(struct io_uring_sqe, rw_flags)}. liburing has no setter for this field. A wrong
   * offset would corrupt {@code len} (24) or {@code user_data} (32), which {@link #probe} checks.
   */
  private static final int SQE_RW_FLAGS_OFFSET = 28;

  /**
   * libc and liburing-ffi handles. A separate holder class so that a missing library or symbol is a
   * class-initialization failure that {@link #isAvailable} can catch.
   */
  private static final class Native {
    static final Linker L = Linker.nativeLinker();
    static final SymbolLookup LIBC = L.defaultLookup();
    static final SymbolLookup URING =
        SymbolLookup.libraryLookup("liburing-ffi.so.2", Arena.global());

    static MethodHandle libc(String n, FunctionDescriptor fd) {
      return L.downcallHandle(
          LIBC.find(n)
              .orElseThrow(
                  () -> new UnsupportedOperationException("libc has no symbol '" + n + "'")),
          fd);
    }

    static MethodHandle u(String n, FunctionDescriptor fd) {
      return L.downcallHandle(
          URING
              .find(n)
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException("liburing-ffi has no symbol '" + n + "'")),
          fd);
    }

    /**
     * {@code open} is variadic, but {@code mode} is only read with {@code O_CREAT}, which is never
     * passed. Only the two named parameters are declared, since a fixed third argument would use
     * the wrong calling convention on some architectures.
     */
    static final MethodHandle MH$open =
        libc("open", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));

    static final MethodHandle MH$close = libc("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT));

    static final MethodHandle MH$queue_init =
        u("io_uring_queue_init", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
    static final MethodHandle MH$queue_exit =
        u("io_uring_queue_exit", FunctionDescriptor.ofVoid(ADDRESS));
    static final MethodHandle MH$get_sqe =
        u("io_uring_get_sqe", FunctionDescriptor.of(ADDRESS, ADDRESS));

    /** {@code io_uring_prep_read(sqe, fd, buf, nbytes, offset)}, with an unregistered buffer. */
    static final MethodHandle MH$prep_read =
        u(
            "io_uring_prep_read",
            FunctionDescriptor.ofVoid(ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG));

    static final MethodHandle MH$sqe_set_data64 =
        u("io_uring_sqe_set_data64", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG));
    static final MethodHandle MH$submit =
        u("io_uring_submit", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    static final MethodHandle MH$submit_and_wait =
        u("io_uring_submit_and_wait", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
    static final MethodHandle MH$peek_batch_cqe =
        u("io_uring_peek_batch_cqe", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
    static final MethodHandle MH$cqe_get_data64 =
        u("io_uring_cqe_get_data64", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    static final MethodHandle MH$cq_advance =
        u("io_uring_cq_advance", FunctionDescriptor.ofVoid(ADDRESS, JAVA_INT));
  }

  /*
   * Native calls use invokeExact. A failure is a binding bug, hence AssertionError.
   */

  static int open(MemorySegment path, int flags) {
    try {
      return (int) Native.MH$open.invokeExact(path, flags);
    } catch (Throwable t) {
      throw new AssertionError("open", t);
    }
  }

  static int close(int fd) {
    try {
      return (int) Native.MH$close.invokeExact(fd);
    } catch (Throwable t) {
      throw new AssertionError("close", t);
    }
  }

  static int queueInit(int entries, MemorySegment ring, int flags) {
    try {
      return (int) Native.MH$queue_init.invokeExact(entries, ring, flags);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_init", t);
    }
  }

  static void queueExit(MemorySegment ring) {
    try {
      Native.MH$queue_exit.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_exit", t);
    }
  }

  /**
   * A submission queue entry, or {@link MemorySegment#NULL} if the queue is full. The pointer is
   * reinterpreted so that {@link #setRwFlags} can write to it; the null check must come first,
   * because a reinterpreted null is no longer equal to {@link MemorySegment#NULL}.
   */
  static MemorySegment getSqe(MemorySegment ring) {
    final MemorySegment sqe;
    try {
      sqe = (MemorySegment) Native.MH$get_sqe.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_get_sqe", t);
    }
    if (sqe.address() == 0) {
      return MemorySegment.NULL;
    }
    return sqe.reinterpret(SQE_BYTES);
  }

  static void prepRead(MemorySegment sqe, int fd, MemorySegment buf, int nbytes, long offset) {
    try {
      Native.MH$prep_read.invokeExact(sqe, fd, buf, nbytes, offset);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_prep_read", t);
    }
  }

  static void setData64(MemorySegment sqe, long data) {
    try {
      Native.MH$sqe_set_data64.invokeExact(sqe, data);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_sqe_set_data64", t);
    }
  }

  /** Hands every queued entry to the kernel. Returns the number submitted, or a negative errno. */
  static int submit(MemorySegment ring) {
    try {
      return (int) Native.MH$submit.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_submit", t);
    }
  }

  static int submitAndWait(MemorySegment ring, int waitNr) {
    try {
      return (int) Native.MH$submit_and_wait.invokeExact(ring, waitNr);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_submit_and_wait", t);
    }
  }

  /**
   * Fills {@code cqes} with pointers to up to {@code count} ready completions and returns how many.
   * Makes no system call. The completions stay owned by the ring until {@link #cqAdvance}.
   */
  static int peekBatchCqe(MemorySegment ring, MemorySegment cqes, int count) {
    try {
      return (int) Native.MH$peek_batch_cqe.invokeExact(ring, cqes, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_peek_batch_cqe", t);
    }
  }

  /** Releases {@code count} completions back to the ring. */
  static void cqAdvance(MemorySegment ring, int count) {
    try {
      Native.MH$cq_advance.invokeExact(ring, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cq_advance", t);
    }
  }

  /** The {@code user_data} the submission carried. */
  static long cqeData(MemorySegment cqe) {
    try {
      return (long) Native.MH$cqe_get_data64.invokeExact(cqe);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cqe_get_data64", t);
    }
  }

  /** Bytes read, or a negative errno. */
  static int cqeResult(MemorySegment cqe) {
    return cqe.get(JAVA_INT, CQE_RES_OFFSET);
  }

  /** The {@code i}th completion pointer from a {@link #peekBatchCqe} array, as a usable segment. */
  static MemorySegment cqeAt(MemorySegment cqePtrs, int i) {
    return cqePtrs.getAtIndex(ADDRESS, i).reinterpret(CQE_BYTES);
  }

  /**
   * Sets {@code sqe->rw_flags}. liburing has no helper for it, and {@code io_uring_prep_read}
   * zeroes the whole entry first, so this must be called after preparing the read.
   */
  static void setRwFlags(MemorySegment sqe, int flags) {
    sqe.set(JAVA_INT, SQE_RW_FLAGS_OFFSET, flags);
  }

  /** What this machine actually supports, as established by {@link #probe}. */
  record Support(boolean available, int readFlags) {
    static final Support NONE = new Support(false, 0);

    /** True if reads should carry {@link #RWF_DONTCACHE}. */
    boolean dontcache() {
      return (readFlags & RWF_DONTCACHE) != 0;
    }
  }

  private static volatile Boolean available;

  /**
   * True if liburing-ffi loads, every symbol resolves and a ring initializes. {@link #probe} is the
   * stronger check.
   */
  static boolean isAvailable() {
    Boolean a = available;
    if (a != null) {
      return a;
    }
    synchronized (LibUring.class) {
      if (available != null) {
        return available;
      }
      boolean ok = false;
      try (Arena probe = Arena.ofConfined()) {
        MemorySegment ring = probe.allocate(RING_BYTES, 8);
        if (queueInit(2, ring, 0) == 0) {
          queueExit(ring);
          ok = true;
        }
      } catch (Throwable _) {
        ok = false;
      }
      return available = ok;
    }
  }

  /**
   * Establishes what this kernel, filesystem and set of bindings support, by reading known bytes
   * back through a real ring. Matching bytes and {@code user_data} also check the descriptors and
   * {@link #SQE_RW_FLAGS_OFFSET}.
   *
   * @param scratchDir directory to write the probe file in, normally the index directory
   */
  static Support probe(Path scratchDir) {
    if (isAvailable() == false) {
      return Support.NONE;
    }
    if (readsBack(scratchDir, RWF_DONTCACHE)) {
      return new Support(true, RWF_DONTCACHE);
    }
    if (readsBack(scratchDir, 0)) {
      return new Support(true, 0);
    }
    return Support.NONE;
  }

  /**
   * Writes a known pattern, reads it back through a two-entry ring with {@code rwFlags}, and
   * returns whether the bytes, the length and the {@code user_data} all came back intact.
   */
  private static boolean readsBack(Path scratchDir, int rwFlags) {
    final int len = 4096;
    final long sentinel = 0x5eed_1234_dead_beefL;
    final byte[] expected = new byte[len];
    for (int i = 0; i < len; i++) {
      expected[i] = (byte) (i * 31 + 7);
    }

    Path file = null;
    try {
      file = Files.createTempFile(scratchDir, "uring-probe", ".tmp");
      Files.write(file, expected);

      try (Arena arena = Arena.ofConfined()) {
        MemorySegment ring = arena.allocate(RING_BYTES, 8);
        if (queueInit(2, ring, 0) != 0) {
          return false;
        }
        int fd = open(arena.allocateFrom(file.toString()), O_RDONLY);
        if (fd < 0) {
          queueExit(ring);
          return false;
        }
        try {
          MemorySegment buf = arena.allocate(len);
          MemorySegment cqePtrs = arena.allocate(ADDRESS.byteSize());

          MemorySegment sqe = getSqe(ring);
          if (MemorySegment.NULL.equals(sqe)) {
            return false;
          }
          prepRead(sqe, fd, buf, len, 0L);
          setRwFlags(sqe, rwFlags);
          setData64(sqe, sentinel);

          int rc = submitAndWait(ring, 1);
          if (rc < 0 && rc != NEG_EINTR) {
            return false;
          }
          if (peekBatchCqe(ring, cqePtrs, 1) != 1) {
            return false;
          }
          MemorySegment cqe = cqeAt(cqePtrs, 0);
          long data = cqeData(cqe);
          int res = cqeResult(cqe);
          cqAdvance(ring, 1);
          if (data != sentinel || res != len) {
            return false;
          }
          byte[] actual = new byte[len];
          MemorySegment.copy(buf, JAVA_BYTE, 0, actual, 0, len);
          return Arrays.equals(expected, actual);
        } finally {
          close(fd);
          queueExit(ring);
        }
      }
    } catch (Throwable _) {
      return false;
    } finally {
      if (file != null) {
        try {
          Files.deleteIfExists(file);
        } catch (IOException _) {
          // best effort
        }
      }
    }
  }
}
