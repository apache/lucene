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

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads through io_uring via {@code liburing-ffi} (pure FFM; liburing owns the ring mmap, memory
 * ordering and arch specifics). Reads are started by one call and consumed by another, so that a
 * caller which starts several before consuming any gets that many in flight. Package-private engine
 * behind {@link IoUringUncachedDirectory}.
 *
 * <p>Reads carry {@link #RWF_DONTCACHE}, so the file is opened without {@code O_DIRECT} and nothing
 * about the offset, length or buffer needs to be block aligned.
 */
@SuppressWarnings("restricted") // FFM downcalls to libc + liburing-ffi
final class IoUring implements Closeable {

  private static final int O_RDONLY = 0;

  /**
   * Per-read flag asking the kernel to drop the pages it used once the read completes: buffered I/O
   * semantics without leaving the data behind in the page cache. Requires Linux 6.14 or later.
   */
  static final int RWF_DONTCACHE = 0x00000080;

  /** Field offsets and sizes from the io_uring ABI. */
  private static final long SQE_BYTES = 64L;

  private static final long SQE_RW_FLAGS_OFFSET = 28L;
  private static final long CQE_BYTES = 16L;
  private static final long CQE_RES_OFFSET = 8L;

  /** {@code EOPNOTSUPP}, which is what a kernel returns for an rw_flag it does not know. */
  private static final int ENOTSUP = 95;

  /**
   * Queued reads that are submitted together without waiting. Without this, reads queued by
   * prefetch reach the kernel only when a later read blocks, so their I/O does not overlap the
   * scoring of the reads before them.
   */
  private static final int SUBMIT_BATCH = 32;

  private static final byte SLOT_FREE = 0;
  private static final byte SLOT_PENDING = 1;
  private static final byte SLOT_DONE = 2;

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

    static final MethodHandle MH$open =
        libc("open", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT));
    static final MethodHandle MH$close = libc("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
    static final MethodHandle MH$posix_fadvise =
        libc(
            "posix_fadvise",
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT));
    static final MethodHandle MH$queue_init =
        u("io_uring_queue_init", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
    static final MethodHandle MH$queue_init_params =
        u(
            "io_uring_queue_init_params",
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS));
    static final MethodHandle MH$queue_exit =
        u("io_uring_queue_exit", FunctionDescriptor.ofVoid(ADDRESS));
    static final MethodHandle MH$get_sqe =
        u("io_uring_get_sqe", FunctionDescriptor.of(ADDRESS, ADDRESS));
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
   * Every native call goes through a small wrapper using invokeExact with the handle's precise
   * signature: unlike invokeWithArguments it neither boxes arguments nor type-checks them at run
   * time, which matters because the read path makes several of these calls per vector. A failure
   * here is a binding bug, not a condition callers can handle, hence AssertionError.
   */

  private static int open(MemorySegment path, int flags) {
    try {
      return (int) Native.MH$open.invokeExact(path, flags, 0);
    } catch (Throwable t) {
      throw new AssertionError("open", t);
    }
  }

  /** {@code POSIX_FADV_RANDOM}: no readahead for reads through this descriptor. */
  private static final int POSIX_FADV_RANDOM = 1;

  private static int fadvise(int fd, int advice) {
    try {
      return (int) Native.MH$posix_fadvise.invokeExact(fd, 0L, 0L, advice);
    } catch (Throwable t) {
      throw new AssertionError("posix_fadvise", t);
    }
  }

  private static int libcClose(int fd) {
    try {
      return (int) Native.MH$close.invokeExact(fd);
    } catch (Throwable t) {
      throw new AssertionError("close", t);
    }
  }

  /**
   * Asks the kernel to let this ring share another ring's async worker pool rather than starting
   * its own. Reads that cannot complete without blocking are handed to those workers, so without
   * sharing a thread whose leaf has many candidates is limited to its own ring's workers while
   * another ring's sit idle -- which is exactly the imbalance a per-ring pool creates.
   */
  private static final int IORING_SETUP_ATTACH_WQ = 32;

  /** Offsets in {@code struct io_uring} and {@code struct io_uring_params}, liburing 2.6. */
  private static final long RING_FD_OFFSET = 196;

  private static final long PARAMS_BYTES = 120;
  private static final long PARAMS_FLAGS_OFFSET = 8;
  private static final long PARAMS_WQ_FD_OFFSET = 24;

  private static int queueInitAttached(int entries, MemorySegment ring, int wqFd) {
    try (Arena a = Arena.ofConfined()) {
      MemorySegment params = a.allocate(PARAMS_BYTES);
      params.set(JAVA_INT, PARAMS_FLAGS_OFFSET, IORING_SETUP_ATTACH_WQ);
      params.set(JAVA_INT, PARAMS_WQ_FD_OFFSET, wqFd);
      try {
        return (int) Native.MH$queue_init_params.invokeExact(entries, ring, params);
      } catch (Throwable t) {
        throw new AssertionError("io_uring_queue_init_params", t);
      }
    }
  }

  private static int ringFd(MemorySegment ring) {
    return ring.get(JAVA_INT, RING_FD_OFFSET);
  }

  private static int queueInit(int entries, MemorySegment ring, int flags) {
    try {
      return (int) Native.MH$queue_init.invokeExact(entries, ring, flags);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_init", t);
    }
  }

  private static void queueExit(MemorySegment ring) {
    try {
      Native.MH$queue_exit.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_exit", t);
    }
  }

  private static MemorySegment getSqe(MemorySegment ring) {
    try {
      MemorySegment sqe = (MemorySegment) Native.MH$get_sqe.invokeExact(ring);
      // A pointer from native code arrives as a zero-length segment; give it the SQE's extent so
      // that rw_flags can be written.
      return sqe.equals(MemorySegment.NULL) ? null : sqe.reinterpret(SQE_BYTES);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_get_sqe", t);
    }
  }

  private static void prepRead(
      MemorySegment sqe, int fd, MemorySegment buf, int nbytes, long offset) {
    try {
      Native.MH$prep_read.invokeExact(sqe, fd, buf, nbytes, offset);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_prep_read", t);
    }
  }

  private static void setData64(MemorySegment sqe, long data) {
    try {
      Native.MH$sqe_set_data64.invokeExact(sqe, data);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_sqe_set_data64", t);
    }
  }

  private static int submit(MemorySegment ring) {
    try {
      return (int) Native.MH$submit.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_submit", t);
    }
  }

  private static int submitAndWait(MemorySegment ring, int waitNr) {
    try {
      return (int) Native.MH$submit_and_wait.invokeExact(ring, waitNr);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_submit_and_wait", t);
    }
  }

  private static int peekBatchCqe(MemorySegment ring, MemorySegment cqes, int count) {
    try {
      return (int) Native.MH$peek_batch_cqe.invokeExact(ring, cqes, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_peek_batch_cqe", t);
    }
  }

  private static long cqeGetData64(MemorySegment cqe) {
    try {
      return (long) Native.MH$cqe_get_data64.invokeExact(cqe);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cqe_get_data64", t);
    }
  }

  private static void cqAdvance(MemorySegment ring, int count) {
    try {
      Native.MH$cq_advance.invokeExact(ring, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cq_advance", t);
    }
  }

  private static volatile Boolean available;

  /** True iff liburing-ffi loads, symbols resolve, and a probe ring initializes. */
  static boolean isAvailable() {
    Boolean a = available;
    if (a != null) {
      return a;
    }
    synchronized (IoUring.class) {
      if (available != null) {
        return available;
      }
      boolean ok = false;
      try (Arena probe = Arena.ofConfined()) {
        MemorySegment ring = probe.allocate(512);
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

  private final int qd;
  private final int maxRings;
  private final int maxRead;
  private final AtomicInteger openFds = new AtomicInteger();

  /** Rings nobody is using. A ring is only ever driven by one thread at a time. */
  private final ArrayBlockingQueue<Reads> idle;

  private final AtomicInteger created = new AtomicInteger();

  /** Descriptor of the ring that owns the shared worker pool; -1 until the first ring exists. */
  private final AtomicInteger primaryRingFd = new AtomicInteger(-1);

  /** Rings that had to fall back to a private worker pool. Visible for testing. */
  private final AtomicInteger attachFailures = new AtomicInteger();

  /**
   * The ring on loan to each thread, for as long as it has reads outstanding. A ring is taken on
   * the first read of a batch and handed back once the batch is fully consumed, so the number of
   * rings tracks how many batches are in flight rather than how many threads have ever read a
   * vector, and a thread that goes away leaves nothing behind.
   */
  private final ConcurrentHashMap<Long, Reads> onLoan = new ConcurrentHashMap<>();

  private volatile boolean closed;

  /**
   * Whether this kernel honours {@link #RWF_DONTCACHE}, decided once against a real descriptor.
   * Reads still go through io_uring without it, but they leave their pages in the page cache, so a
   * caller that depends on not disturbing the cache needs to know.
   */
  private volatile Boolean dontCache;

  /**
   * @param queueDepth reads a ring may keep in flight, rounded up to a power of two
   * @param maxRings rings shared between concurrent batches
   * @param maxReadBytes largest read to track; anything bigger is left to the caller. A ring's
   *     buffers follow the size of the reads it actually serves, so the engine holds about {@code
   *     maxRings * queueDepth * vectorBytes} and this only caps how large a served read may be.
   */
  IoUring(int queueDepth, int maxRings, int maxReadBytes) {
    if (isAvailable() == false) {
      throw new IllegalStateException("io_uring unavailable");
    }
    if (maxRings < 1) {
      throw new IllegalArgumentException("maxRings must be >= 1, got " + maxRings);
    }
    if (maxReadBytes < 1) {
      throw new IllegalArgumentException("maxReadBytes must be >= 1, got " + maxReadBytes);
    }
    this.qd = Integer.highestOneBit(Math.max(queueDepth, 2) - 1) << 1;
    this.maxRings = maxRings;
    this.maxRead = maxReadBytes;
    this.idle = new ArrayBlockingQueue<>(maxRings);
  }

  /** Rings that could not attach to the shared worker pool and started their own. */
  int attachFailureCount() {
    return attachFailures.get();
  }

  /** Largest read this engine will track; bigger reads have to be served by the caller. */
  int maxReadBytes() {
    return maxRead;
  }

  /**
   * Opens an ordinary read-only fd. Reads through it are buffered, so nothing has to be block
   * aligned; {@link #RWF_DONTCACHE} is what keeps the pages out of the page cache. The caller owns
   * the fd and must {@link #closeFd} it, which is what lets a merged-away segment release the file
   * instead of pinning it for the directory's lifetime.
   */
  int openPlain(String path) throws IOException {
    try (Arena a = Arena.ofConfined()) {
      int fd = open(a.allocateFrom(path), O_RDONLY);
      if (fd < 0) {
        throw new IOException("open failed for " + path);
      }
      // Reads through this descriptor follow its own readahead state, not the advice on the
      // delegate's mapping, so a read next to cached pages could otherwise read ahead.
      fadvise(fd, POSIX_FADV_RANDOM);
      openFds.incrementAndGet();
      return fd;
    }
  }

  /**
   * Whether reads on this engine are uncached. Probed once by reading through {@code fd} with the
   * flag set: the flag was added in Linux 6.14, and an older kernel fails every read that carries
   * it rather than ignoring it, so this has to be settled before any query runs.
   */
  boolean isUncached(int fd) {
    Boolean known = dontCache;
    if (known != null) {
      return known;
    }
    synchronized (this) {
      if (dontCache != null) {
        return dontCache;
      }
      boolean ok = false;
      try (Arena a = Arena.ofConfined()) {
        MemorySegment ring = a.allocate(512);
        if (queueInit(2, ring, 0) == 0) {
          try {
            MemorySegment buf = a.allocate(maxRead);
            MemorySegment sqe = getSqe(ring);
            if (sqe != null) {
              prepRead(sqe, fd, buf, maxRead, 0L);
              sqe.set(JAVA_INT, SQE_RW_FLAGS_OFFSET, RWF_DONTCACHE);
              setData64(sqe, 0L);
              if (submitAndWait(ring, 1) >= 0) {
                MemorySegment cqes = a.allocate(ADDRESS.byteSize());
                if (peekBatchCqe(ring, cqes, 1) == 1) {
                  int res =
                      cqes.getAtIndex(ADDRESS, 0)
                          .reinterpret(CQE_BYTES)
                          .get(JAVA_INT, CQE_RES_OFFSET);
                  ok = res != -ENOTSUP;
                  cqAdvance(ring, 1);
                }
              }
            }
          } finally {
            queueExit(ring);
          }
        }
      } catch (Throwable _) {
        ok = false;
      }
      return dontCache = ok;
    }
  }

  /** Closes an fd handed out by {@link #openPlain}. */
  void closeFd(int fd) {
    openFds.decrementAndGet();
    libcClose(fd);
  }

  /** Descriptors opened and not yet closed. Visible for testing. */
  int openFdCount() {
    return openFds.get();
  }

  /**
   * Starts a read, returning false if it could not be tracked, in which case the caller must read
   * the range itself. Takes a ring for this thread's batch if it does not already have one; when
   * every ring is on loan this returns false rather than waiting, because a prefetch is advisory
   * and a caller is better served reading the range directly than blocking for a ring.
   */
  boolean startRead(int fd, long offset, int length, boolean dontCache) {
    if (closed || length <= 0 || length > maxRead) {
      return false;
    }
    dontCache &= isUncached(fd);
    long tid = Thread.currentThread().threadId();
    Reads r = onLoan.get(tid);
    if (r == null) {
      r = idle.poll();
      if (r == null) {
        if (created.incrementAndGet() > maxRings) {
          created.decrementAndGet();
          return false;
        }
        try {
          r = new Reads();
        } catch (Throwable _) {
          created.decrementAndGet();
          return false;
        }
      }
      onLoan.put(tid, r);
    }
    return r.start(fd, offset, length, dontCache);
  }

  /**
   * Copies a read started earlier into {@code dst}, returning the bytes transferred or -1 when this
   * range was never started. Hands the ring back once the batch it belongs to is fully consumed.
   */
  int finishRead(int fd, long offset, int length, byte[] dst, int dstOffset) throws IOException {
    long tid = Thread.currentThread().threadId();
    Reads r = onLoan.get(tid);
    if (r == null) {
      return -1;
    }
    try {
      return r.finish(fd, offset, length, dst, dstOffset);
    } finally {
      if (r.hasNothingOutstanding()) {
        onLoan.remove(tid, r);
        if (closed || idle.offer(r) == false) {
          created.decrementAndGet();
          r.close();
        }
      }
    }
  }

  /**
   * One io_uring instance plus the reads started on it but not yet consumed. Driven by a single
   * thread, so none of this needs synchronization.
   */
  final class Reads implements Closeable {
    private final Arena arena = Arena.ofShared();
    private final MemorySegment handle = arena.allocate(512);

    /**
     * Slot buffers, sized to the largest read served so far rather than to the largest allowed: a
     * file's vectors are all one size, so this settles at the vector size and the allowance above
     * it costs nothing. Grown from {@code arena}, which frees on close only, so a buffer the kernel
     * is still writing into stays valid.
     */
    private MemorySegment buf = MemorySegment.NULL;

    private int slotBytes;
    private final MemorySegment cqePtrs = arena.allocate((long) qd * ADDRESS.byteSize());
    private final int[] slotFd = new int[qd];
    private final long[] slotOffset = new long[qd];
    private final int[] slotLength = new int[qd];
    private final int[] slotResult = new int[qd];
    private final byte[] slotState = new byte[qd];

    /** Reads started but not yet harvested; a single wait can cover all of them. */
    private int outstanding;

    /** Reads queued on the ring but not yet handed to the kernel. */
    private int unsubmitted;

    private int cursor;
    private int inUse;
    private boolean freed;

    private Reads() {
      // The first ring starts the worker pool and every later ring attaches to it, so a batch with
      // many candidates can draw on all the workers instead of only its own ring's share. Attaching
      // needs the primary's descriptor, so a failure there falls back to a private pool rather than
      // failing the read path.
      int primary = primaryRingFd.get();
      int init = primary > 0 ? queueInitAttached(qd, handle, primary) : queueInit(qd, handle, 0);
      if (init < 0 && primary > 0) {
        init = queueInit(qd, handle, 0);
        if (init >= 0) {
          attachFailures.incrementAndGet();
        }
      }
      if (init < 0) {
        arena.close();
        throw new IllegalStateException("io_uring_queue_init=" + init);
      }
      primaryRingFd.compareAndSet(-1, ringFd(handle));
    }

    /**
     * Starts a read, returning false if it could not be tracked, in which case the caller must read
     * the range itself. {@code dontCache} asks the kernel not to retain the pages.
     */
    boolean start(int fd, long offset, int length, boolean dontCache) {
      if (freed || length <= 0 || length > maxRead) {
        return false;
      }
      if (length > slotBytes && grow(length) == false) {
        return false;
      }
      int slot = claimSlot();
      if (slot < 0) {
        return false;
      }
      MemorySegment sqe = getSqe(handle);
      if (sqe == null) {
        // The submission queue is full; make room and try once more.
        if (submitAndWait(handle, 0) >= 0) {
          unsubmitted = 0;
        }
        sqe = getSqe(handle);
        if (sqe == null) {
          free(slot);
          return false;
        }
      }
      prepRead(sqe, fd, buf.asSlice((long) slot * slotBytes, length), length, offset);
      if (dontCache) {
        sqe.set(JAVA_INT, SQE_RW_FLAGS_OFFSET, RWF_DONTCACHE);
      }
      setData64(sqe, slot);
      slotFd[slot] = fd;
      slotOffset[slot] = offset;
      slotLength[slot] = length;
      slotState[slot] = SLOT_PENDING;
      outstanding++;
      if (++unsubmitted >= SUBMIT_BATCH && submit(handle) >= 0) {
        unsubmitted = 0;
      }
      return true;
    }

    /**
     * Copies a previously started read into {@code dst}, returning the number of bytes or -1 when
     * this range was never started.
     */
    int finish(int fd, long offset, int length, byte[] dst, int dstOffset) throws IOException {
      if (freed) {
        return -1;
      }
      int slot = findSlot(fd, offset, length);
      if (slot < 0) {
        return -1;
      }
      while (slotState[slot] == SLOT_PENDING) {
        // Reads are started in a batch and consumed in a batch, so waiting for every read already
        // in
        // flight costs nothing extra and turns one io_uring_enter per read into one per batch.
        int waited = submitAndWait(handle, Math.max(1, outstanding));
        if (waited >= 0) {
          unsubmitted = 0;
        } else if (waited != -11 && waited != -4) { // -EAGAIN and -EINTR can be retried
          free(slot);
          outstanding--;
          throw new IOException("io_uring_submit_and_wait=" + waited);
        }
        harvest();
      }
      int res = slotResult[slot];
      if (res > 0) {
        MemorySegment.copy(buf, JAVA_BYTE, (long) slot * slotBytes, dst, dstOffset, res);
      }
      free(slot);
      if (res < 0) {
        throw new IOException("io_uring read failed, errno=" + (-res));
      }
      return res;
    }

    private void harvest() {
      int ready = peekBatchCqe(handle, cqePtrs, qd);
      for (int i = 0; i < ready; i++) {
        MemorySegment cqe = cqePtrs.getAtIndex(ADDRESS, i).reinterpret(CQE_BYTES);
        int slot = (int) cqeGetData64(cqe);
        if (slot >= 0 && slot < qd && slotState[slot] == SLOT_PENDING) {
          slotResult[slot] = cqe.get(JAVA_INT, CQE_RES_OFFSET);
          slotState[slot] = SLOT_DONE;
          outstanding--;
        }
      }
      if (ready > 0) {
        cqAdvance(handle, ready);
      }
    }

    /**
     * Re-sizes the slot buffers to hold reads of {@code length}. Only possible while no read is in
     * flight, since the offsets of the ones already started point into the current buffer; a read
     * that arrives larger mid-batch is left to the caller instead.
     */
    private boolean grow(int length) {
      if (outstanding > 0 || inUse > 0) {
        return false;
      }
      slotBytes = length;
      buf = arena.allocate((long) qd * slotBytes);
      return true;
    }

    /**
     * A free slot, reusing a completed one that nobody asked for rather than refusing to track the
     * read: a started read whose range is never consumed would otherwise hold its slot forever and
     * silently push every later read onto the caller's fallback path.
     */
    private int claimSlot() {
      int done = -1;
      for (int i = 0; i < qd; i++) {
        int slot = (cursor + i) % qd;
        if (slotState[slot] == SLOT_FREE) {
          cursor = (slot + 1) % qd;
          inUse++;
          return slot;
        }
        if (done < 0 && slotState[slot] == SLOT_DONE) {
          done = slot;
        }
      }
      if (done >= 0) {
        cursor = (done + 1) % qd;
      }
      return done;
    }

    private void free(int slot) {
      slotState[slot] = SLOT_FREE;
      inUse--;
    }

    /** True once every read started on this ring has been consumed, so it can be handed back. */
    private boolean hasNothingOutstanding() {
      return inUse == 0;
    }

    /**
     * Scans forward from the slot after the newest claimed one, so the first match is normally the
     * oldest outstanding read, which is the one a caller asks for next.
     */
    private int findSlot(int fd, long offset, int length) {
      for (int i = 0; i < qd; i++) {
        int slot = (cursor + i) % qd;
        if (slotState[slot] != SLOT_FREE
            && slotFd[slot] == fd
            && slotOffset[slot] == offset
            && slotLength[slot] == length) {
          return slot;
        }
      }
      return -1;
    }

    @Override
    public void close() {
      if (freed) {
        return;
      }
      freed = true;
      try {
        queueExit(handle);
      } finally {
        arena.close();
      }
    }
  }

  /** Frees every ring, whether idle or still on loan. */
  @Override
  public void close() {
    closed = true;
    for (Reads r : onLoan.values()) {
      try {
        r.close();
      } catch (Throwable _) {
      }
    }
    onLoan.clear();
    for (Reads r; (r = idle.poll()) != null; ) {
      try {
        r.close();
      } catch (Throwable _) {
      }
    }
  }
}
