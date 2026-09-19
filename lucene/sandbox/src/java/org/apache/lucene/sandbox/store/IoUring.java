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
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
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
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.ArrayUtil;

/**
 * io_uring O_DIRECT batched vector reader via {@code liburing-ffi} (pure FFM; liburing owns the
 * ring mmap, memory ordering, and arch specifics). A reader borrows a ring from a bounded pool and
 * submits a whole shortlist as one batched submit-and-wait, so the kernel supplies the read
 * concurrency and no read-thread pool is needed. Package-private engine behind {@link
 * IoUringDirectory}.
 */
@SuppressWarnings("restricted") // FFM downcalls to libc + liburing-ffi
final class IoUring implements Closeable {
  private static final int BLK = 4096;
  private static final int O_RDONLY = 0;

  private static final int F_GETFL = 3; // same on every Linux architecture

  /** {@code -EINTR}: a submit interrupted by a signal, which is retried rather than failed. */
  private static final int NEG_EINTR = -4;

  /** {@code struct io_uring_cqe} is 16 bytes wide, with its {@code res} field at offset 8. */
  private static final int CQE_BYTES = 16;

  private static final int CQE_RES_OFFSET = 8;

  /**
   * O_DIRECT open flag, which is one of the few {@code open(2)} flags that is not identical across
   * Linux architectures. A wrong guess here would be silently harmful — {@code open} can succeed
   * while ignoring the flag, leaving us with ordinary buffered reads — so {@link #openDirect}
   * verifies with {@code fcntl(F_GETFL)} that the kernel really honoured it and fails if not.
   */
  private static final int O_DIRECT =
      switch (System.getProperty("os.arch", "")) {
        case "aarch64" -> 0x10000; // arm64 asm/fcntl.h
        case "ppc64", "ppc64le" -> 0x20000; // powerpc
        default -> 0x4000; // x86_64, s390x, and the asm-generic default
      };

  /** libc + liburing-ffi bindings; <clinit> throws (caught by {@link #isAvailable}) if absent. */
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

    /** {@code fcntl} is variadic; F_GETFL passes no variadic argument. */
    static final MethodHandle MH$fcntl =
        L.downcallHandle(
            LIBC.find("fcntl")
                .orElseThrow(() -> new UnsupportedOperationException("libc has no symbol 'fcntl'")),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT),
            Linker.Option.firstVariadicArg(2));

    static final MethodHandle MH$io_uring_queue_init =
        u("io_uring_queue_init", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
    static final MethodHandle MH$io_uring_queue_exit =
        u("io_uring_queue_exit", FunctionDescriptor.ofVoid(ADDRESS));
    static final MethodHandle MH$io_uring_get_sqe =
        u("io_uring_get_sqe", FunctionDescriptor.of(ADDRESS, ADDRESS));
    static final MethodHandle MH$io_uring_prep_read_fixed =
        u(
            "io_uring_prep_read_fixed",
            FunctionDescriptor.ofVoid(ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG, JAVA_INT));
    static final MethodHandle MH$io_uring_sqe_set_data64 =
        u("io_uring_sqe_set_data64", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG));
    static final MethodHandle MH$io_uring_register_buffers =
        u("io_uring_register_buffers", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
    static final MethodHandle MH$io_uring_submit_and_wait =
        u("io_uring_submit_and_wait", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
    static final MethodHandle MH$io_uring_peek_batch_cqe =
        u("io_uring_peek_batch_cqe", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
    static final MethodHandle MH$io_uring_cqe_get_data64 =
        u("io_uring_cqe_get_data64", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    static final MethodHandle MH$io_uring_cq_advance =
        u("io_uring_cq_advance", FunctionDescriptor.ofVoid(ADDRESS, JAVA_INT));
  }

  private static volatile Boolean available;

  /** True iff liburing-ffi loads, symbols resolve, and a probe ring initializes. */
  static boolean isAvailable() {
    Boolean a = available;
    if (a != null) return a;
    synchronized (IoUring.class) {
      if (available != null) return available;
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

  private static int libcClose(int fd) {
    try {
      return (int) Native.MH$close.invokeExact(fd);
    } catch (Throwable t) {
      throw new AssertionError("close", t);
    }
  }

  private static int fcntl(int fd, int cmd) {
    try {
      return (int) Native.MH$fcntl.invokeExact(fd, cmd);
    } catch (Throwable t) {
      throw new AssertionError("fcntl", t);
    }
  }

  private static int queueInit(int entries, MemorySegment ring, int flags) {
    try {
      return (int) Native.MH$io_uring_queue_init.invokeExact(entries, ring, flags);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_init", t);
    }
  }

  private static void queueExit(MemorySegment ring) {
    try {
      Native.MH$io_uring_queue_exit.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_queue_exit", t);
    }
  }

  private static MemorySegment getSqe(MemorySegment ring) {
    try {
      return (MemorySegment) Native.MH$io_uring_get_sqe.invokeExact(ring);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_get_sqe", t);
    }
  }

  private static void prepReadFixed(
      MemorySegment sqe, int fd, MemorySegment buf, int len, long offset, int bufIndex) {
    try {
      Native.MH$io_uring_prep_read_fixed.invokeExact(sqe, fd, buf, len, offset, bufIndex);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_prep_read_fixed", t);
    }
  }

  private static void setData64(MemorySegment sqe, long data) {
    try {
      Native.MH$io_uring_sqe_set_data64.invokeExact(sqe, data);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_sqe_set_data64", t);
    }
  }

  private static int registerBuffers(MemorySegment ring, MemorySegment iovecs, int count) {
    try {
      return (int) Native.MH$io_uring_register_buffers.invokeExact(ring, iovecs, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_register_buffers", t);
    }
  }

  private static int submitAndWait(MemorySegment ring, int waitNr) {
    try {
      return (int) Native.MH$io_uring_submit_and_wait.invokeExact(ring, waitNr);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_submit_and_wait", t);
    }
  }

  private static int peekBatchCqe(MemorySegment ring, MemorySegment cqes, int count) {
    try {
      return (int) Native.MH$io_uring_peek_batch_cqe.invokeExact(ring, cqes, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_peek_batch_cqe", t);
    }
  }

  private static long cqeGetData64(MemorySegment cqe) {
    try {
      return (long) Native.MH$io_uring_cqe_get_data64.invokeExact(cqe);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cqe_get_data64", t);
    }
  }

  private static void cqAdvance(MemorySegment ring, int count) {
    try {
      Native.MH$io_uring_cq_advance.invokeExact(ring, count);
    } catch (Throwable t) {
      throw new AssertionError("io_uring_cq_advance", t);
    }
  }

  /** Slot index stashed in the SQE's user data, identifying which read this completion is for. */
  private static int readIndexOf(MemorySegment cqe) {
    return (int) cqeGetData64(cqe);
  }

  /** Bytes read, or a negative errno. */
  private static int resultOf(MemorySegment cqe) {
    return cqe.get(JAVA_INT, CQE_RES_OFFSET);
  }

  private final int qd;
  private final int maxRings;

  /** Idle rings available to borrow; a ring is only ever driven by one thread at a time. */
  private final ArrayBlockingQueue<Ring> idle;

  private final AtomicInteger created = new AtomicInteger();

  /** O_DIRECT descriptors handed out and not yet closed. Visible for testing. */
  private final AtomicInteger openFds = new AtomicInteger();

  private volatile boolean closed;

  IoUring(int queueDepth, int maxRings) {
    if (!isAvailable()) {
      throw new IllegalStateException("io_uring unavailable");
    }
    if (maxRings < 1) {
      throw new IllegalArgumentException("maxRings must be >= 1, got " + maxRings);
    }
    this.qd = Integer.highestOneBit(Math.max(queueDepth, 2) - 1) << 1;
    this.maxRings = maxRings;
    this.idle = new ArrayBlockingQueue<>(maxRings);
  }

  /**
   * Opens an O_DIRECT read-only fd for {@code path}. The caller owns the returned fd and must
   * {@link #closeFd} it; tying it to the {@link org.apache.lucene.store.IndexInput} that uses it is
   * what lets a merged-away segment release the file instead of pinning it for the directory's
   * lifetime.
   */
  int openDirect(String path) throws IOException {
    try (Arena a = Arena.ofConfined()) {
      int fd = open(a.allocateFrom(path), O_RDONLY | O_DIRECT);
      if (fd < 0) {
        throw new IOException("open with O_DIRECT failed for " + path);
      }
      // open() may succeed while ignoring an O_DIRECT flag it does not recognise, which would leave
      // us doing buffered reads under a name that promises otherwise. Confirm before trusting it.
      int flags = fcntl(fd, F_GETFL);
      if (flags < 0 || (flags & O_DIRECT) == 0) {
        libcClose(fd);
        throw new IOException("O_DIRECT not honoured for " + path);
      }
      openFds.incrementAndGet();
      return fd;
    }
  }

  /** Closes an fd handed out by {@link #openDirect}. */
  void closeFd(int fd) {
    openFds.decrementAndGet();
    libcClose(fd);
  }

  /**
   * Descriptors opened by {@link #openDirect} and not yet closed. Visible for testing, which uses
   * it to check that closing an {@link org.apache.lucene.store.IndexInput} releases its descriptor.
   */
  int openFdCount() {
    return openFds.get();
  }

  /** First block boundary at or before {@code filePos}: the offset O_DIRECT will accept. */
  private static long blockStart(long filePos) {
    return filePos & -(long) BLK;
  }

  /**
   * How far {@code filePos} sits past its block boundary, i.e. where the vector starts in a span.
   */
  private static int offsetInBlock(long filePos) {
    return (int) (filePos & (BLK - 1));
  }

  /** Block-aligned byte count that must be read to cover {@code len} bytes at {@code pos}. */
  private static int span(long pos, int len) {
    int skew = (int) (pos & (BLK - 1));
    return (skew + len + BLK - 1) & -BLK;
  }

  private final class Ring {
    final Arena arena = Arena.ofShared();

    /** The native {@code struct io_uring} this object drives. */
    final MemorySegment handle = arena.allocate(512);

    /** Buffer registered with the kernel, carved into one span per in-flight read. */
    final MemorySegment buf = arena.allocate((long) qd * BLK, BLK);

    /** Scratch array the kernel fills with pointers to ready completions. */
    final MemorySegment cqePtrs = arena.allocate((long) qd * ADDRESS.byteSize());

    Ring() {
      int init = queueInit(qd, handle, 0);
      if (init < 0) {
        throw new RuntimeException("io_uring_queue_init=" + init);
      }
      MemorySegment iov = arena.allocate(16);
      iov.set(JAVA_LONG, 0, buf.address());
      iov.set(JAVA_LONG, 8, (long) qd * BLK);
      int registered = registerBuffers(handle, iov, 1);
      if (registered < 0) {
        queueExit(handle);
        throw new RuntimeException("register_buffers=" + registered);
      }
    }

    void free() {
      queueExit(handle);
      arena.close();
    }
  }

  /**
   * Borrows an idle ring, creating one while under {@code maxRings}, otherwise waiting for a peer
   * to finish. Rings are not thread-affine (the registered buffer travels with the ring), so
   * borrowing per call rather than per thread keeps the resource count bounded by read concurrency
   * instead of by how many threads have ever reranked, and leaves nothing behind when a thread
   * dies.
   */
  private Ring acquire() throws IOException {
    Ring r = idle.poll();
    if (r != null) {
      return r;
    }
    if (created.incrementAndGet() <= maxRings) {
      return new Ring();
    }
    created.decrementAndGet();
    try {
      return idle.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted waiting for an io_uring ring", e);
    }
  }

  /**
   * Rings currently allocated, whether idle or on loan. Visible for testing, which uses it to check
   * that a ring is discarded rather than pooled when a read fails.
   */
  int ringCount() {
    return created.get();
  }

  /** Returns a quiesced ring to the pool: every submission it carried has been reaped. */
  private void release(Ring r) {
    if (closed || idle.offer(r) == false) {
      created.decrementAndGet();
      r.free();
    }
  }

  /**
   * Destroys a ring instead of pooling it. Used whenever a read gives up part way through, because
   * submissions that were never reaped would otherwise surface as completions for the <em>next</em>
   * borrower, which would decode them against its own request array and silently return the wrong
   * vectors. Losing one ring on an I/O error is a cheap price for that guarantee.
   */
  private void discard(Ring r) {
    created.decrementAndGet();
    try {
      r.free();
    } catch (Throwable _) {
      // best effort: the ring is being abandoned anyway
    }
  }

  /** A set of reads gathered across files, submitted together by {@link Batch#execute()}. */
  final class Batch {
    private int[] fds = new int[128];
    private long[] positions = new long[128];
    private float[][] outs = new float[128][];
    private int[] offsets = new int[128];
    private int n;
    private int dim = -1;

    /**
     * Queues one input's reads. Flushes first if the dimension changes, since a submission is
     * uniform.
     */
    void add(int fd, long[] pos, int vectorDim, int count, float[] out) throws IOException {
      if (count == 0) {
        return;
      }
      if (dim != -1 && dim != vectorDim) {
        execute();
      }
      dim = vectorDim;
      // ArrayUtil rather than Arrays.copyOf: Lucene's growth helpers over-allocate sanely and are
      // what the codebase mandates. Each array is checked on its own length, because oversize()
      // rounds to a whole number of 8-byte words and so returns a different length for int[] than
      // for long[]: after one grow these four are no longer the same size.
      int needed = n + count;
      if (needed > fds.length) {
        fds = ArrayUtil.grow(fds, needed);
      }
      if (needed > positions.length) {
        positions = ArrayUtil.grow(positions, needed);
      }
      if (needed > outs.length) {
        outs = ArrayUtil.grow(outs, needed);
      }
      if (needed > offsets.length) {
        offsets = ArrayUtil.grow(offsets, needed);
      }
      for (int i = 0; i < count; i++) {
        fds[n] = fd;
        positions[n] = pos[i];
        outs[n] = out;
        offsets[n] = i * vectorDim;
        n++;
      }
    }

    /** Submits everything queued and clears the batch. */
    void execute() throws IOException {
      if (n == 0) {
        return;
      }
      try {
        readEntries(fds, positions, dim, n, outs, offsets);
      } finally {
        Arrays.fill(outs, 0, n, null); // do not pin caller buffers
        n = 0;
        dim = -1;
      }
    }
  }

  Batch newBatch() {
    return new Batch();
  }

  /**
   * Reads {@code count} vectors of {@code dim} floats, where read {@code i} comes from {@code
   * fds[i]} at {@code positions[i]} and lands in {@code outs[i]} at {@code offsets[i]}. Because a
   * submission entry carries its own descriptor, reads spanning many files go out together — which
   * is how a rerank shortlist spread across every segment becomes one submission instead of one per
   * segment.
   */
  void readEntries(int[] fds, long[] positions, int dim, int count, float[][] outs, int[] offsets)
      throws IOException {
    if (closed) {
      throw new IOException("closed");
    }
    if (count == 0) {
      return;
    }
    final int vectorBytes = dim * Float.BYTES;
    final int blocksPerSlot = blocksPerSlot(vectorBytes);
    final int slotsPerWave = Math.max(1, qd / blocksPerSlot);
    Ring ring = acquire();
    boolean quiesced = false;
    try {
      // One wave per load of the registered buffer: fill every slot, then drain them all before
      // those slots are reused.
      for (int waveStart = 0; waveStart < count; waveStart += slotsPerWave) {
        int waveSize = Math.min(slotsPerWave, count - waveStart);
        prepareWave(ring, waveStart, waveSize, fds, positions, vectorBytes, blocksPerSlot);
        reapWave(
            ring, waveStart, waveSize, positions, outs, offsets, dim, vectorBytes, blocksPerSlot);
      }
      quiesced = true; // every submission has been reaped, so the ring is reusable
    } finally {
      if (quiesced) {
        release(ring);
      } else {
        discard(ring);
      }
    }
  }

  /**
   * Blocks of registered buffer that one read occupies. O_DIRECT requires a block-aligned file
   * offset, length and buffer address, so a vector is fetched as the aligned span containing it and
   * copied out from its offset within that span: the vector rounded up to whole blocks, plus one
   * more because an unaligned start pushes it into the following block. Page-aligning the vector
   * data (see {@code Lucene99FlatVectorsWriter}) is what removes that extra block.
   */
  private static int blocksPerSlot(int vectorBytes) {
    return (vectorBytes + BLK - 1) / BLK + 1;
  }

  /**
   * Queues {@code waveSize} reads, starting at read {@code waveStart}, onto the submission queue.
   */
  private void prepareWave(
      Ring ring,
      int waveStart,
      int waveSize,
      int[] fds,
      long[] positions,
      int vectorBytes,
      int blocksPerSlot)
      throws IOException {
    for (int slot = 0; slot < waveSize; slot++) {
      int readIndex = waveStart + slot;
      MemorySegment sqe = getSqe(ring.handle);
      if (MemorySegment.NULL.equals(sqe)) {
        throw new IOException("io_uring SQ ring exhausted");
      }
      long filePos = positions[readIndex];
      int spanBytes = span(filePos, vectorBytes);
      MemorySegment slotBuffer = ring.buf.asSlice((long) slot * blocksPerSlot * BLK, spanBytes);
      prepReadFixed(sqe, fds[readIndex], slotBuffer, spanBytes, blockStart(filePos), 0);
      setData64(sqe, readIndex); // so a completion identifies which read it belongs to
    }
  }

  /**
   * Submits the queued wave and copies each vector out as its completion arrives, returning once
   * all {@code waveSize} reads have been reaped. Loops because a submit can be interrupted by a
   * signal, can be short, and can yield fewer completions than were asked for.
   */
  private void reapWave(
      Ring ring,
      int waveStart,
      int waveSize,
      long[] positions,
      float[][] outs,
      int[] offsets,
      int dim,
      int vectorBytes,
      int blocksPerSlot)
      throws IOException {
    int reaped = 0;
    while (reaped < waveSize) {
      int submitted = submitAndWait(ring.handle, waveSize - reaped);
      if (submitted < 0 && submitted != NEG_EINTR) {
        throw new IOException("io_uring_submit_and_wait errno=" + (-submitted));
      }
      int ready = peekBatchCqe(ring.handle, ring.cqePtrs, qd);
      try {
        for (int i = 0; i < ready; i++) {
          MemorySegment cqe = ring.cqePtrs.getAtIndex(ADDRESS, i).reinterpret(CQE_BYTES);
          copyOut(
              ring,
              readIndexOf(cqe),
              resultOf(cqe),
              waveStart,
              positions,
              outs,
              offsets,
              dim,
              vectorBytes,
              blocksPerSlot);
        }
      } finally {
        if (ready > 0) {
          cqAdvance(ring.handle, ready);
        }
      }
      reaped += ready;
    }
  }

  /** Copies one completed read out of the registered buffer into the caller's destination array. */
  private static void copyOut(
      Ring ring,
      int readIndex,
      int result,
      int waveStart,
      long[] positions,
      float[][] outs,
      int[] offsets,
      int dim,
      int vectorBytes,
      int blocksPerSlot)
      throws IOException {
    long filePos = positions[readIndex];
    if (result < 0) {
      throw new IOException("io_uring read errno=" + (-result) + " at " + filePos);
    }
    int vectorOffsetInSpan = offsetInBlock(filePos);
    if (result < vectorOffsetInSpan + vectorBytes) {
      throw new IOException("io_uring short read at " + filePos + " res=" + result);
    }
    long slotBufferStart = (long) (readIndex - waveStart) * blocksPerSlot * BLK;
    // JAVA_FLOAT_UNALIGNED: a vector's offset inside its span need not be 4-byte aligned.
    MemorySegment.copy(
        ring.buf,
        JAVA_FLOAT_UNALIGNED,
        slotBufferStart + vectorOffsetInSpan,
        outs[readIndex],
        offsets[readIndex],
        dim);
  }

  /**
   * Frees every idle ring. Rings still on loan are freed by the borrower on {@link #release}, so a
   * read in flight during close finishes against a valid ring instead of a freed one.
   */
  @Override
  public void close() {
    closed = true;
    for (Ring r; (r = idle.poll()) != null; ) {
      created.decrementAndGet();
      try {
        r.free();
      } catch (Throwable _) {
      }
    }
  }
}
