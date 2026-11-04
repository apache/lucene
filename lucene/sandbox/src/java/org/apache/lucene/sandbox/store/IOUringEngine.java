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

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Ring pool behind {@link IOUringDirectory}: leases a ring to a thread, turns a {@code
 * prefetchRange} call into a queued read, and hands back an {@link UringRange} the caller reads
 * when it wants the bytes.
 *
 * <ul>
 *   <li>Reads are queued in userspace and submitted together. The kernel is entered from {@link
 *       #prefetchRange} when the slot table is full, from {@link #ensureReady} when a caller wants
 *       bytes that have not arrived, and from the reclaim pass in {@link #obtain}.
 *   <li>A slot holds a buffer only while the kernel is writing to it. Reaping frees the slot, after
 *       which the handle's own reference keeps the buffer alive. So {@code qd} bounds concurrent
 *       in-flight reads, not outstanding handles.
 *   <li>Failures throw. A failed prefetch is never covered by reading the bytes another way.
 * </ul>
 *
 * <p>Not thread safe per lease; a lease belongs to one thread until it is released.
 */
@SuppressWarnings("restricted") // FFM downcalls via LibUring
final class IOUringEngine implements Closeable {

  private final int qd;
  private final int maxRings;
  private final int readFlags;

  /**
   * Idle rings, clean or dirty. {@link #obtain} cleans a dirty ring as it passes and pushes it to
   * the tail if it is not clean yet.
   */
  private final ArrayBlockingQueue<Lease> pool;

  /** Tears down the rings of leases that become unreachable without being freed. */
  private static final Cleaner CLEANER = Cleaner.create();

  private final AtomicInteger created = new AtomicInteger();
  private final AtomicInteger openFds = new AtomicInteger();

  /** Ranges handed out over this engine's lifetime. Exposed for benchmarks. */
  private final AtomicLong rangesServed = new AtomicLong();

  // Kernel-entry accounting, for checking that reads are batched.
  private final LongAdder syscalls = new LongAdder();
  private final LongAdder waitingSyscalls = new LongAdder();
  private final LongAdder sqesSubmitted = new LongAdder();
  private final LongAdder cqesReaped = new LongAdder();
  private final LongAdder depthSum = new LongAdder();

  /** Why we entered the kernel: a prefetch found every slot busy. */
  private static final int CALLER_SLOT_FULL = 0;

  /** Why we entered the kernel: a read needed bytes that had not arrived. */
  private static final int CALLER_READ_WAIT = 1;

  /** Why we entered the kernel: the submission queue was full while building entries. */
  private static final int CALLER_SQ_FULL = 2;

  private final LongAdder[] byCaller = newAdders(3);

  /** Syscalls bucketed by reads submitted: 0, 1, 2-3, 4-7, 8-15, 16-31, 32-63, 64-127, 128+. */
  private final LongAdder[] sqesPerSyscall = newAdders(9);

  /** Syscalls bucketed by completions reaped straight after, same buckets as above. */
  private final LongAdder[] cqesPerSyscall = newAdders(9);

  private static LongAdder[] newAdders(int n) {
    LongAdder[] a = new LongAdder[n];
    for (int i = 0; i < n; i++) {
      a[i] = new LongAdder();
    }
    return a;
  }

  /**
   * The calling thread's lease, if it holds one. Held per thread rather than per input, because
   * prefetch and read happen through different clones and a batch can span several files.
   */
  private final ThreadLocal<Lease> current = new ThreadLocal<>();

  private volatile boolean closed;

  IOUringEngine(int queueDepth, int maxRings, int readFlags) {
    if (maxRings < 1) {
      throw new IllegalArgumentException("maxRings must be >= 1, got " + maxRings);
    }
    // Round up to a power of two, as io_uring_queue_init does internally.
    this.qd = Integer.highestOneBit(Math.max(queueDepth, 2) - 1) << 1;
    this.maxRings = maxRings;
    this.readFlags = readFlags;
    this.pool = new ArrayBlockingQueue<>(maxRings);
  }

  int queueDepth() {
    return qd;
  }

  /** Descriptors handed out and not yet closed. Visible for testing. */
  int openFdCount() {
    return openFds.get();
  }

  /** Rings allocated, idle or on loan. Visible for testing. */
  int ringCount() {
    return created.get();
  }

  /** Ranges handed out over this engine's lifetime. See {@link #rangesServed}. */
  long rangesServedCount() {
    return rangesServed.get();
  }

  /** A snapshot of the counters. The fields are read one after another. */
  IOUringDirectory.Stats stats() {
    long[] hist = new long[sqesPerSyscall.length];
    long[] reapedHist = new long[cqesPerSyscall.length];
    for (int i = 0; i < hist.length; i++) {
      hist[i] = sqesPerSyscall[i].sum();
      reapedHist[i] = cqesPerSyscall[i].sum();
    }
    return new IOUringDirectory.Stats(
        rangesServed.get(),
        syscalls.sum(),
        waitingSyscalls.sum(),
        sqesSubmitted.sum(),
        cqesReaped.sum(),
        depthSum.sum(),
        byCaller[CALLER_SLOT_FULL].sum(),
        byCaller[CALLER_READ_WAIT].sum(),
        byCaller[CALLER_SQ_FULL].sum(),
        hist,
        reapedHist);
  }

  /**
   * Counts one {@code io_uring_enter}. liburing only enters the kernel when there is something to
   * submit or wait for, so the caller must not record a call that did neither.
   *
   * @param submitted entries the kernel took in this call, possibly zero for a pure wait
   * @param depth reads in flight once this call has submitted
   */
  private void recordEnter(int caller, int submitted, boolean waited, int depth) {
    syscalls.increment();
    if (waited) {
      waitingSyscalls.increment();
    }
    sqesSubmitted.add(submitted);
    depthSum.add(depth);
    byCaller[caller].increment();
    sqesPerSyscall[bucketOf(submitted)].increment();
  }

  /** Maps a count to its histogram bucket: 0, 1, 2-3, 4-7, 8-15, 16-31, 32-63, 64-127, 128+. */
  private static int bucketOf(int n) {
    return n == 0 ? 0 : Math.min(8, 1 + (31 - Integer.numberOfLeadingZeros(n)));
  }

  /** A plain read-only descriptor. The caller owns it and must {@link #closeFd} it. */
  int openFile(String path) throws IOException {
    try (Arena a = Arena.ofConfined()) {
      int fd = LibUring.open(a.allocateFrom(path), LibUring.O_RDONLY);
      if (fd < 0) {
        throw new IOException("open failed for " + path);
      }
      openFds.incrementAndGet();
      return fd;
    }
  }

  void closeFd(int fd) {
    // Safe with reads in flight: the kernel holds its own reference to the file from submission.
    LibUring.close(fd);
    openFds.decrementAndGet();
  }

  /**
   * Queues a read of {@code [offset, offset+length)} from {@code fd} and returns the handle it will
   * land in. No SQE is built and the kernel is not entered, unless the slot table is full, in which
   * case this cranks the ring until a slot frees.
   */
  UringRange prefetchRange(String description, int fd, long offset, int length) throws IOException {
    if (closed) {
      throw new IOException("directory is closed");
    }
    Lease l = lease();
    int slot = freeSlot(l);
    while (slot < 0) {
      if (l.unsubmitted + l.resources.inFlight == 0) {
        throw new IllegalStateException("no free slot but nothing outstanding");
      }
      crank(l, 1, CALLER_SLOT_FULL);
      slot = freeSlot(l);
    }
    final MemorySegment buf;
    try {
      buf = l.resources.buffers.allocate(length);
    } catch (OutOfMemoryError e) {
      throw new IOException("out of memory reserving " + length + " bytes for a prefetch", e);
    }
    UringRange range = new UringRange(description, this, l, slot, fd, offset, length, buf);
    rangesServed.incrementAndGet();
    l.slots[slot] = range;
    l.unsubmitted++;
    l.liveHandles++;
    return range;
  }

  /**
   * Blocks until {@code range}'s bytes have arrived, submitting it first if it has not gone out.
   */
  void ensureReady(Lease l, UringRange range) throws IOException {
    if (current.get() != l) {
      throw new IllegalStateException("handle read on a thread that does not hold its lease");
    }
    while (range.result == UringRange.PENDING) {
      crank(l, 1, CALLER_READ_WAIT);
    }
  }

  /**
   * Builds SQEs for everything recorded but not submitted, enters the kernel waiting for {@code
   * minComplete} completions, and reaps whatever landed.
   */
  private void crank(Lease l, int minComplete, int caller) throws IOException {
    submitPending(l);
    int wait = l.resources.inFlight == 0 ? 0 : minComplete;
    int rc = LibUring.submitAndWait(l.ring, wait);
    if (rc < 0 && rc != LibUring.NEG_EINTR) {
      throw new IOException("io_uring_submit_and_wait errno=" + (-rc));
    }
    int submitted = Math.max(rc, 0);
    boolean entered = wait > 0 || submitted > 0;
    if (entered) {
      recordEnter(caller, submitted, wait > 0, l.resources.inFlight);
    }
    int reaped = reap(l);
    if (entered) {
      cqesPerSyscall[bucketOf(reaped)].increment();
    }
  }

  /**
   * Turns recorded requests into submission entries. This is deferred until submit time because
   * {@code io_uring_get_sqe} commits the entry to the next submit on that ring, which could be made
   * by the next lease of a pooled ring. Recorded requests can instead be dropped for free.
   */
  private void submitPending(Lease l) throws IOException {
    if (l.unsubmitted == 0) {
      return;
    }
    for (int i = 0; i < l.slots.length && l.unsubmitted > 0; i++) {
      UringRange r = l.slots[i];
      if (r == null || r.submitted) {
        continue;
      }
      MemorySegment sqe = LibUring.getSqe(l.ring);
      if (MemorySegment.NULL.equals(sqe)) {
        // Submission queue full: submitting frees every SQE without waiting for a completion.
        int rc = LibUring.submit(l.ring);
        if (rc < 0) {
          throw new IOException("io_uring_submit errno=" + (-rc));
        }
        if (rc > 0) {
          recordEnter(CALLER_SQ_FULL, rc, false, l.resources.inFlight);
        }
        sqe = LibUring.getSqe(l.ring);
        if (MemorySegment.NULL.equals(sqe)) {
          throw new IOException("no submission queue entry available after submit");
        }
      }
      LibUring.prepRead(sqe, r.fd, r.buffer, r.length, r.offset);
      LibUring.setRwFlags(sqe, readFlags);
      LibUring.setData64(sqe, userData(l.generation, i));
      r.submitted = true;
      l.unsubmitted--;
      l.resources.inFlight++;
    }
  }

  /**
   * Consumes the completions that have landed, marking each handle and freeing its slot. Does not
   * enter the kernel.
   */
  private int reap(Lease l) {
    int n = LibUring.peekBatchCqe(l.ring, l.cqePtrs, qd);
    if (n == 0) {
      return 0;
    }
    cqesReaped.add(n);
    for (int i = 0; i < n; i++) {
      MemorySegment cqe = LibUring.cqeAt(l.cqePtrs, i);
      long data = LibUring.cqeData(cqe);
      int slot = slotOf(data);
      if (generationOf(data) != l.generation) {
        throw new IllegalStateException("completion for a stale lease generation");
      }
      UringRange r = l.slots[slot];
      if (r == null || r.submitted == false) {
        throw new IllegalStateException("completion for an unoccupied slot " + slot);
      }
      r.result = LibUring.cqeResult(cqe);
      l.slots[slot] = null;
      l.resources.inFlight--;
    }
    LibUring.cqAdvance(l.ring, n);
    return n;
  }

  /** Called by a handle as it closes. */
  void handleClosed(Lease l, UringRange range, int slot) {
    l.liveHandles--;
    if (l.slots[slot] == range && range.submitted == false) {
      l.slots[slot] = null;
      l.unsubmitted--;
    }
    if (l.liveHandles == 0) {
      release(l);
    }
  }

  private Lease lease() throws IOException {
    Lease l = current.get();
    if (l == null) {
      l = obtain();
      current.set(l);
    }
    return l;
  }

  /**
   * A leasable ring: one from the pool that is clean or can be cleaned without blocking, otherwise
   * a new one.
   */
  private Lease obtain() throws IOException {
    for (int i = pool.size(); i > 0; i--) {
      Lease l = pool.poll();
      if (l == null) {
        break;
      }
      if (l.resources.inFlight != 0) {
        reap(l);
      }
      if (l.resources.inFlight == 0) {
        l.makeClean();
        return l;
      }
      pool.offer(l); // still dirty
    }
    if (created.incrementAndGet() <= maxRings) {
      try {
        return new Lease(qd, created);
      } catch (RuntimeException | IOException e) {
        created.decrementAndGet();
        throw e;
      }
    }
    created.decrementAndGet();
    throw new IOException(
        "no io_uring ring available: all "
            + maxRings
            + " are leased or have reads outstanding from an abandoned query");
  }

  /** Returns the thread's lease to the pool, clean if nothing is in flight. */
  private void release(Lease l) {
    current.remove();
    // Unsubmitted requests belong to handles that are all closed.
    if (l.unsubmitted != 0) {
      for (int i = 0; i < l.slots.length; i++) {
        UringRange r = l.slots[i];
        if (r != null && r.submitted == false) {
          l.slots[i] = null;
        }
      }
      l.unsubmitted = 0;
    }
    if (l.resources.inFlight == 0) {
      l.makeClean();
    }
    if (closed || pool.offer(l) == false) {
      l.free();
    }
  }

  private static int freeSlot(Lease l) {
    for (int i = 0; i < l.slots.length; i++) {
      if (l.slots[i] == null) {
        return i;
      }
    }
    return -1;
  }

  private static long userData(int generation, int slot) {
    return ((long) generation << 32) | (slot & 0xFFFFFFFFL);
  }

  private static int slotOf(long userData) {
    return (int) userData;
  }

  private static int generationOf(long userData) {
    return (int) (userData >>> 32);
  }

  /** Frees every idle ring. A ring on loan is freed by its borrower on release. */
  @Override
  public void close() {
    closed = true;
    for (Lease l; (l = pool.poll()) != null; ) {
      l.free();
    }
  }

  /** A ring, its slot table and the memory its reads land in, held by one thread. */
  static final class Lease {

    /**
     * What must be released when this lease goes away. Kept apart from the lease so that the {@link
     * Cleaner} action does not reference the lease it is waiting on.
     */
    private static final class Resources implements Runnable {
      /** Holds the {@code struct io_uring} and the completion pointer array. */
      final Arena ringArena = Arena.ofShared();

      MemorySegment ring;
      MemorySegment cqePtrs;
      int qd;

      /** Submitted minus reaped. Owned here so teardown can drain without the lease. */
      int inFlight;

      /**
       * Buffers for the reads of one batch, replaced in {@link #makeClean}. An automatic arena is
       * used because closing a confined one while the kernel is still writing would unmap memory
       * with no Java-side check to catch it. Referenced from here so that the buffers stay
       * reachable until the ring has been torn down.
       */
      Arena buffers = Arena.ofAuto();

      final AtomicInteger created;

      Resources(AtomicInteger created) {
        this.created = created;
      }

      @Override
      public void run() {
        drain();
        LibUring.queueExit(ring);
        ringArena.close();
        created.decrementAndGet();
      }

      /**
       * Waits for every outstanding read to complete. Closing the ring does not: the kernel can
       * still write into a buffer after {@code io_uring_queue_exit} returns, and a plain read pins
       * nothing, so the buffers must not be released until the completions are in.
       */
      private void drain() {
        while (inFlight > 0) {
          int rc = LibUring.submitAndWait(ring, 1);
          if (rc < 0 && rc != LibUring.NEG_EINTR) {
            return;
          }
          int n = LibUring.peekBatchCqe(ring, cqePtrs, qd);
          if (n > 0) {
            LibUring.cqAdvance(ring, n);
            inFlight -= n;
          }
        }
      }
    }

    final Resources resources;
    private final Cleaner.Cleanable cleanable;

    final MemorySegment ring;
    final MemorySegment cqePtrs;

    /** {@code null} means free. Non-null means awaiting submission or in flight. */
    final UringRange[] slots;

    /** Recorded by {@code prefetchRange} but with no SQE built yet. */
    int unsubmitted;

    /** Handed out and not yet closed. The lease is released when this reaches zero. */
    int liveHandles;

    /** Bumped whenever the ring is cleaned. Used to detect a stale completion. */
    int generation;

    /** Counts against {@code created} once constructed, until the ring is freed. */
    Lease(int qd, AtomicInteger created) throws IOException {
      Resources res = new Resources(created);
      boolean success = false;
      try {
        ring = res.ringArena.allocate(LibUring.RING_BYTES, 8);
        res.ring = ring;
        cqePtrs = res.ringArena.allocate((long) qd * ADDRESS.byteSize(), 8);
        res.cqePtrs = cqePtrs;
        res.qd = qd;
        slots = new UringRange[qd];
        // Neither SINGLE_ISSUER nor DEFER_TASKRUN, which pin the ring to one task and so do not
        // allow returning it to a shared pool.
        int rc = LibUring.queueInit(qd, ring, 0);
        if (rc < 0) {
          throw new IOException("io_uring_queue_init errno=" + (-rc));
        }
        success = true;
      } finally {
        if (success == false) {
          res.ringArena.close();
        }
      }
      resources = res;
      cleanable = CLEANER.register(this, res);
    }

    /** Drops the batch's memory and makes the ring leasable again. */
    void makeClean() {
      if (resources.inFlight != 0) {
        throw new IllegalStateException(
            "cleaning a ring with " + resources.inFlight + " reads in flight");
      }
      java.util.Arrays.fill(slots, null);
      unsubmitted = 0;
      resources.buffers = Arena.ofAuto();
      generation++;
    }

    /** Tears down the ring now, rather than when the lease is garbage collected. */
    void free() {
      cleanable.clean();
    }
  }
}
