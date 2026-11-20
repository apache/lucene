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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Batched reads of a file through Linux io_uring, by raw syscalls, used to read a rerank's
 * scattered fine codes off disk (see {@link FineCodeIO}).
 *
 * <p>A rerank reads a few hundred small records at random offsets. Read through a mapping, each
 * page-cache miss is a synchronous fault, so misses run one after another. One io_uring submission
 * issues every read at once, so misses overlap. An instance owns only its file descriptor. Rings
 * are shared by every instance: a batch takes an idle ring and gives it back, at most {@link
 * #MAX_IDLE} stay idle, and they are freed when the last instance closes. Reads land in a buffer
 * the caller owns ({@link #buffer}).
 *
 * <p>A batch is {@link #submit submitted} without waiting and {@link #await awaited} later,
 * possibly on another thread, so a search can issue reads and keep working while they complete;
 * {@link #readBatch} does both at once.
 *
 * <p>Opened {@code direct}, reads bypass the page cache (O_DIRECT): a fine code is read once per
 * query and rarely again, so caching it only evicts the doc values and postings that every query
 * rereads. That needs page-aligned offsets, lengths and buffers.
 *
 * <p>This class only reads; it knows nothing of the index format. On other platforms, or when
 * io_uring cannot be set up, {@link #open} fails and callers read through the mapping instead.
 *
 * <h2>How io_uring works, as used here</h2>
 *
 * <p>A ring is memory shared between this process and the kernel, mapped from the ring's file
 * descriptor, holding three things:
 *
 * <pre>
 *   SQE array   requests: "read len bytes of fd at off into addr, tagged user_data" (64 bytes each)
 *   SQ ring     indices of SQEs ready for the kernel   we move the tail, the kernel the head
 *   CQ ring     one completion per request (CQE):     the kernel moves the tail, we the head
 *               its user_data tag and res, the bytes read or -errno
 * </pre>
 *
 * <p>A batch writes one SQE per read, publishes them by moving the SQ tail, and makes one {@code
 * io_uring_enter} syscall to submit them all; the device then works on every read at once. It waits
 * with {@code io_uring_enter} again, asking for the remaining count, and walks the CQEs between the
 * CQ head and tail, moving the head past those it consumed. Head and tail are 32-bit counters that
 * only grow, wrapping around; {@code counter & mask} is the slot.
 *
 * <p>There is no liburing: setup, mapping and both syscalls are made directly through the foreign
 * function API, so the struct layouts below are the kernel's ABI ({@code
 * include/uapi/linux/io_uring.h}) written out as byte offsets. Two rules carry the safety of the
 * class:
 *
 * <ul>
 *   <li><b>Ordering.</b> The kernel reads SQEs once it sees the tail cover them, and writes a CQE
 *       before moving the tail past it; on a weakly ordered CPU such as aarch64, fences make each
 *       side's writes visible before the counter that publishes them ({@link Ring#submit}, {@link
 *       Ring#reap}).
 *   <li><b>Buffer lifetime.</b> Once submitted, a read may land in its buffer at any moment until
 *       its CQE is reaped. A buffer is reused or freed only after every read of its batch is
 *       reaped; when that cannot be known, after a failure, the buffer is kept reachable for good
 *       ({@link Ring#retire}).
 * </ul>
 */
@SuppressWarnings("restricted")
final class IoUring implements Closeable {
  /** The most reads one batch holds; a rerank's batch rarely needs more. */
  static final int RING_ENTRIES = 256;

  // Syscall numbers, which are the same on x86-64 and aarch64: syscalls added since Linux 5.1 get
  // one number on every architecture. Package-private for tests.
  static final long SYS_IO_URING_SETUP = 425;
  static final long SYS_IO_URING_ENTER = 426;

  // mmap offsets of the submission ring, completion ring and submission entries: not file offsets
  // but tags that tell mmap on a ring descriptor which of its regions to map
  // (IORING_OFF_SQ_RING, IORING_OFF_CQ_RING, IORING_OFF_SQES).
  private static final long OFF_SQ_RING = 0L;
  private static final long OFF_CQ_RING = 0x8000000L;
  private static final long OFF_SQES = 0x10000000L;

  /** io_uring_enter flag: wait until min_complete completions are available. */
  private static final int IORING_ENTER_GETEVENTS = 1;

  /** The opcode of a plain read at an offset, like pread (kernel 5.6+). */
  private static final int IORING_OP_READ = 22;

  /** Feature bit: the SQ and CQ rings share one mapping (kernel 5.4+). */
  private static final int IORING_FEAT_SINGLE_MMAP = 1;

  private static final int PROT_READ_WRITE = 1 | 2;
  private static final int MAP_SHARED = 1;
  private static final int O_RDONLY = 0;
  // O_DIRECT's value differs by architecture; a wrong one fails the open, which then falls back.
  private static final int O_DIRECT =
      System.getProperty("os.arch", "").contains("aarch64") ? 0x10000 : 0x4000;
  private static final int POSIX_FADV_RANDOM = 1;

  /** O_DIRECT's alignment unit for offsets, lengths and buffers. */
  static final int PAGE_BYTES = 4096;

  private static final int EINTR = 4;

  // struct io_uring_params, with its embedded io_sqring_offsets and io_cqring_offsets. setup
  // fills it in: entry counts, features, and where each ring's head, tail, mask and arrays sit
  // within its mapping (offsets from the mapping's start, so they are read, not assumed).
  private static final int PARAMS_SIZE = 120;
  private static final int PARAMS_SQ_ENTRIES = 0;
  private static final int PARAMS_CQ_ENTRIES = 4;
  private static final int PARAMS_FEATURES = 20;
  private static final int SQ_OFF_TAIL = 44;
  private static final int SQ_OFF_RING_MASK = 48;
  private static final int SQ_OFF_ARRAY = 64;
  private static final int CQ_OFF_HEAD = 80;
  private static final int CQ_OFF_TAIL = 84;
  private static final int CQ_OFF_RING_MASK = 88;
  private static final int CQ_OFF_CQES = 100;

  // struct io_uring_sqe. Only these fields are written; the rest stay zero (flags, ioprio,
  // rw_flags, buf_index...), which a plain read wants. The opcode is the first byte.
  private static final int SQE_SIZE = 64;
  private static final int SQE_FD = 4;
  private static final int SQE_OFF = 8;
  private static final int SQE_ADDR = 16;
  private static final int SQE_LEN = 24;
  private static final int SQE_USER_DATA = 32;

  // struct io_uring_cqe: the request's user_data, then res (bytes read, or -errno), then flags.
  private static final int CQE_SIZE = 16;
  private static final int CQE_USER_DATA = 0;
  private static final int CQE_RES = 8;

  private static final boolean LINUX =
      System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("linux");

  // Core: store.PosixNativeAccess does the libc lookup, but it's package-private.
  /** libc bindings, in their own class so that other platforms never resolve them. */
  private static final class Native {
    static final MethodHandle SYSCALL;
    static final MethodHandle MMAP;
    static final MethodHandle MUNMAP;
    static final MethodHandle OPEN;
    static final MethodHandle CLOSE;
    static final MethodHandle FADVISE;

    /**
     * The thread state that {@link #SYSCALL} saves right after the call, before the JVM can run
     * anything that overwrites errno.
     */
    static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();

    static final long ERRNO_OFFSET =
        CALL_STATE.byteOffset(MemoryLayout.PathElement.groupElement("errno"));

    static {
      ValueLayout.OfLong j = ValueLayout.JAVA_LONG;
      ValueLayout.OfInt i = ValueLayout.JAVA_INT;
      AddressLayout p = ValueLayout.ADDRESS;
      // syscall(long, ...) and open(const char *, int, ...) are variadic. libc's syscall returns -1
      // and sets errno on failure, so it captures errno: the handle takes a CALL_STATE segment
      // first.
      SYSCALL =
          libc(
              "syscall",
              FunctionDescriptor.of(j, j, j, j, j, j, j, j),
              Linker.Option.firstVariadicArg(1),
              Linker.Option.captureCallState("errno"));
      MMAP = libc("mmap", FunctionDescriptor.of(p, p, j, i, i, i, j));
      MUNMAP = libc("munmap", FunctionDescriptor.of(i, p, j));
      OPEN = libc("open", FunctionDescriptor.of(i, p, i), Linker.Option.firstVariadicArg(2));
      CLOSE = libc("close", FunctionDescriptor.of(i, i));
      FADVISE = libc("posix_fadvise", FunctionDescriptor.of(i, i, j, j, i));
    }

    /** A downcall to {@code name}. */
    private static MethodHandle libc(String name, FunctionDescriptor fd, Linker.Option... options) {
      Linker linker = Linker.nativeLinker();
      MemorySegment symbol = linker.defaultLookup().find(name).orElseThrow();
      return linker.downcallHandle(symbol, fd, options);
    }
  }

  /**
   * At most this many rings are kept idle between batches; others are freed after use. Opening a
   * reader sets the pool up to this many ({@link #open}), so searches take rings already made
   * instead of setting one up, and a setup that fails does so at open, not mid-search.
   */
  static final int MAX_IDLE = Runtime.getRuntime().availableProcessors();

  private static final Queue<Ring> IDLE = new ConcurrentLinkedQueue<>();

  /** How many rings {@link #IDLE} holds, kept apart because the queue's own count is a walk. */
  private static final AtomicInteger IDLE_COUNT = new AtomicInteger();

  /** Open instances: the last to close frees the idle rings. */
  private static final AtomicInteger OPEN = new AtomicInteger();

  /** Whether io_uring is off for the process, after {@link #MAX_FAILED_BATCHES} failed batches. */
  static boolean disabled() {
    return FAILED_BATCHES.get() >= MAX_FAILED_BATCHES;
  }

  /**
   * Whether reads may still land in {@code buffer}: whether it overlaps a buffer a failed batch
   * retired. A batch that failed otherwise (before submitting, or after reaping every read) leaves
   * no read in flight, and its buffer may be reused.
   */
  static boolean retired(MemorySegment buffer) {
    long start = buffer.address();
    long end = start + buffer.byteSize();
    for (MemorySegment retired : RETIRED) {
      if (retired.address() < end && start < retired.address() + retired.byteSize()) {
        return true;
      }
    }
    return false;
  }

  /** Buffers that reads may still land in after a failure; see {@link Ring#retire}. */
  private static final Queue<MemorySegment> RETIRED = new ConcurrentLinkedQueue<>();

  /**
   * Batches that failed after submitting, each retiring a ring and the buffer its reads may still
   * reach. Counted apart from {@link #RETIRED}, whose own count is a walk.
   */
  private static final AtomicInteger FAILED_BATCHES = new AtomicInteger();

  /**
   * After this many failed batches io_uring is turned off for the process, so a persistent failure
   * cannot keep retiring, and leaking, buffers. Callers then read through the mapping.
   */
  private static final int MAX_FAILED_BATCHES = 64;

  /**
   * How long after a ring fails to set up no new ring is tried; batches meanwhile fail at once and
   * their callers read through the mapping. A setup failure leaks nothing, so it is not counted
   * towards {@link #MAX_FAILED_BATCHES}: it is usually transient, a limit that frees as other
   * searches' rings go idle or are freed. On kernels before 5.12 ring memory counts against
   * RLIMIT_MEMLOCK, whose common 64 KiB default holds about two 256-entry rings, so concurrent
   * searches meet it often; the pause keeps them from paying a failing syscall per batch.
   */
  private static final long SETUP_BACKOFF_NANOS = 1_000_000_000L;

  /** {@link System#nanoTime} before which no new ring is tried. */
  private static volatile long setupRetryAt = System.nanoTime();

  /** Set once a ring setup failure has been logged, which is logged once per process. */
  private static final AtomicBoolean SETUP_FAILURE_LOGGED = new AtomicBoolean();

  /**
   * A read the kernel completed but failed or cut short, or an open the file system refused: the
   * file or kernel refused the read itself. Only this sets {@link #directRefused} or {@link
   * #readsRefused}; a failure to set up or submit to a ring, such as one under memlock limits, is
   * transient and refuses nothing for later opens.
   */
  static final class ReadRefusedException extends IOException {
    ReadRefusedException(String message) {
      super(message);
    }
  }

  /** Set once a file system accepted O_DIRECT but refused the reads: later opens skip it. */
  private static volatile boolean directRefused;

  /**
   * Set once a buffered probe read failed: kernels from 5.1 to 5.5 set up rings but refuse
   * IORING_OP_READ, so every later open fails at once instead of probing again.
   */
  private static volatile boolean readsRefused;

  private final int fd;

  /** The file's length when opened; Lucene files are written once, so it never changes. */
  private final long fileLength;

  /**
   * Guards {@link #fd} against {@link #close}. A submission writes the descriptor's number into its
   * entries and the kernel resolves it at io_uring_enter, so a close in between, and an open that
   * reuses the number, would have the reads served from another file. Submissions hold the read
   * lock until the kernel has taken their entries (from then on a read holds its own reference to
   * the file); close takes the write lock.
   */
  private final ReentrantReadWriteLock fdLock = new ReentrantReadWriteLock();

  /**
   * Set by the first {@link #close}, under {@link #fdLock}; later calls do nothing, and submissions
   * fail. A second close would otherwise close {@link #fd} again, by then possibly another file's
   * descriptor, and miscount {@link #OPEN}.
   */
  private boolean closed;

  /**
   * Whether the file is read uncached (O_DIRECT): every read is then whole pages at a page offset.
   */
  final boolean direct;

  private IoUring(int fd, long fileLength, boolean direct) {
    this.fd = fd;
    this.fileLength = fileLength;
    this.direct = direct;
  }

  /**
   * Opens {@code path} for batched reads, uncached when {@code direct} and the file system allows
   * it, else through the page cache. Fails if a ring cannot be set up or cannot read.
   */
  static IoUring open(Path path, boolean direct) throws IOException {
    if (direct && directRefused == false) {
      try {
        return open(path, O_RDONLY | O_DIRECT);
      } catch (ReadRefusedException _) {
        // The file system refuses O_DIRECT: read through the page cache instead. Any other
        // failure, such as no ring to be had right now, would fail a cached open too.
      }
    }
    return open(path, O_RDONLY);
  }

  private static IoUring open(Path path, int flags) throws IOException {
    if (LINUX == false) {
      throw new IOException("io_uring is unsupported on this platform");
    }
    if (readsRefused) {
      throw new IOException("io_uring reads are unsupported by this kernel");
    }
    int fd;
    try (Arena arena = Arena.ofConfined()) {
      fd = (int) Native.OPEN.invokeExact(arena.allocateFrom(path.toString()), flags);
    } catch (Throwable t) {
      throw new IOException("open failed for " + path, t);
    }
    if (fd < 0) {
      if ((flags & O_DIRECT) != 0) {
        // Some file systems refuse O_DIRECT at open (EINVAL); a file that cannot be opened at all
        // fails the cached open after it too.
        throw new ReadRefusedException("O_DIRECT open failed for " + path);
      }
      throw new IOException("open failed for " + path);
    }
    long fileLength;
    try {
      fileLength = Files.size(path);
    } catch (IOException e) {
      closeFd(fd);
      throw e;
    }
    // Each read is one code: no readahead. A no-op when the page cache is bypassed.
    try {
      int _ = (int) Native.FADVISE.invokeExact(fd, 0L, 0L, POSIX_FADV_RANDOM);
    } catch (Throwable _) {
      // Best effort: reads are correct either way.
    }
    OPEN.incrementAndGet();
    boolean direct = (flags & O_DIRECT) != 0;
    IoUring ioUring = new IoUring(fd, fileLength, direct);
    // Probe with one read: a file system may accept O_DIRECT and still refuse its reads, and an
    // older kernel may set up a ring yet refuse every read. Only such a refusal is remembered for
    // later opens; ring setup failing, say under memlock limits, fails only this open.
    try {
      ioUring.readBatch(new long[] {0}, direct ? PAGE_BYTES : 1, buffer(MemorySegment.NULL, 1));
    } catch (Throwable t) {
      if (t instanceof ReadRefusedException) {
        if (direct) {
          directRefused = true;
        } else {
          readsRefused = true;
        }
      }
      try {
        ioUring.close();
      } catch (IOException suppressed) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
    fillIdle();
    return ioUring;
  }

  /**
   * Sets up idle rings until {@link #MAX_IDLE} are idle; a no-op once they are, as for every open
   * after the first. Stops at the first ring that cannot be set up: batches then set up their own,
   * as without this.
   */
  private static void fillIdle() {
    while (IDLE_COUNT.get() < MAX_IDLE) {
      Ring ring;
      try {
        ring = Ring.create();
      } catch (RuntimeException _) {
        return;
      }
      IDLE_COUNT.incrementAndGet();
      IDLE.add(ring);
    }
  }

  /**
   * Returns {@code current} if it holds at least {@code size} bytes, else a new buffer that does,
   * page-aligned as O_DIRECT reads require. It is freed once unreachable.
   */
  static MemorySegment buffer(MemorySegment current, long size) {
    if (current.byteSize() >= size) {
      return current;
    }
    return Arena.ofAuto().allocate(Math.max(size, PAGE_BYTES), PAGE_BYTES);
  }

  /**
   * Reads {@code length} bytes at every position into {@code buffer} ({@link #buffer}), read {@code
   * k} landing at {@code k * length}. After a failure, reads may still land in {@code buffer}, so
   * the caller must not reuse it.
   */
  MemorySegment readBatch(long[] positions, int length, MemorySegment buffer) throws IOException {
    return readBatch(positions, positions.length, length, buffer);
  }

  /**
   * Reads the first {@code n} of {@code positions}, as {@link #readBatch(long[], int,
   * MemorySegment)}: every chunk is submitted before any is awaited, so all the reads are in flight
   * at once, and whatever is thrown every submitted chunk is awaited before this returns.
   */
  MemorySegment readBatch(long[] positions, int n, int length, MemorySegment buffer)
      throws IOException {
    read(this, null, positions, n, length, buffer);
    return buffer;
  }

  /**
   * {@link #readBatch(long[], int, int, MemorySegment)} over several files: read {@code k} reads
   * {@code files[k]}. One ring can read any number of files, each submission entry naming its own,
   * so a rerank spanning many segments costs one submission and one wait, not one of each per
   * segment.
   */
  static void readBatch(IoUring[] files, long[] positions, int n, int length, MemorySegment buffer)
      throws IOException {
    read(null, files, positions, n, length, buffer);
  }

  /**
   * Reads {@code n} positions of {@code file}, or, when it is null, read {@code k} of {@code
   * files[k]}: every chunk is submitted before any is awaited, so all the reads are in flight at
   * once, and whatever is thrown every submitted chunk is awaited before this returns. Each file is
   * locked against {@link #close} until its reads are submitted.
   */
  private static void read(
      IoUring file, IoUring[] files, long[] positions, int n, int length, MemorySegment buffer)
      throws IOException {
    if (buffer.byteSize() < (long) n * length) {
      throw new IllegalArgumentException("buffer of " + buffer.byteSize() + " bytes is too small");
    }
    if (FAILED_BATCHES.get() >= MAX_FAILED_BATCHES) {
      throw new IOException("io_uring disabled after " + MAX_FAILED_BATCHES + " failed batches");
    }
    // The distinct files, each locked once. Sized for every file up front, so adding one never
    // allocates: a file is added right after it is locked, and the finally below unlocks the list.
    List<IoUring> locked = new ArrayList<>(file != null ? 1 : n);
    int[] fds = null;
    Batch[] batches = new Batch[(n + RING_ENTRIES - 1) / RING_ENTRIES];
    int submitted = 0;
    Throwable failure = null;
    try {
      if (file != null) {
        lock(file, locked);
      } else {
        fds = new int[n];
        for (int k = 0; k < n; k++) {
          if (locked.contains(files[k]) == false) {
            lock(files[k], locked);
          }
          fds[k] = files[k].fd;
        }
      }
      for (; submitted < batches.length; submitted++) {
        int from = submitted * RING_ENTRIES;
        int count = Math.min(RING_ENTRIES, n - from);
        batches[submitted] = submit(file, fds, files, positions, from, count, length, buffer);
      }
    } catch (Throwable t) {
      failure = t;
    } finally {
      // Submitted reads hold the files themselves; the locks only guarded their descriptors.
      for (IoUring f : locked) {
        f.fdLock.readLock().unlock();
      }
    }
    for (int b = 0; b < submitted; b++) {
      try {
        await(batches[b]);
      } catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else {
          failure.addSuppressed(t);
        }
      }
    }
    if (failure == null) {
      return;
    }
    if (failure instanceof IOException e) {
      throw e;
    }
    if (failure instanceof RuntimeException e) {
      throw e;
    }
    throw (Error) failure;
  }

  /**
   * Locks {@code file}'s descriptor against close and adds it to {@code locked}, failing if it is
   * closed already. The caller unlocks every file in {@code locked}, including one that failed
   * here; this method never unlocks, so each lock has exactly one release, in the caller's finally.
   */
  private static void lock(IoUring file, List<IoUring> locked) throws IOException {
    file.fdLock.readLock().lock();
    locked.add(file);
    if (file.closed) {
      throw new IOException("io_uring reader is closed");
    }
  }

  /**
   * Reads in flight on one ring, from {@link #submit} until {@link #await}. A batch must be awaited
   * exactly once, by any thread, before its buffer is read or reused.
   */
  static final class Batch {
    private Ring ring;
    private final int count;
    private final int length;
    private final MemorySegment buffer;

    // What a read must return to have succeeded: its length, or less for a read that ends its
    // file, the batch's file or, reading several, files[read]. The positions must not change until
    // the batch is awaited.
    private final long[] positions;
    private final IoUring file;
    private final IoUring[] files;

    private Batch(
        Ring ring,
        int count,
        int length,
        MemorySegment buffer,
        long[] positions,
        IoUring file,
        IoUring[] files) {
      this.ring = ring;
      this.count = count;
      this.length = length;
      this.buffer = buffer;
      this.positions = positions;
      this.file = file;
      this.files = files;
    }

    /** The length of the file read {@code read} reads. */
    long fileLength(int read) {
      return (file != null ? file : files[read]).fileLength;
    }
  }

  /**
   * Submits one ring's worth of reads, of {@code file} or, when it is null, read {@code k} of
   * descriptor {@code fds[k]} ({@code files[k]}); the caller holds their locks.
   */
  private static Batch submit(
      IoUring file,
      int[] fds,
      IoUring[] files,
      long[] positions,
      int from,
      int count,
      int length,
      MemorySegment buffer)
      throws IOException {
    if (count < 1 || count > RING_ENTRIES) {
      throw new IllegalArgumentException(
          "a batch holds 1 to " + RING_ENTRIES + " reads; got " + count);
    }
    Ring ring = IDLE.poll();
    if (ring != null) {
      IDLE_COUNT.decrementAndGet();
    } else {
      if (System.nanoTime() - setupRetryAt < 0) {
        throw new IOException("io_uring ring setup is paused after a failure");
      }
      try {
        ring = Ring.create();
      } catch (RuntimeException e) {
        // Nothing leaked: create undoes its own work. Pause new setups instead of counting it.
        setupRetryAt = System.nanoTime() + SETUP_BACKOFF_NANOS;
        if (SETUP_FAILURE_LOGGED.compareAndSet(false, true)) {
          System.getLogger(IoUring.class.getName())
              .log(
                  System.Logger.Level.WARNING,
                  "io_uring ring setup failed ("
                      + e.getMessage()
                      + "); reranks read through the mapping until rings can be set up. On"
                      + " kernels before 5.12, raise the locked-memory limit (ulimit -l, or"
                      + " LimitMEMLOCK under systemd).");
        }
        throw new IOException("io_uring unavailable", e);
      }
    }
    // Allocated before submitting: once the kernel holds the reads, nothing may fail before the
    // Batch that awaits them exists.
    Batch batch = new Batch(ring, count, length, buffer, positions, file, files);
    try {
      ring.submit(file == null ? -1 : file.fd, fds, positions, from, count, length, buffer);
    } catch (Throwable t) {
      // Some reads may have been submitted, so the ring is not reused; an Error too, such as one
      // allocating a syscall's arena, must not leave the buffer unretired under them.
      ring.retire(buffer);
      if (t instanceof Error e) {
        throw e;
      }
      throw new IOException("io_uring submission failed", t);
    }
    return batch;
  }

  /**
   * Waits for every read of {@code batch} and returns its buffer. Throws if a read came back short;
   * the ring goes back to the pool either way once its reads are done.
   *
   * <p>The wait has no timeout and is not interruptible: if the device stalls, so does the caller,
   * as a page fault through the mapping would. A timeout could not free the buffer either, since
   * the reads would still be in flight, so it would only retire the ring and buffer.
   */
  private static MemorySegment await(Batch batch) throws IOException {
    Ring ring = batch.ring;
    if (ring == null) {
      throw new IllegalStateException("batch already awaited");
    }
    batch.ring = null;
    IOException shortRead;
    try {
      shortRead = ring.reap(batch);
    } catch (Throwable t) {
      // Reads may still be in flight, so the ring is not reused, whatever was thrown.
      ring.retire(batch.buffer);
      if (t instanceof Error e) {
        throw e;
      }
      throw new IOException("io_uring batch failed", t);
    }
    // Every read of the batch completed, so the ring is idle again.
    release(ring);
    if (shortRead != null) {
      throw shortRead;
    }
    return batch.buffer;
  }

  private static void release(Ring ring) {
    if (IDLE_COUNT.get() < MAX_IDLE && OPEN.get() > 0) {
      IDLE_COUNT.incrementAndGet();
      IDLE.add(ring);
    } else {
      ring.free();
    }
  }

  /** The rings currently idle, for tests. */
  static int idleRings() {
    return IDLE_COUNT.get();
  }

  /** Closes the file and, if this was the last open instance, frees the idle rings. Idempotent. */
  @Override
  public void close() throws IOException {
    fdLock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      // Waits for submissions still writing or handing over this descriptor; later ones fail.
      closed = true;
    } finally {
      fdLock.writeLock().unlock();
    }
    try {
      closeFd(fd);
    } finally {
      if (OPEN.decrementAndGet() == 0) {
        // An instance opened meanwhile creates rings as it needs them.
        for (Ring ring = IDLE.poll(); ring != null; ring = IDLE.poll()) {
          IDLE_COUNT.decrementAndGet();
          ring.free();
        }
      }
    }
  }

  private static void closeFd(int fd) throws IOException {
    int rc;
    try {
      rc = (int) Native.CLOSE.invokeExact(fd);
    } catch (Throwable t) {
      throw new IOException("close failed", t);
    }
    if (rc != 0) {
      throw new IOException("close failed");
    }
  }

  /**
   * Makes a raw syscall with up to four arguments; the last two argument registers are passed as
   * zero (io_uring_enter's sigset and its size, unused here). Returns what the kernel returned: a
   * result, or {@code -errno} on failure. libc's {@code syscall} itself returns -1 and leaves the
   * error in errno, so errno is captured and negated here; callers compare against {@code -EINTR}
   * and report the errno in messages. Package-private for tests.
   */
  static long syscall(long number, long a, long b, long c, long d) throws Throwable {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment state = arena.allocate(Native.CALL_STATE);
      long ret = (long) Native.SYSCALL.invokeExact(state, number, a, b, c, d, 0L, 0L);
      if (ret != -1) {
        return ret;
      }
      return -state.get(ValueLayout.JAVA_INT, Native.ERRNO_OFFSET);
    }
  }

  /**
   * A submission ring, its completion ring and its submission entries, used by one batch at a time.
   */
  private static final class Ring {
    final int ringFd;
    final MemorySegment sq;
    final MemorySegment cq;
    final MemorySegment sqes;

    // Offsets of ring fields within the mapped rings, read from io_uring_params, and index masks.
    final int sqTail;
    final int sqArray;
    final int sqMask;
    final int cqHead;
    final int cqTail;
    final int cqCqes;
    final int cqMask;

    /**
     * Counts this ring's batches. Each read is tagged with its batch, so a completion left over
     * from an earlier batch is recognized and skipped instead of being counted for the current one.
     */
    private int generation;

    private Ring(
        int ringFd, MemorySegment params, MemorySegment sq, MemorySegment cq, MemorySegment sqes) {
      this.ringFd = ringFd;
      this.sq = sq;
      this.cq = cq;
      this.sqes = sqes;
      sqTail = params.get(ValueLayout.JAVA_INT, SQ_OFF_TAIL);
      sqMask = sq.get(ValueLayout.JAVA_INT, params.get(ValueLayout.JAVA_INT, SQ_OFF_RING_MASK));
      sqArray = params.get(ValueLayout.JAVA_INT, SQ_OFF_ARRAY);
      cqHead = params.get(ValueLayout.JAVA_INT, CQ_OFF_HEAD);
      cqTail = params.get(ValueLayout.JAVA_INT, CQ_OFF_TAIL);
      cqMask = cq.get(ValueLayout.JAVA_INT, params.get(ValueLayout.JAVA_INT, CQ_OFF_RING_MASK));
      cqCqes = params.get(ValueLayout.JAVA_INT, CQ_OFF_CQES);
    }

    /**
     * Sets up a ring and maps it. On failure, unmaps what was mapped and closes the ring before
     * throwing, so a failed setup leaks nothing.
     */
    static Ring create() {
      MemorySegment params = Arena.ofAuto().allocate(PARAMS_SIZE);
      // Each is set once it exists, so the failure path knows what to undo.
      long ringFd = -1;
      MemorySegment sq = null;
      MemorySegment cq = null;
      MemorySegment sqes = null;
      try {
        // 1. Ask for a ring of RING_ENTRIES submissions; the kernel fills params with the sizes it
        //    chose (the CQ ring gets twice the entries) and the layout of each ring.
        ringFd = syscall(SYS_IO_URING_SETUP, RING_ENTRIES, params.address(), 0, 0);
        if (ringFd < 0) {
          throw new IllegalStateException("io_uring_setup errno=" + -ringFd);
        }
        // 2. Size the mappings: each ring ends with its array, of 4-byte SQE indices for the SQ
        //    ring and of 16-byte CQEs for the CQ ring.
        int sqEntries = params.get(ValueLayout.JAVA_INT, PARAMS_SQ_ENTRIES);
        int cqEntries = params.get(ValueLayout.JAVA_INT, PARAMS_CQ_ENTRIES);
        int features = params.get(ValueLayout.JAVA_INT, PARAMS_FEATURES);
        long sqSize =
            params.get(ValueLayout.JAVA_INT, SQ_OFF_ARRAY) + (long) sqEntries * Integer.BYTES;
        long cqSize = params.get(ValueLayout.JAVA_INT, CQ_OFF_CQES) + (long) cqEntries * CQE_SIZE;
        // 3. Map the rings and the SQE array into this process.
        if ((features & IORING_FEAT_SINGLE_MMAP) != 0) {
          // The completion ring shares the submission ring's mapping.
          sq = map(Math.max(sqSize, cqSize), ringFd, OFF_SQ_RING);
          cq = sq;
        } else {
          sq = map(sqSize, ringFd, OFF_SQ_RING);
          cq = map(cqSize, ringFd, OFF_CQ_RING);
        }
        sqes = map((long) sqEntries * SQE_SIZE, ringFd, OFF_SQES);
        // Zeroed once: a read sets only its own fields, so every other field stays zero.
        sqes.fill((byte) 0);
        return new Ring((int) ringFd, params, sq, cq, sqes);
      } catch (Throwable t) {
        // Nothing else refers to these yet. A failed setup returned -errno, which is no ring.
        int fdToClose = -1;
        if (ringFd >= 0) {
          fdToClose = (int) ringFd;
        }
        freeQuietly(fdToClose, sq, cq, sqes);
        if (t instanceof RuntimeException e) {
          throw e;
        }
        throw new IllegalStateException("io_uring setup failed", t);
      }
    }

    /**
     * Takes the ring out of service after a failure: closes and unmaps it, which the kernel allows
     * with reads in flight. Those reads may still land in {@code buffer}, so the buffer stays
     * reachable and is never freed; after {@link #MAX_FAILED_BATCHES} failures io_uring is turned
     * off.
     */
    void retire(MemorySegment buffer) {
      RETIRED.add(buffer);
      FAILED_BATCHES.incrementAndGet();
      free();
    }

    /** Unmaps the rings and closes the descriptor; only for a ring with no reads in flight. */
    void free() {
      freeQuietly(ringFd, sq, cq, sqes);
    }

    /**
     * Unmaps whichever of the mappings exist (non-null; {@code cq} may be {@code sq} itself) and
     * closes {@code ringFd} if it is a descriptor (non-negative). Best effort: each step runs even
     * if an earlier one failed, and nothing else refers to the ring.
     */
    private static void freeQuietly(
        int ringFd, MemorySegment sq, MemorySegment cq, MemorySegment sqes) {
      unmapQuietly(sqes);
      if (!Objects.equals(cq, sq)) {
        unmapQuietly(cq);
      }
      unmapQuietly(sq);
      if (ringFd >= 0) {
        try {
          int _ = (int) Native.CLOSE.invokeExact(ringFd);
        } catch (Throwable _) {
          // Best effort, as above.
        }
      }
    }

    private static void unmapQuietly(MemorySegment mapping) {
      if (mapping == null) {
        return;
      }
      try {
        int _ = (int) Native.MUNMAP.invokeExact(mapping, mapping.byteSize());
      } catch (Throwable _) {
        // Best effort, as above.
      }
    }

    /**
     * Submits a read of {@code length} bytes at each of {@code count} positions from {@code from},
     * read {@code i} landing at {@code i * length} of {@code buffer}, without waiting for them.
     */
    void submit(
        int fd, int[] fds, long[] positions, int from, int count, int length, MemorySegment buffer)
        throws IOException {
      // 1. One submission entry per read, tagged with the batch and its index. The entries were
      //    zeroed when the ring was made, and only these fields are ever written. This ring holds
      //    one batch at a time and every earlier one was reaped, so the SQ ring is empty and the
      //    count, at most RING_ENTRIES, always fits.
      generation++;
      long address = buffer.address();
      int tail = sq.get(ValueLayout.JAVA_INT, sqTail);
      for (int k = 0; k < count; k++) {
        int index = (tail + k) & sqMask;
        long sqe = (long) index * SQE_SIZE;
        int read = from + k;
        sqes.set(ValueLayout.JAVA_BYTE, sqe, (byte) IORING_OP_READ);
        // Each entry names its file, so one ring can read several: fds[read], or fd for all.
        sqes.set(ValueLayout.JAVA_INT, sqe + SQE_FD, fds == null ? fd : fds[read]);
        sqes.set(ValueLayout.JAVA_LONG, sqe + SQE_OFF, positions[read]);
        sqes.set(ValueLayout.JAVA_LONG, sqe + SQE_ADDR, address + (long) read * length);
        sqes.set(ValueLayout.JAVA_INT, sqe + SQE_LEN, length);
        // The tag comes back in the read's CQE: the batch in the high half, the read in the low.
        sqes.set(ValueLayout.JAVA_LONG, sqe + SQE_USER_DATA, (long) generation << 32 | read);
        // The SQ ring holds SQE indices, not SQEs: slot i names SQE i, the identity mapping.
        sq.set(ValueLayout.JAVA_INT, sqArray + (long) index * Integer.BYTES, index);
      }
      // 2. Publish the entries by moving the tail past them.
      // Release: every entry above is visible before the kernel can see the new tail that covers
      // it. The second fence keeps the tail store ahead of io_uring_enter, where the kernel reads
      // it.
      VarHandle.fullFence();
      sq.set(ValueLayout.JAVA_INT, sqTail, tail + count);
      VarHandle.fullFence();
      // 3. Hand them to the kernel. It may take fewer than asked, or be interrupted by a signal
      //    before taking any; retry until it has taken them all.
      for (int submitted = 0; submitted < count; ) {
        long ret = enter(count - submitted, 0, 0);
        if (ret == -EINTR) {
          continue;
        }
        if (ret <= 0) {
          throw new IOException("io_uring_enter submit errno=" + -ret);
        }
        submitted += (int) ret;
      }
    }

    /**
     * Waits for the {@code count} reads of the last submission. Returns null, or the failure of a
     * short read once every read has completed; throws if reads may still be in flight.
     */
    IOException reap(Batch batch) throws IOException {
      int count = batch.count;
      int length = batch.length;
      // Completions that already arrived are taken without a syscall; then wait for the rest,
      // retrying interrupted waits. A short read fails the batch, but only once all are reaped.
      int reaped = 0;
      IOException failure = null;
      while (true) {
        // 1. The completions available: those between the head (ours) and the tail (the kernel's).
        // Only this thread moves the head; the kernel moves the tail.
        int head = cq.get(ValueLayout.JAVA_INT, cqHead);
        int end = cq.get(ValueLayout.JAVA_INT, cqTail);
        // Acquire: pairs with the kernel's store-release of the tail, so the entries it covers are
        // read only after it, and their contents are complete. Without it a weakly ordered CPU
        // such as aarch64 may read an entry before the kernel finished writing it.
        VarHandle.acquireFence();
        // 2. Consume them. A result short of what the read should return is a failed read (res <
        //    0 is -errno); it fails the batch, but the rest are still reaped.
        for (; head != end; head++) {
          long cqe = cqCqes + (long) (head & cqMask) * CQE_SIZE;
          long tag = cq.get(ValueLayout.JAVA_LONG, cqe + CQE_USER_DATA);
          if ((int) (tag >>> 32) != generation) {
            // An earlier batch's completion.
            continue;
          }
          reaped++;
          int read = (int) tag;
          int result = cq.get(ValueLayout.JAVA_INT, cqe + CQE_RES);
          // A read that ends the file returns only the bytes up to its end: it succeeded if it
          // returned those. Nothing at or past the end can succeed.
          long want = Math.min(length, batch.fileLength(read) - batch.positions[read]);
          if ((want <= 0 || result < want) && failure == null) {
            failure =
                new ReadRefusedException(
                    "io_uring read " + read + " res=" + result + " want=" + want);
          }
        }
        // 3. Free their slots. Release: the entries are read before the kernel can see the head
        //    move past them and reuse their slots.
        VarHandle.fullFence();
        cq.set(ValueLayout.JAVA_INT, cqHead, head);
        if (reaped >= count) {
          return failure;
        }
        // 4. Block until the rest have completed, then take them on the next pass. An interrupted
        //    wait is simply retried.
        long ret = enter(0, count - reaped, IORING_ENTER_GETEVENTS);
        if (ret < 0 && ret != -EINTR) {
          throw new IOException("io_uring_enter wait errno=" + -ret);
        }
      }
    }

    private long enter(int toSubmit, int minComplete, int flags) throws IOException {
      try {
        return syscall(SYS_IO_URING_ENTER, ringFd, toSubmit, minComplete, flags);
      } catch (Throwable t) {
        throw new IOException("io_uring_enter failed", t);
      }
    }

    private static MemorySegment map(long length, long fd, long offset) throws Throwable {
      MemorySegment address =
          (MemorySegment)
              Native.MMAP.invokeExact(
                  MemorySegment.NULL, length, PROT_READ_WRITE, MAP_SHARED, (int) fd, offset);
      if (address.address() == -1L) {
        throw new IllegalStateException("io_uring mmap failed");
      }
      return address.reinterpret(length);
    }
  }
}
