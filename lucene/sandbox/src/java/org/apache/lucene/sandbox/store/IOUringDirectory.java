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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * Wraps an existing {@link FSDirectory}, serving {@link IndexInput#prefetchRange} through io_uring
 * so that a batch of small random reads is submitted together.
 *
 * <p>This is the only method it implements differently. All other reads go to the wrapped
 * directory.
 *
 * <p>When io_uring is unavailable, or the {@code lucene.store.ioUring} system property is not set,
 * this is a pass-through with no ring and no descriptors. Once active it does not silently degrade:
 * a prefetch it cannot serve throws.
 *
 * @lucene.experimental
 */
public final class IOUringDirectory extends FilterDirectory {

  /** Set this to {@code true} to enable the ring. Off by default. */
  public static final String ENABLE_PROPERTY = "lucene.store.ioUring";

  /**
   * Set this to {@code true} to read with {@code RWF_DONTCACHE}, where the filesystem supports it.
   * Off by default.
   */
  public static final String DONTCACHE_PROPERTY = "lucene.store.ioUring.dontcache";

  /** Extensions served through the ring. Everything else is handed to the delegate untouched. */
  private static final Set<String> DEFAULT_EXTENSIONS = Set.of("vec");

  private static final int DEFAULT_QUEUE_DEPTH = 64;
  private static final int DEFAULT_MAX_RINGS = 64;

  private final Path directory;
  private final Set<String> extensions;
  private final boolean enabled;
  private final boolean dontCache;

  /** Null when disabled, which is what makes the disabled path free. */
  private final IOUringEngine engine;

  public IOUringDirectory(FSDirectory inner) throws IOException {
    this(
        inner,
        inner.getDirectory(),
        DEFAULT_EXTENSIONS,
        DEFAULT_QUEUE_DEPTH,
        DEFAULT_MAX_RINGS,
        Boolean.getBoolean(ENABLE_PROPERTY));
  }

  /** For tests and benchmarks, which need to switch it on without a system property. */
  public IOUringDirectory(
      FSDirectory inner, Set<String> extensions, int queueDepth, int maxRings, boolean enable)
      throws IOException {
    this(inner, inner.getDirectory(), extensions, queueDepth, maxRings, enable);
  }

  private IOUringDirectory(
      Directory inner,
      Path directory,
      Set<String> extensions,
      int queueDepth,
      int maxRings,
      boolean enable)
      throws IOException {
    super(inner);
    this.directory = directory;
    this.extensions = Set.copyOf(extensions);
    // Probed in the index directory because RWF_DONTCACHE depends on the filesystem.
    LibUring.Support support = enable ? LibUring.probe(directory) : LibUring.Support.NONE;
    this.enabled = support.available();
    this.dontCache = support.dontcache() && Boolean.getBoolean(DONTCACHE_PROPERTY);
    this.engine =
        support.available()
            ? new IOUringEngine(queueDepth, maxRings, dontCache ? LibUring.RWF_DONTCACHE : 0)
            : null;
  }

  /** Whether reads are actually going through a ring, as opposed to straight to the delegate. */
  public boolean isEnabled() {
    return enabled;
  }

  /** Whether reads use {@code RWF_DONTCACHE}: requested and supported by the filesystem. */
  public boolean usesDontCache() {
    return dontCache;
  }

  /**
   * Counters for how the ring has been driven. Cumulative since this directory was opened; use
   * {@link #minus} to measure a phase.
   *
   * @param rangesServed ranges handed out by {@code prefetchRange}
   * @param syscalls {@code io_uring_enter} calls that reached the kernel
   * @param waitingSyscalls those among them that blocked for at least one completion
   * @param sqesSubmitted reads the kernel accepted; divide by {@code syscalls} for reads per
   *     syscall
   * @param cqesReaped completions consumed
   * @param depthSum reads in flight after each syscall's submit, summed; see {@link #meanDepth}
   * @param fromSlotFull syscalls made because a prefetch found every slot busy
   * @param fromRead syscalls made because a read needed bytes that had not arrived
   * @param fromSqFull syscalls made because the submission queue filled while building entries
   * @param sqesPerSyscall syscalls bucketed by reads submitted: 0, 1, 2-3, 4-7, 8-15, 16-31, 32-63,
   *     64-127, 128 or more. Bucket 0 is a pure wait that submitted nothing.
   * @param cqesPerSyscall syscalls bucketed the same way by completions reaped straight after.
   *     Bucket 0 is a syscall that came back with nothing to reap.
   */
  public record Stats(
      long rangesServed,
      long syscalls,
      long waitingSyscalls,
      long sqesSubmitted,
      long cqesReaped,
      long depthSum,
      long fromSlotFull,
      long fromRead,
      long fromSqFull,
      long[] sqesPerSyscall,
      long[] cqesPerSyscall) {

    /** Bucket labels matching {@link #sqesPerSyscall}. */
    public static final String[] BUCKETS = {
      "0", "1", "2-3", "4-7", "8-15", "16-31", "32-63", "64-127", "128+"
    };

    /** The counters accumulated since {@code earlier}. */
    public Stats minus(Stats earlier) {
      long[] hist = new long[sqesPerSyscall.length];
      long[] reapedHist = new long[cqesPerSyscall.length];
      for (int i = 0; i < hist.length; i++) {
        hist[i] = sqesPerSyscall[i] - earlier.sqesPerSyscall[i];
        reapedHist[i] = cqesPerSyscall[i] - earlier.cqesPerSyscall[i];
      }
      return new Stats(
          rangesServed - earlier.rangesServed,
          syscalls - earlier.syscalls,
          waitingSyscalls - earlier.waitingSyscalls,
          sqesSubmitted - earlier.sqesSubmitted,
          cqesReaped - earlier.cqesReaped,
          depthSum - earlier.depthSum,
          fromSlotFull - earlier.fromSlotFull,
          fromRead - earlier.fromRead,
          fromSqFull - earlier.fromSqFull,
          hist,
          reapedHist);
    }

    /** Reads submitted per syscall. Near 1.0 would mean reads are not being batched. */
    public double readsPerSyscall() {
      return syscalls == 0 ? 0 : (double) sqesSubmitted / syscalls;
    }

    /** Completions consumed per syscall. */
    public double completionsPerSyscall() {
      return syscalls == 0 ? 0 : (double) cqesReaped / syscalls;
    }

    /** Average reads in flight immediately after a syscall submitted. */
    public double meanDepth() {
      return syscalls == 0 ? 0 : (double) depthSum / syscalls;
    }
  }

  /** Counters for how the ring has been driven; all zero when this directory is disabled. */
  public Stats stats() {
    return engine == null
        ? new Stats(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            new long[Stats.BUCKETS.length],
            new long[Stats.BUCKETS.length])
        : engine.stats();
  }

  /** The engine, for tests to inspect ring and descriptor counts. */
  IOUringEngine getEngine() {
    return engine;
  }

  /** How many ranges the ring has served, or zero when disabled. Exposed for benchmarks. */
  public long rangesServed() {
    return engine == null ? 0L : engine.rangesServedCount();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegate = in.openInput(name, context);
    if (enabled == false || servesRanges(name, context) == false) {
      return delegate;
    }
    boolean success = false;
    int fd = engine.openFile(directory.resolve(name).toString());
    try {
      IndexInput input =
          new IOUringIndexInput("IOUringIndexInput(" + name + ")", delegate, engine, fd, 0L, true);
      success = true;
      return input;
    } finally {
      if (success == false) {
        engine.closeFd(fd);
      }
    }
  }

  private boolean servesRanges(String name, IOContext context) {
    // Merges read front to back and are well served by readahead, so only search-time opens get a
    // ring.
    if (context.context() == IOContext.Context.MERGE) {
      return false;
    }
    int dot = name.lastIndexOf('.');
    return dot >= 0 && extensions.contains(name.substring(dot + 1));
  }

  @Override
  public void close() throws IOException {
    try {
      if (engine != null) {
        engine.close();
      }
    } finally {
      super.close();
    }
  }

  /**
   * Adds a ring-backed {@link #prefetchRange} to the delegate input.
   *
   * <p>{@link FilterIndexInput} forwards only the abstract methods of {@link IndexInput}, so {@code
   * clone} and {@code slice} are overridden to keep the capability, and {@code close} to release
   * the descriptor.
   */
  private static final class IOUringIndexInput extends FilterIndexInput {

    private final IOUringEngine engine;
    private final int fd;

    /** This input's byte zero as a file offset, so a slice can translate back for the ring. */
    private final long base;

    /** Only the input returned by {@code openInput} closes the descriptor. */
    private final boolean ownsFd;

    IOUringIndexInput(
        String description,
        IndexInput delegate,
        IOUringEngine engine,
        int fd,
        long base,
        boolean ownsFd) {
      super(description, delegate);
      this.engine = engine;
      this.fd = fd;
      this.base = base;
      this.ownsFd = ownsFd;
    }

    @Override
    public IndexInput prefetchRange(String rangeDescription, long offset, long length)
        throws IOException {
      checkRange(offset, length);
      if (length == 0) {
        return null;
      }
      if (length > Integer.MAX_VALUE) {
        throw new IOException("prefetch range too long: " + length);
      }
      return engine.prefetchRange(rangeDescription, fd, base + offset, (int) length);
    }

    @Override
    public IndexInput clone() {
      return new IOUringIndexInput(toString(), in.clone(), engine, fd, base, false);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new IOUringIndexInput(
          sliceDescription,
          in.slice(sliceDescription, offset, length),
          engine,
          fd,
          base + offset,
          false);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        if (ownsFd) {
          engine.closeFd(fd);
        }
      }
    }
  }
}
