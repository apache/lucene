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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * Serves reads of the full-precision vector file through io_uring, starting them in {@link
 * IndexInput#prefetch} and consuming them in the read that needs them. Nothing beyond the standard
 * {@code IndexInput} contract is asked of callers: one that prefetches several candidates before
 * reading any of them gets that many reads in flight, and one that does not still gets correct, if
 * serial, reads.
 *
 * <p>Reads carry {@code RWF_DONTCACHE}, which keeps the pages out of the page cache without the
 * alignment constraints of {@code O_DIRECT}, so the graph and quantized codes that every query
 * touches are not evicted by a one-shot rerank. A configurable share of reads is admitted to the
 * page cache instead: repeated reads then hit the cache, while most reads still leave it alone.
 *
 * <p>Only the full-precision vector file is served, and deliberately by extension rather than by
 * {@link FileDataHint#KNN_VECTORS}: that hint also covers the graph and the quantized codes, and
 * wrapping those costs far more than io_uring wins. The vectorized scorers take a fast path only
 * when the input is a {@code MemorySegmentAccessInput}, which this is not, so wrapping the codes
 * drops the first-phase scoring off it. Measured at 180 queries per second on a 25M vector index,
 * that was 9 ms at the 99th percentile. Selecting by hint needs a hint that distinguishes the
 * full-precision vectors from the rest, which does not exist today.
 *
 * <p>For the same reason this belongs in front of an index whose search phase scores quantized
 * vectors, where the full-precision file is read only to rerank. On an index that searches the
 * full-precision vectors directly, wrapping their file would take that scoring off the fast path
 * too.
 *
 * @lucene.experimental
 */
public final class IoUringUncachedDirectory extends FilterDirectory {

  /** System property that turns this directory on; it is a no-op otherwise. */
  public static final String ENABLE_PROPERTY = "lucene.store.ioUringUncached";

  private final Path dir;
  private final IoUring engine;

  /** Extension of the full-precision vector file, the only one whose reads are prefetched. */
  private static final String VECTOR_EXTENSION = ".vec";

  /**
   * Serve every file rather than only the vector file. Exists so that the generic directory
   * compliance tests exercise these reads at all rather than the delegate's.
   */
  private final boolean serveEveryFile;

  /** Percentage of reads, 0 to 100, served without {@code RWF_DONTCACHE}. */
  private final int admitPercent;

  /** Seeds the admission choice of each input, so that clones do not repeat one sequence. */
  private static final AtomicLong SEEDS = new AtomicLong();

  /** Set once the first vector file is opened and the kernel's answer on RWF_DONTCACHE is known. */
  private volatile Boolean uncached;

  /**
   * Reads a ring keeps in flight. This has to cover a whole rerank batch: a read that finds the
   * ring full falls back to the delegate, and those faults cost more than the depth saves. Measured
   * at 180 queries per second on a 25M vector index, depth 64 took 19,799 major faults and 78.7 ms
   * at the 99th percentile where depth 256 took 2 faults and 68.4 ms.
   */
  private static final int DEFAULT_QUEUE_DEPTH = 256;

  /** Largest read served through io_uring: enough for a 4096-dimension float vector. */
  private static final int DEFAULT_MAX_READ_BYTES = 16 * 1024;

  /**
   * Wraps {@code delegate}, enabled only if the system property is set and io_uring is available.
   */
  public IoUringUncachedDirectory(FSDirectory delegate) throws IOException {
    this(delegate, DEFAULT_QUEUE_DEPTH, Boolean.getBoolean(ENABLE_PROPERTY), defaultMaxRings());
  }

  /** Keeps the default ring count for callers that do not care. */
  public IoUringUncachedDirectory(FSDirectory delegate, int queueDepth, boolean enable)
      throws IOException {
    this(delegate, queueDepth, enable, defaultMaxRings());
  }

  private static int defaultMaxRings() {
    return Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
  }

  /**
   * @param queueDepth reads a ring may keep in flight
   * @param enable whether to use io_uring at all
   * @param maxRings rings to share between concurrent batches; a read that finds none free is left
   *     to the delegate rather than waiting
   */
  public IoUringUncachedDirectory(
      FSDirectory delegate, int queueDepth, boolean enable, int maxRings) throws IOException {
    this(delegate, queueDepth, enable, maxRings, 0);
  }

  /**
   * @param admitPercent percentage of reads, 0 to 100, that are left in the page cache rather than
   *     read with {@code RWF_DONTCACHE}
   */
  public IoUringUncachedDirectory(
      FSDirectory delegate, int queueDepth, boolean enable, int maxRings, int admitPercent)
      throws IOException {
    this(delegate, queueDepth, enable, maxRings, admitPercent, false);
  }

  IoUringUncachedDirectory(
      FSDirectory delegate,
      int queueDepth,
      boolean enable,
      int maxRings,
      int admitPercent,
      boolean serveEveryFile)
      throws IOException {
    super(delegate);
    if (admitPercent < 0 || admitPercent > 100) {
      throw new IllegalArgumentException("admitPercent must be 0 to 100, got " + admitPercent);
    }
    this.admitPercent = admitPercent;
    this.serveEveryFile = serveEveryFile;
    this.dir = delegate.getDirectory();
    IoUring started = null;
    if (enable && IoUring.isAvailable()) {
      try {
        started = new IoUring(queueDepth, maxRings, DEFAULT_MAX_READ_BYTES);
      } catch (Throwable _) {
        started = null; // fall back to the delegate rather than fail the open
      }
    }
    this.engine = started;
  }

  /** Whether reads are actually being served through io_uring. */
  public boolean isEnabled() {
    return engine != null;
  }

  /**
   * Whether reads keep their pages out of the page cache, known once a vector file has been opened.
   * False on a kernel older than 6.14: reads still go through io_uring and still gain queue depth,
   * but they leave the data behind, so a caller relying on the cache being left alone should check.
   */
  public boolean isUncached() {
    return Boolean.TRUE.equals(uncached);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegateInput = in.openInput(name, context);
    if (engine == null || (serveEveryFile == false && name.endsWith(VECTOR_EXTENSION) == false)) {
      return delegateInput;
    }
    try {
      int fd = engine.openPlain(dir.resolve(name).toString());
      uncached = engine.isUncached(fd);
      return new UncachedInput(delegateInput, engine, fd, admitPercent);
    } catch (Throwable _) {
      return delegateInput;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (engine != null) {
        engine.close();
      }
    }
  }

  /**
   * Reads the vector file through io_uring. The delegate owns the position, so seeking, slicing and
   * cloning keep working; only the bytes come from elsewhere. The descriptor is closed by the input
   * {@code openInput} returned, since clones must not outlive it.
   */
  private static final class UncachedInput extends FilterIndexInput {
    private final IoUring engine;
    private final int fd;
    private final long base;
    private final boolean ownsFd;
    private final int admitPercent;
    // a Weyl sequence: distinct and never zero, as xorshift needs
    private long rnd = SEEDS.addAndGet(0x9E3779B97F4A7C15L);
    private byte[] scratch;
    private boolean closed;

    UncachedInput(IndexInput in, IoUring engine, int fd, int admitPercent) {
      this(in, engine, fd, 0L, true, admitPercent);
    }

    private UncachedInput(
        IndexInput in, IoUring engine, int fd, long base, boolean ownsFd, int admitPercent) {
      super("IoUringUncachedInput(" + in + ")", in);
      this.engine = engine;
      this.fd = fd;
      this.base = base;
      this.ownsFd = ownsFd;
      this.admitPercent = admitPercent;
    }

    /**
     * Starts the read through io_uring, or, if the engine cannot take it, passes the hint to the
     * delegate, so that the later read still has its pages on the way.
     */
    @Override
    public boolean prefetch(long offset, long length) throws IOException {
      if (length > 0
          && length <= engine.maxReadBytes()
          && engine.startRead(fd, base + offset, (int) length, dontCacheThisRead())) {
        return true;
      }
      return in.prefetch(offset, length);
    }

    /** False for the admitted share of reads, which then stay in the page cache. */
    private boolean dontCacheThisRead() {
      if (admitPercent == 0) {
        return true;
      }
      if (admitPercent == 100) {
        return false;
      }
      rnd ^= rnd << 13;
      rnd ^= rnd >>> 7;
      rnd ^= rnd << 17;
      return Math.floorMod(rnd, 100) >= admitPercent;
    }

    /**
     * Consumes a read started earlier for the current position, advancing the delegate to match,
     * and returns the bytes transferred or a value other than {@code bytes} if the caller must read
     * the range itself.
     */
    private int consume(byte[] into, int intoOffset, int bytes) throws IOException {
      int got = engine.finishRead(fd, base + in.getFilePointer(), bytes, into, intoOffset);
      if (got == bytes) {
        in.seek(in.getFilePointer() + bytes);
      }
      return got;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      if (consume(b, offset, len) != len) {
        in.readBytes(b, offset, len);
      }
    }

    /**
     * The vector path reads floats, so this has to be served here too or reads go to the delegate.
     */
    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
      int bytes = len * Float.BYTES;
      if (bytes > 0 && bytes <= engine.maxReadBytes()) {
        if (scratch == null || scratch.length < bytes) {
          scratch = new byte[bytes];
        }
        if (consume(scratch, 0, bytes) == bytes) {
          ByteBuffer.wrap(scratch, 0, bytes)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer()
              .get(dst, offset, len);
          return;
        }
      }
      in.readFloats(dst, offset, len);
    }

    /**
     * The delegate's mapping still backs every read this does not serve, so its answer about pages
     * being resident is this input's answer too. {@link FilterIndexInput} does not forward this.
     */
    @Override
    public Optional<Boolean> isLoaded() {
      return in.isLoaded();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new UncachedInput(
          in.slice(sliceDescription, offset, length),
          engine,
          fd,
          base + offset,
          false,
          admitPercent);
    }

    @Override
    public IndexInput clone() {
      return new UncachedInput(in.clone(), engine, fd, base, false, admitPercent);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        if (ownsFd && closed == false) {
          closed = true;
          engine.closeFd(fd);
        }
      }
    }
  }
}
