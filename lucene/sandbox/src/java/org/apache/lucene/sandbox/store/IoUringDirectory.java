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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.store.VectorBatchCapable;

/**
 * Experimental {@link org.apache.lucene.store.Directory} that serves the raw float32 vector file
 * ({@code .vec}) through io_uring with {@code O_DIRECT}, so a larger-than-RAM KNN rerank can fetch
 * a candidate shortlist as a single batched submission that bypasses the page cache. The submission
 * is batched, not asynchronous: the calling thread issues one {@code io_uring_enter} for the whole
 * shortlist and blocks until it has reaped every completion, which is what removes the per-read
 * syscall and cross-thread hand-off without needing a read-thread pool. The {@code .vec} input
 * implements {@link VectorBatchCapable}; all other files (HNSW graph, quantized codes, metadata)
 * are served unchanged by the wrapped {@link FSDirectory}, staying page-cached.
 *
 * <p>Memory: each ring registers a {@code queueDepth × 4 KB} buffer with the kernel, and up to
 * {@code maxRings} exist at once, so the default 256/64 configuration can hold ~64 MB of registered
 * buffers off-heap. Size {@code maxRings} to the read concurrency you actually want.
 *
 * <p>io_uring support (a recent Linux kernel plus {@code liburing-ffi}) is detected at
 * construction. When it is unavailable this directory is a transparent pass-through: {@code .vec}
 * is served by the delegate and the rerank path reads it serially, so io_uring is a pure opt-in
 * upgrade and never a hard dependency. Requires {@code --enable-native-access}.
 *
 * @lucene.experimental
 */
public final class IoUringDirectory extends FilterDirectory {

  /** Default cap on concurrently borrowed rings; also the default read concurrency. */
  public static final int DEFAULT_MAX_RINGS = 64;

  private final Path dir;
  private final boolean enabled;
  private final IoUring engine;

  /**
   * System property master switch. io_uring is <b>opt-in</b>: it stays off (transparent
   * pass-through to the delegate) unless this property is {@code true} or the explicit-enable
   * constructor is used.
   */
  public static final String ENABLE_PROPERTY = "lucene.store.ioUring";

  /**
   * Wraps {@code delegate}; io_uring is enabled only if the {@link #ENABLE_PROPERTY} system
   * property is set to {@code true} (and the platform supports it). Otherwise this is a
   * pass-through.
   */
  public IoUringDirectory(FSDirectory delegate) throws IOException {
    this(delegate, 256, Boolean.getBoolean(ENABLE_PROPERTY));
  }

  /**
   * Wraps {@code delegate}, serving {@code .vec} via io_uring when {@code enable} is true and the
   * platform supports it; a pure pass-through otherwise.
   *
   * @param queueDepth io_uring submission-queue depth per ring (rounded up to a power of 2)
   * @param enable explicit opt-in; io_uring is never activated by default
   */
  public IoUringDirectory(FSDirectory delegate, int queueDepth, boolean enable) throws IOException {
    this(delegate, queueDepth, enable, DEFAULT_MAX_RINGS);
  }

  /**
   * Wraps {@code delegate}, serving {@code .vec} via io_uring when {@code enable} is true and the
   * platform supports it; a pure pass-through otherwise.
   *
   * @param queueDepth io_uring submission-queue depth per ring (rounded up to a power of 2)
   * @param enable explicit opt-in; io_uring is never activated by default
   * @param maxRings cap on rings borrowed at once, so ring memory is bounded by read concurrency
   *     rather than by the number of threads that reach the rerank path; readers beyond the cap
   *     wait for a ring
   */
  public IoUringDirectory(FSDirectory delegate, int queueDepth, boolean enable, int maxRings)
      throws IOException {
    super(delegate);
    this.dir = delegate.getDirectory();
    boolean on = enable && IoUring.isAvailable();
    this.enabled = on;
    this.engine = on ? new IoUring(queueDepth, maxRings) : null;
  }

  /** Whether io_uring is active (false means transparent fallback to the delegate). */
  public boolean isIoUringEnabled() {
    return enabled;
  }

  /** Rings currently allocated by the engine. Visible for testing. */
  int ringCount() {
    return engine == null ? 0 : engine.ringCount();
  }

  /** O_DIRECT descriptors currently held by open inputs. Visible for testing. */
  int openFdCount() {
    return engine == null ? 0 : engine.openFdCount();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput delegateInput = in.openInput(name, context);
    // Merges read .vec front to back, where batching buys nothing and O_DIRECT gives up read-ahead,
    // so only search-time opens get the batch capability.
    if (enabled == false
        || name.endsWith(".vec") == false
        || context.context() == IOContext.Context.MERGE) {
      return delegateInput;
    }
    int fd;
    try {
      fd = engine.openDirect(dir.resolve(name).toString());
    } catch (IOException _) {
      // O_DIRECT open failed for this file; fall back to the delegate input.
      return delegateInput;
    }
    return new UringInput("IoUringInput(" + name + ")", delegateInput, engine, fd);
  }

  @Override
  public void close() throws IOException {
    try {
      if (engine != null) {
        engine.close(); // frees the rings; fds belong to the inputs that opened them
      }
    } finally {
      super.close();
    }
  }

  /**
   * Delegating input whose shortlist reads go through io_uring; normal reads use the delegate. It
   * owns the O_DIRECT fd, so the fd lives exactly as long as the input: closing the reader of a
   * segment that has been merged away releases the file rather than pinning it until the directory
   * closes.
   */
  private static final class UringInput extends FilterIndexInput implements VectorBatchCapable {
    private final IoUring engine;
    private final int fd;

    UringInput(String desc, IndexInput in, IoUring engine, int fd) {
      super(desc, in);
      this.engine = engine;
      this.fd = fd;
    }

    @Override
    public VectorBatch newBatch() {
      return new UringBatch(engine);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        engine.closeFd(fd);
      }
    }

    @Override
    public IndexInput clone() {
      // Plain independent clone for the normal codec read path: it shares no state with this input
      // and must not close the fd, which only the original owns.
      return in.clone();
    }
  }

  /**
   * Gathers reads from any {@code .vec} input of this directory — they all share one engine, and a
   * submission entry carries its own descriptor — so a shortlist spread over every segment goes out
   * as a single submission.
   */
  private static final class UringBatch implements VectorBatch {
    private final IoUring engine;
    private final IoUring.Batch batch;

    UringBatch(IoUring engine) {
      this.engine = engine;
      this.batch = engine.newBatch();
    }

    @Override
    public boolean add(IndexInput in, long[] positions, int dim, int count, float[] out)
        throws IOException {
      if (in instanceof UringInput u && u.engine == engine) {
        batch.add(u.fd, positions, dim, count, out);
        return true;
      }
      return false; // a different directory's input: the caller reads it on its own
    }

    @Override
    public void execute() throws IOException {
      batch.execute();
    }
  }
}
