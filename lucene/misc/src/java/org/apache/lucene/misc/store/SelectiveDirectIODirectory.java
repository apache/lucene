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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.store.VectorBatchCapable;
import org.apache.lucene.util.ArrayUtil;

/**
 * Example {@link DirectIODirectory} that applies {@code O_DIRECT} only to the raw float32 vector
 * file ({@code .vec}), so that larger-than-RAM rerank reads bypass the OS page cache while the HNSW
 * graph ({@code .vex}), quantized codes and metadata keep using the mmap delegate and stay
 * page-cached. Combined with 4KB-aligned vector data (see {@code Lucene99FlatVectorsWriter}), each
 * vector read is a single aligned block with no read amplification.
 *
 * <p>The {@code .vec} input is a {@link VectorBatchCapable}: when a read {@link Executor} is
 * supplied, the KNN full-precision rerank path fetches a candidate shortlist as one concurrent
 * batch (see {@link ExecutorBatch}). The executor is owned by the caller (created and shut down by
 * them, like {@link org.apache.lucene.search.IndexSearcher}'s executor); this directory only
 * references it. With no executor the shortlist is read serially on the calling thread. Sizing the
 * read pool independently of the searcher's executor lets a larger-than-RAM deployment keep enough
 * reads in flight to saturate the device without widening CPU-side search parallelism.
 *
 * @lucene.experimental
 */
public final class SelectiveDirectIODirectory extends DirectIODirectory {

  /** Default upper bound on the number of read tasks a shortlist is split into. */
  public static final int DEFAULT_MAX_READ_TASKS = 64;

  private final Executor readExecutor;
  private final int maxReadTasks;

  /** Wraps {@code delegate} with serial rerank reads (no read executor). */
  public SelectiveDirectIODirectory(FSDirectory delegate) throws IOException {
    this(delegate, null);
  }

  /**
   * Wraps {@code delegate}, fetching {@code .vec} rerank shortlists through {@code readExecutor}.
   *
   * @param readExecutor caller-owned executor for parallel rerank reads, or {@code null} for serial
   */
  public SelectiveDirectIODirectory(FSDirectory delegate, Executor readExecutor)
      throws IOException {
    this(delegate, readExecutor, DEFAULT_MAX_READ_TASKS);
  }

  /**
   * Wraps {@code delegate}, fetching {@code .vec} rerank shortlists through {@code readExecutor}.
   *
   * @param readExecutor caller-owned executor for parallel rerank reads, or {@code null} for serial
   * @param maxReadTasks target number of tasks a shortlist is split into; the useful value is the
   *     read concurrency the device rewards, independent of the shortlist size. It is a target
   *     rather than a hard cap because a task never spans two files, so a shortlist drawn from many
   *     segments can yield somewhat more tasks; actual read concurrency is bounded by the executor.
   */
  public SelectiveDirectIODirectory(FSDirectory delegate, Executor readExecutor, int maxReadTasks)
      throws IOException {
    super(delegate, 4096, DEFAULT_MIN_BYTES_DIRECT);
    if (maxReadTasks < 1) {
      throw new IllegalArgumentException("maxReadTasks must be >= 1, got " + maxReadTasks);
    }
    this.readExecutor = readExecutor;
    this.maxReadTasks = maxReadTasks;
  }

  /**
   * Adds the {@code .vec} file to whatever the superclass already routes through direct I/O, so
   * search-time rerank reads bypass the page cache while merge behaviour stays as inherited.
   */
  @Override
  protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
    return name.endsWith(".vec") || super.useDirectIO(name, context, fileLength);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput in = super.openInput(name, context);
    if (name.endsWith(".vec")) {
      return new BatchInput("BatchInput(" + name + ")", in, readExecutor, maxReadTasks);
    }
    return in;
  }

  /**
   * Wraps the {@code .vec} input and adds {@link VectorBatchCapable}. Reads run through the
   * caller-owned {@link Executor}; a task clones this input once, when it runs, because a clone
   * carries its own cursor and its own aligned read buffer and so cannot be shared between threads.
   */
  private static final class BatchInput extends FilterIndexInput implements VectorBatchCapable {

    private final Executor readExecutor;
    private final int maxReadTasks;

    /** Clones parked for reuse: each carries its own cursor and its own aligned direct buffer. */
    private final ArrayBlockingQueue<IndexInput> clonePool;

    BatchInput(String resourceDescription, IndexInput in, Executor readExecutor, int maxReadTasks) {
      super(resourceDescription, in);
      this.readExecutor = readExecutor;
      this.maxReadTasks = maxReadTasks;
      this.clonePool = new ArrayBlockingQueue<>(maxReadTasks);
    }

    /**
     * A clone for one read task, allocating only when the pool is empty. Never blocks, so the pool
     * bounds retained memory without bounding read concurrency.
     */
    private IndexInput borrowClone() {
      IndexInput c = clonePool.poll();
      return c != null ? c : in.clone();
    }

    /** Parks a clone for the next task; dropped if the pool is already full. */
    private void releaseClone(IndexInput c) {
      clonePool.offer(c);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        // Clones share this input's channel and are never closable, so releasing the references is
        // the whole cleanup; their direct buffers are reclaimed once unreachable.
        clonePool.clear();
      }
    }

    @Override
    public VectorBatch newBatch() {
      return new ExecutorBatch(readExecutor, maxReadTasks);
    }

    @Override
    public IndexInput clone() {
      // Independent plain clone with its own cursor and aligned buffer, for the normal codec read
      // path and for one read task of a batch.
      return in.clone();
    }
  }

  /**
   * Gathers reads from every {@code .vec} input of this directory and runs them as one round of
   * tasks. A rerank shortlist is spread over all segments, so taking them together is what keeps
   * the device busy: one round of {@code count} reads rather than one round per segment.
   */
  private static final class ExecutorBatch implements VectorBatch {
    private final Executor readExecutor;
    private final int maxReadTasks;

    // One queued read per slot, held as parallel arrays rather than a Callable per vector: a
    // shortlist runs to hundreds of reads per query, and only the strided tasks need to be objects.
    private BatchInput[] inputs = new BatchInput[128];
    private long[] positions = new long[128];
    private float[][] outs = new float[128][];
    private int[] offsets = new int[128];
    private int[] dims = new int[128];
    private int n;

    ExecutorBatch(Executor readExecutor, int maxReadTasks) {
      this.readExecutor = readExecutor;
      this.maxReadTasks = maxReadTasks;
    }

    @Override
    public boolean add(IndexInput in, long[] positions, int dim, int count, float[] out) {
      // Negated form with a binding requires !(...): `instanceof T x == false` does not bind x.
      if (!(in instanceof BatchInput input)) {
        return false; // another directory's input: the caller reads it on its own
      }
      if (count == 0) {
        return true;
      }
      // Each array is checked on its own length: oversize() rounds to a whole number of 8-byte
      // words, so it returns a different length for int[] than for long[], and after one grow these
      // are no longer the same size.
      int needed = n + count;
      if (needed > inputs.length) {
        inputs = ArrayUtil.grow(inputs, needed);
      }
      if (needed > this.positions.length) {
        this.positions = ArrayUtil.grow(this.positions, needed);
      }
      if (needed > outs.length) {
        outs = ArrayUtil.grow(outs, needed);
      }
      if (needed > offsets.length) {
        offsets = ArrayUtil.grow(offsets, needed);
      }
      if (needed > dims.length) {
        dims = ArrayUtil.grow(dims, needed);
      }
      // Appended as one contiguous run per input; execute() partitions along those runs so that a
      // task never spans two inputs and therefore needs only one clone.
      for (int i = 0; i < count; i++) {
        inputs[n] = input;
        this.positions[n] = positions[i];
        outs[n] = out;
        offsets[n] = i * dim;
        dims[n] = dim;
        n++;
      }
      return true;
    }

    @Override
    public void execute() throws IOException {
      if (n == 0) {
        return;
      }
      try {
        // Split along the per-input runs, and no further than `target` reads, so that every task
        // reads from exactly one input (one clone) while no task becomes the critical path because
        // its segment contributed more candidates than the others. Tasks write disjoint regions of
        // their output arrays, so no synchronization is needed.
        final int reads = n;
        final int target = Math.max(1, (reads + maxReadTasks - 1) / maxReadTasks);
        List<Callable<Void>> tasks = new ArrayList<>(Math.min(reads, maxReadTasks));
        int i = 0;
        while (i < reads) {
          final int from = i;
          final BatchInput owner = inputs[i];
          int end = i;
          while (end < reads && inputs[end] == owner && end - from < target) {
            end++;
          }
          final int to = end;
          tasks.add(
              () -> {
                // One clone per task, reused across queries: allocating an aligned direct buffer
                // is too costly to do per read.
                IndexInput c = owner.borrowClone();
                try {
                  for (int k = from; k < to; k++) {
                    c.seek(positions[k]);
                    c.readFloats(outs[k], offsets[k], dims[k]);
                  }
                } finally {
                  owner.releaseClone(c);
                }
                return null;
              });
          i = to;
        }
        new TaskExecutor(readExecutor != null ? readExecutor : Runnable::run).invokeAll(tasks);
      } finally {
        Arrays.fill(inputs, 0, n, null); // do not pin inputs or caller buffers
        Arrays.fill(outs, 0, n, null);
        n = 0;
      }
    }
  }
}
