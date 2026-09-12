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
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ParallelVectorReadable;

/**
 * Example {@link DirectIODirectory} that applies {@code O_DIRECT} only to the raw float32 vector
 * file ({@code .vec}), so that larger-than-RAM rerank reads bypass the OS page cache while the HNSW
 * graph ({@code .vex}), quantized codes and metadata keep using the mmap delegate and stay
 * page-cached. Combined with 4KB-aligned vector data (see {@code Lucene99FlatVectorsWriter}), each
 * vector read is a single aligned block with no read amplification.
 *
 * <p>The {@code .vec} input is a {@link ParallelVectorReadable}: when a read {@link Executor} is
 * supplied, the KNN full-precision rerank path fetches a candidate shortlist as one concurrent
 * batch (see {@link BatchInput#readVectors}). The executor is owned by the caller (created and shut
 * down by them, like {@link org.apache.lucene.search.IndexSearcher}'s executor); this directory
 * only references it. With no executor the shortlist is read serially on the calling thread. Sizing
 * the read pool independently of the searcher's executor lets a larger-than-RAM deployment keep
 * enough reads in flight to saturate the device without widening CPU-side search parallelism.
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
   * @param maxReadTasks upper bound on the number of tasks a shortlist is split into; the useful
   *     value is the read concurrency the device rewards, independent of the shortlist size
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
   * Wraps the {@code .vec} input and adds {@link ParallelVectorReadable}. Reads run through the
   * caller-owned {@link Executor}; each task reuses one {@link IndexInput#clone()} per thread so an
   * aligned read buffer is not reallocated per query.
   */
  private static final class BatchInput extends FilterIndexInput implements ParallelVectorReadable {

    private final Executor readExecutor;
    private final int maxReadTasks;
    private final ThreadLocal<IndexInput> threadClone;

    BatchInput(String resourceDescription, IndexInput in, Executor readExecutor, int maxReadTasks) {
      super(resourceDescription, in);
      this.readExecutor = readExecutor;
      this.maxReadTasks = maxReadTasks;
      this.threadClone = ThreadLocal.withInitial(in::clone);
    }

    @Override
    public void readVectors(long[] positions, int dim, int count, float[] out) throws IOException {
      if (count == 0) {
        return;
      }
      int nTasks = Math.min(count, maxReadTasks);
      List<Callable<Void>> tasks = new ArrayList<>(nTasks);
      for (int t = 0; t < nTasks; t++) {
        final int start = t;
        final int step = nTasks;
        tasks.add(
            () -> {
              IndexInput c = threadClone.get();
              // Strided so every task reads a similar number of vectors; tasks write disjoint
              // regions of out, so no synchronization is needed.
              for (int i = start; i < count; i += step) {
                c.seek(positions[i]);
                c.readFloats(out, i * dim, dim);
              }
              return null;
            });
      }
      new TaskExecutor(readExecutor != null ? readExecutor : Runnable::run).invokeAll(tasks);
    }

    @Override
    public IndexInput clone() {
      // Independent plain clone for the normal codec read path (no shared cursor).
      return in.clone();
    }
  }
}
