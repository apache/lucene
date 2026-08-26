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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.TaskExecutor;

/**
 * Range-parallel helpers for the build path.
 *
 * <p>Indexing is embarrassingly parallel over documents, and the routing scan dominates build cost.
 *
 * <h2>A bounded shared pool</h2>
 *
 * <p>{@link TaskExecutor} forks all but one task and then runs as many as it can ON THE CALLING
 * THREAD, so a saturated or zero-parallelism executor costs parallelism and never liveness. That is
 * what makes a pool safe underneath a caller-supplied indexing thread, and it puts the merge thread
 * to work instead of leaving it blocked in {@code join()}.
 *
 * <p>The pool is shared and bounded because build concurrency MULTIPLIES with the merge
 * scheduler's: {@code ConcurrentMergeScheduler} runs several merges at once and each merge calls
 * these helpers many times, so a thread per core per call oversubscribes the box by that product.
 * One worker set for the whole JVM is the property the scheduler already assumes it has.
 *
 * <p>Not injectable. Lucene's convention is a {@code numMergeWorkers} plus {@code ExecutorService}
 * pair on the format ({@code Lucene99HnswVectorsFormat}), which is the right shape if a caller ever
 * needs to own the pool; it is not needed to bound concurrency.
 *
 * @lucene.experimental
 */
final class Parallel {

  /**
   * Default minimum items per worker. Below this, coordination dominates the split. It is also why
   * small segments run single-threaded and stay deterministic.
   *
   * <p>Calibrated for a PER-DOCUMENT item, such as a routing step or a code encode. A stage whose
   * item is orders of magnitude more expensive passes its own grain to {@link #overRange(int, int,
   * RangeTask)}; see the graph build, where one item is a whole HNSW insertion.
   */
  private static final int MIN_PER_THREAD = 4096;

  /**
   * Diagnostic: report each split's chosen width and wall time ({@code -Divfaster.buildTrace}).
   *
   * <p>The split width is derived rather than configured, from {@code count / MIN_PER_THREAD}, so a
   * stage whose count is a cell count rather than a document count can resolve to one thread and
   * still read as parallel at the call site.
   */
  static final boolean TRACE = Boolean.getBoolean("ivfaster.buildTrace");

  /**
   * Worker count for the shared pool, and therefore the widest any single split can be.
   *
   * <p>Overridable ({@code -Divfaster.buildThreads}). The pool is shared across concurrent merges,
   * so this is JVM-wide build concurrency.
   */
  static final int WORKERS =
      Math.max(
          1,
          Integer.getInteger("ivfaster.buildThreads", Runtime.getRuntime().availableProcessors()));

  /**
   * The shared worker set. Daemon threads, so it never holds JVM exit open, since this class has no
   * lifecycle hook to shut a pool down from.
   *
   * <p>Created eagerly, since the alternative is double-checked locking on a field every split
   * touches, to save {@link #WORKERS} idle threads in a JVM that indexes nothing.
   */
  private static final TaskExecutor EXEC =
      new TaskExecutor(
          Executors.newFixedThreadPool(
              WORKERS,
              new ThreadFactory() {
                private final AtomicInteger n = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                  final Thread t = new Thread(r, "ivfaster-build-" + n.getAndIncrement());
                  t.setDaemon(true);
                  return t;
                }
              }));

  private Parallel() {}

  /** A unit of work over the half-open item range {@code [from, to)}. */
  interface RangeTask {
    void run(int from, int to) throws IOException;
  }

  private static void trace(String what, int count, int threads, long startNs) {
    IvfDiag.err(
        "[ivfaster-par] %-14s count=%-9d threads=%-3d %.3f s%n",
        what, count, threads, (System.nanoTime() - startNs) / 1e9);
  }

  /**
   * Runs {@code body} over disjoint contiguous ranges of {@code [0, count)}.
   *
   * <p>Ranges are disjoint, so a body that writes only to indices in its own range needs no
   * synchronization. That is the intended usage; anything shared must be handled by the caller.
   */
  static void overRange(int count, RangeTask body) throws IOException {
    overRange(count, MIN_PER_THREAD, body);
  }

  /**
   * As {@link #overRange(int, RangeTask)}, but with an explicit minimum items per worker.
   *
   * <p>For stages whose ITEM is much more expensive than a per-document step, where the default
   * grain would resolve the whole stage to one thread. The grain is a property of the body, so it
   * belongs at the call site.
   *
   * <p>ONE RANGE PER TASK, rather than a finer slicing for load balancing: several bodies allocate
   * per-task scratch proportional to {@code nlist} (the graph build takes an {@code int[nlist]} and
   * a {@code boolean[nlist]}), so over-slicing multiplies that allocation, and equal ranges are
   * already equal-cost.
   */
  static void overRange(int count, int minPerThread, RangeTask body) throws IOException {
    final long t0 = TRACE ? System.nanoTime() : 0L;
    final int tasks = Math.min(WORKERS, Math.max(1, count / minPerThread));
    if (tasks <= 1) {
      body.run(0, count);
      if (TRACE) {
        trace("overRange", count, 1, t0);
      }
      return;
    }
    final int chunk = (count + tasks - 1) / tasks;
    final List<Callable<Void>> work = new ArrayList<>(tasks);
    for (int t = 0; t < tasks; t++) {
      final int from = t * chunk;
      final int to = Math.min(count, from + chunk);
      if (from >= to) {
        continue;
      }
      work.add(
          () -> {
            body.run(from, to);
            return null;
          });
    }
    EXEC.invokeAll(work);
    if (TRACE) {
      trace("overRange", count, work.size(), t0);
    }
  }

  /**
   * Runs {@code body} over disjoint ranges of CELL ids, split so each worker gets roughly the same
   * number of assigned DOCUMENTS rather than the same number of cells.
   *
   * <p>Cell populations are heavily skewed, so an equal-cells split leaves one worker holding a
   * mega-cell while the rest idle. {@code cellStart} is the counting-sort prefix array, so cell
   * {@code c}'s population is {@code cellStart[c+1] - cellStart[c]} and a balanced split is a walk
   * over the prefix sums.
   *
   * <p>Because ranges are disjoint in CELL space, each worker owns its cells outright and can write
   * shared per-cell accumulators without synchronization. That is what makes the Lloyd mean update
   * possible without per-thread {@code nlist x dim} partials, which would not fit at high {@code
   * nlist}.
   */
  static void overCells(int nlist, int count, int[] cellStart, RangeTask body) throws IOException {
    final long t0 = TRACE ? System.nanoTime() : 0L;
    final int tasks = Math.min(WORKERS, Math.max(1, count / MIN_PER_THREAD));
    if (tasks <= 1 || nlist <= 1) {
      body.run(0, nlist);
      if (TRACE) {
        trace("overCells", count, 1, t0);
      }
      return;
    }
    // Split points from the prefix sums: each worker's cells hold about count/tasks documents.
    final int[] bounds = new int[tasks + 1];
    bounds[tasks] = nlist;
    int worker = 1;
    for (int c = 0; c < nlist && worker < tasks; c++) {
      if ((long) cellStart[c + 1] * tasks >= (long) count * worker) {
        bounds[worker++] = c + 1;
      }
    }
    // If skew left workers unassigned, give them empty ranges rather than duplicating cells.
    while (worker < tasks) {
      bounds[worker++] = nlist;
    }

    final List<Callable<Void>> work = new ArrayList<>(tasks);
    for (int t = 0; t < tasks; t++) {
      final int from = bounds[t];
      final int to = bounds[t + 1];
      if (from >= to) {
        continue;
      }
      work.add(
          () -> {
            body.run(from, to);
            return null;
          });
    }
    EXEC.invokeAll(work);
    if (TRACE) {
      trace("overCells", count, work.size(), t0);
    }
  }
}
