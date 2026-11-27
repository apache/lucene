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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Executor wrapper responsible for the execution of concurrent tasks. Used to parallelize search
 * across segments as well as query rewrite in some cases. Exposes a single {@link
 * #invokeAll(Collection)} method that takes a collection of {@link Callable}s and executes them
 * concurrently. Once all but one task have been submitted to the executor, it tries to run as many
 * tasks as possible on the calling thread, then waits for all tasks that have been executed in
 * parallel on the executor to be completed and then returns a list with the obtained results.
 *
 * @lucene.experimental
 */
public final class TaskExecutor {
  private static final ScopedValue<TaskGroup> CURRENT_TASK_GROUP = ScopedValue.newInstance();

  private final Executor executor;

  /**
   * Creates a TaskExecutor instance
   *
   * @param executor the executor to be used for running tasks concurrently
   */
  public TaskExecutor(Executor executor) {
    Objects.requireNonNull(executor, "Executor is null");
    this.executor =
        r -> {
          try {
            executor.execute(r);
          } catch (RejectedExecutionException _) {
            // execute directly on the current thread in case of rejection to ensure a rejecting
            // executor only reduces parallelism and does not
            // result in failure
            r.run();
          }
        };
  }

  /**
   * Execute all the callables provided as an argument, wait for them to complete and return the
   * obtained results. If an exception is thrown by more than one callable, the subsequent ones will
   * be added as suppressed exceptions to the first one that was caught. Additionally, if one task
   * throws an exception, all other tasks from the same group are cancelled, to avoid needless
   * computation as their results would not be exposed anyways. Tasks that are already running are
   * not interrupted, but {@link IndexSearcher} stops searching the slices of such tasks.
   *
   * @param callables the callables to execute
   * @return a list containing the results from the tasks execution
   * @param <T> the return type of the task execution
   */
  public <T> List<T> invokeAll(Collection<Callable<T>> callables) throws IOException {
    List<RunnableFuture<T>> futures = new ArrayList<>(callables.size());
    final TaskGroup group = new TaskGroup(callables.size() > 1);
    for (Callable<T> callable : callables) {
      futures.add(new Task<>(callable, futures, group));
    }
    final int count = futures.size();
    // taskId provides the first index of an un-executed task in #futures
    final AtomicInteger taskId = new AtomicInteger(0);
    // we fork execution count - 1 tasks to execute at least one task on the current thread to
    // minimize needless forking and blocking of the current thread
    if (count > 1) {
      final Runnable work =
          () -> {
            int id = taskId.getAndIncrement();
            if (id < count) {
              futures.get(id).run();
            }
          };
      for (int j = 0; j < count - 1; j++) {
        executor.execute(work);
      }
    }
    // try to execute as many tasks as possible on the current thread to minimize context
    // switching in case of long running concurrent
    // tasks as well as dead-locking if the current thread is part of #executor for executors that
    // have limited or no parallelism
    int id;
    while ((id = taskId.getAndIncrement()) < count) {
      futures.get(id).run();
      if (id >= count - 1) {
        // save redundant CAS in case this was the last task
        break;
      }
    }
    return collectResults(futures);
  }

  private static <T> List<T> collectResults(List<RunnableFuture<T>> futures) throws IOException {
    Throwable exc = null;
    List<T> results = new ArrayList<>(futures.size());
    for (Future<T> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        exc = IOUtils.useOrSuppress(exc, new ThreadInterruptedException(e));
      } catch (ExecutionException e) {
        exc = IOUtils.useOrSuppress(exc, e.getCause());
      }
    }
    assert assertAllFuturesCompleted(futures) : "Some tasks are still running?";
    if (exc != null) {
      throw IOUtils.rethrowAlways(exc);
    }
    return results;
  }

  @Override
  public String toString() {
    return "TaskExecutor(" + "executor=" + executor + ')';
  }

  private static boolean assertAllFuturesCompleted(Collection<? extends Future<?>> futures) {
    for (Future<?> future : futures) {
      if (future.isDone() == false) {
        return false;
      }
    }
    return true;
  }

  private static <T> void cancelAll(Collection<? extends Future<T>> futures) {
    for (Future<?> future : futures) {
      future.cancel(false);
    }
  }

  /**
   * Returns the group of the {@link #invokeAll(Collection)} task that is running on the current
   * thread, or {@link TaskGroup#NONE} if the current thread is not running such a task.
   */
  static TaskGroup currentTaskGroup() {
    return CURRENT_TASK_GROUP.orElse(TaskGroup.NONE);
  }

  /**
   * The tasks of a single {@link #invokeAll(Collection)} call. Once one of them fails, the results
   * of the others are discarded, so running tasks may stop early by calling {@link
   * #ensureNotFailed()}.
   */
  static final class TaskGroup {
    /** The group of code that does not run in a {@link #invokeAll(Collection)} task. */
    static final TaskGroup NONE = new TaskGroup(false);

    private final boolean hasSiblings;
    private volatile boolean failed;

    private TaskGroup(boolean hasSiblings) {
      this.hasSiblings = hasSiblings;
    }

    /** Whether this group has more than one task, so that a task may fail while others run. */
    boolean hasSiblings() {
      return hasSiblings;
    }

    boolean isFailed() {
      return failed;
    }

    /**
     * Throws a {@link CancellationException} if a task of this group failed. When thrown by a task
     * of this group, the exception is ignored: the caller of {@link #invokeAll(Collection)} gets
     * the original failure.
     */
    void ensureNotFailed() {
      if (failed) {
        throw new CancellationException("Another task of the same group failed");
      }
    }
  }

  private static class Task<T> extends FutureTask<T> {

    private final AtomicBoolean startedOrCancelled = new AtomicBoolean(false);

    private final Collection<? extends Future<T>> futures;

    private final TaskGroup group;

    public Task(Callable<T> callable, Collection<? extends Future<T>> futures, TaskGroup group) {
      super(callable);
      this.futures = futures;
      this.group = group;
    }

    @Override
    public void run() {
      if (startedOrCancelled.compareAndSet(false, true)) {
        ScopedValue.where(CURRENT_TASK_GROUP, group).run(super::run);
      }
    }

    @Override
    protected void setException(Throwable t) {
      if (t instanceof CancellationException && group.isFailed()) {
        // the task stopped because another one failed, its result is discarded like the one of a
        // cancelled task
        set(null);
      } else {
        group.failed = true;
        super.setException(t);
        cancelAll(futures);
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      assert mayInterruptIfRunning == false : "cancelling tasks that are running is not supported";
      /*
      Future#get (called in #collectResults) throws CancellationException when invoked against a running task that has been cancelled but
      leaves the task running. We rather want to make sure that invokeAll does not leave any running tasks behind when it returns.
      Overriding cancel ensures that tasks that are already started will complete normally once cancelled, and Future#get will
      wait for them to finish instead of throwing CancellationException. A cleaner way would have been to override FutureTask#get and
      make it wait for cancelled tasks, but FutureTask#awaitDone is private. Tasks that are cancelled before they are started will be no-op.
       */
      if (startedOrCancelled.compareAndSet(false, true)) {
        // task is cancelled hence it has no results to return. That's fine: they would be
        // ignored anyway.
        set(null);
        return true;
      }
      return false;
    }
  }
}
