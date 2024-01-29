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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Executor wrapper responsible for the execution of concurrent tasks. Used to parallelize search
 * across segments as well as query rewrite in some cases. Exposes a single {@link
 * #invokeAll(Collection)} method that takes a collection of {@link Callable}s and executes them
 * concurrently/ Once all tasks are submitted to the executor, it blocks and wait for all tasks to
 * be completed, and then returns a list with the obtained results. Ensures that the underlying
 * executor is only used for top-level {@link #invokeAll(Collection)} calls, and not for potential
 * {@link #invokeAll(Collection)} calls made from one of the tasks. This is to prevent deadlock with
 * certain types of pool based executors (e.g. {@link java.util.concurrent.ThreadPoolExecutor}).
 *
 * @lucene.experimental
 */
public final class TaskExecutor {
  // a static thread local is ok as long as we use a counter, which accounts for multiple
  // searchers holding a different TaskExecutor all backed by the same executor
  private static final ThreadLocal<Integer> numberOfRunningTasksInCurrentThread =
      ThreadLocal.withInitial(() -> 0);

  private final Executor executor;

  /**
   * Creates a TaskExecutor instance
   *
   * @param executor the executor to be used for running tasks concurrently
   */
  public TaskExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor, "Executor is null");
  }

  /**
   * Execute all the callables provided as an argument, wait for them to complete and return the
   * obtained results. If an exception is thrown by more than one callable, the subsequent ones will
   * be added as suppressed exceptions to the first one that was caught.
   *
   * @param callables the callables to execute
   * @return a list containing the results from the tasks execution
   * @param <T> the return type of the task execution
   */
  public <T> List<T> invokeAll(Collection<Callable<T>> callables) throws IOException {
    TaskGroup<T> taskGroup = new TaskGroup<>(callables);
    return taskGroup.invokeAll(executor);
  }

  @Override
  public String toString() {
    return "TaskExecutor(" + "executor=" + executor + ')';
  }

  /**
   * Holds all the sub-tasks that a certain operation gets split into as it gets parallelized and
   * exposes the ability to invoke such tasks and wait for them all to complete their execution and
   * provide their results. Ensures that each task does not get parallelized further: this is
   * important to avoid a deadlock in situations where one executor thread waits on other executor
   * threads to complete before it can progress. This happens in situations where for instance
   * {@link Query#createWeight(IndexSearcher, ScoreMode, float)} is called as part of searching each
   * slice, like {@link TopFieldCollector#populateScores(ScoreDoc[], IndexSearcher, Query)} does.
   * Additionally, if one task throws an exception, all other tasks from the same group are
   * cancelled, to avoid needless computation as their results would not be exposed anyways. Creates
   * one {@link FutureTask} for each {@link Callable} provided
   *
   * @param <T> the return type of all the callables
   */
  private static final class TaskGroup<T> {
    private final Collection<RunnableFuture<T>> futures;

    TaskGroup(Collection<Callable<T>> callables) {
      List<RunnableFuture<T>> tasks = new ArrayList<>(callables.size());
      for (Callable<T> callable : callables) {
        tasks.add(createTask(callable));
      }
      this.futures = Collections.unmodifiableCollection(tasks);
    }

    RunnableFuture<T> createTask(Callable<T> callable) {
      AtomicBoolean startedOrCancelled = new AtomicBoolean(false);
      return new FutureTask<>(
          () -> {
            if (startedOrCancelled.compareAndSet(false, true)) {
              try {
                Integer counter = numberOfRunningTasksInCurrentThread.get();
                numberOfRunningTasksInCurrentThread.set(counter + 1);
                return callable.call();
              } catch (Throwable t) {
                cancelAll();
                throw t;
              } finally {
                Integer counter = numberOfRunningTasksInCurrentThread.get();
                numberOfRunningTasksInCurrentThread.set(counter - 1);
              }
            }
            // task is cancelled hence it has no results to return. That's fine: they would be
            // ignored anyway.
            return null;
          }) {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          assert mayInterruptIfRunning == false
              : "cancelling tasks that are running is not supported";
          /*
          Future#get (called in invokeAll) throws CancellationException when invoked against a running task that has been cancelled but
          leaves the task running. We rather want to make sure that invokeAll does not leave any running tasks behind when it returns.
          Overriding cancel ensures that tasks that are already started will complete normally once cancelled, and Future#get will
          wait for them to finish instead of throwing CancellationException. A cleaner way would have been to override FutureTask#get and
          make it wait for cancelled tasks, but FutureTask#awaitDone is private. Tasks that are cancelled before they are started will be no-op.
           */
          return startedOrCancelled.compareAndSet(false, true);
        }
      };
    }

    List<T> invokeAll(Executor executor) throws IOException {
      boolean runOnCallerThread = numberOfRunningTasksInCurrentThread.get() > 0;
      for (Runnable runnable : futures) {
        if (runOnCallerThread) {
          runnable.run();
        } else {
          executor.execute(runnable);
        }
      }
      Throwable exc = null;
      List<T> results = new ArrayList<>(futures.size());
      for (Future<T> future : futures) {
        try {
          results.add(future.get());
        } catch (InterruptedException e) {
          var newException = new ThreadInterruptedException(e);
          if (exc == null) {
            exc = newException;
          } else {
            exc.addSuppressed(newException);
          }
        } catch (ExecutionException e) {
          if (exc == null) {
            exc = e.getCause();
          } else {
            exc.addSuppressed(e.getCause());
          }
        }
      }
      assert assertAllFuturesCompleted() : "Some tasks are still running?";
      if (exc != null) {
        throw IOUtils.rethrowAlways(exc);
      }
      return results;
    }

    private boolean assertAllFuturesCompleted() {
      for (RunnableFuture<T> future : futures) {
        if (future.isDone() == false) {
          return false;
        }
      }
      return true;
    }

    private void cancelAll() {
      for (Future<T> future : futures) {
        future.cancel(false);
      }
    }
  }
}
