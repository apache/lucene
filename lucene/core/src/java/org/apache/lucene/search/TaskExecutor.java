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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
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

  TaskExecutor(Executor executor) {
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
    List<Task<T>> tasks = new ArrayList<>(callables.size());
    boolean runOnCallerThread = numberOfRunningTasksInCurrentThread.get() > 0;
    for (Callable<T> callable : callables) {
      Task<T> task = new Task<>(callable);
      tasks.add(task);
      if (runOnCallerThread) {
        task.run();
      } else {
        executor.execute(task);
      }
    }

    Throwable exc = null;
    final List<T> results = new ArrayList<>();
    for (Future<T> future : tasks) {
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
    if (exc != null) {
      throw IOUtils.rethrowAlways(exc);
    }
    return results;
  }

  /**
   * Extension of {@link FutureTask} that tracks the number of tasks that are running in each
   * thread.
   *
   * @param <V> the return type of the task
   */
  private static final class Task<V> extends FutureTask<V> {
    private Task(Callable<V> callable) {
      super(callable);
    }

    @Override
    public void run() {
      try {
        Integer counter = numberOfRunningTasksInCurrentThread.get();
        numberOfRunningTasksInCurrentThread.set(counter + 1);
        super.run();
      } finally {
        Integer counter = numberOfRunningTasksInCurrentThread.get();
        numberOfRunningTasksInCurrentThread.set(counter - 1);
      }
    }
  }

  @Override
  public String toString() {
    return "TaskExecutor(" + "executor=" + executor + ')';
  }
}
