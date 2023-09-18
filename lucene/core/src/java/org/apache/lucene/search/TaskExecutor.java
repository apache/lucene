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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Executor wrapper responsible for the execution of concurrent tasks. Used to parallelize search
 * across segments as well as query rewrite in some cases.
 */
class TaskExecutor {
  private final Executor executor;

  TaskExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor, "Executor is null");
  }

  /**
   * Execute all the tasks provided as an argument, wait for them to complete and return the
   * obtained results.
   *
   * @param tasks the tasks to execute
   * @return a list containing the results from the tasks execution
   * @param <T> the return type of the task execution
   */
  final <T> List<T> invokeAll(Collection<RunnableFuture<T>> tasks) throws IOException {
    for (Runnable task : tasks) {
      executor.execute(task);
    }
    final List<T> results = new ArrayList<>();
    for (Future<T> future : tasks) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        throw IOUtils.rethrowAlways(e.getCause());
      }
    }
    return results;
  }
}
