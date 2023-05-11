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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Executor which is responsible for execution of slices based on the current status of the system
 * and current system load
 */
class SliceExecutor {
  private final Executor executor;

  SliceExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor, "Executor is null");
  }

  final void invokeAll(Collection<? extends Runnable> tasks) {
    int i = 0;
    for (Runnable task : tasks) {
      if (shouldExecuteOnCallerThread(i, tasks.size())) {
        task.run();
      } else {
        try {
          executor.execute(task);
        } catch (
            @SuppressWarnings("unused")
            RejectedExecutionException e) {
          task.run();
        }
      }
      ++i;
    }
  }

  boolean shouldExecuteOnCallerThread(int index, int numTasks) {
    // Execute last task on caller thread
    return index == numTasks - 1;
  }
}
