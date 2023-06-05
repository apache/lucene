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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Executor which is responsible for execution of slices based on the current status of the system
 * and current system load
 */
public class SliceExecutor {

  /** Thresholds for index slice allocation logic */
  private static final int MAX_DOCS_PER_SLICE = 250_000;

  private static final int MAX_SEGMENTS_PER_SLICE = 5;

  private final Executor executor;

  public SliceExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor, "Executor is null");
  }

  /**
   * method to segregate LeafReaderContexts amongst multiple slices using the default
   * MAX_SEGMENTS_PER_SLICE and MAX_DOCUMENTS_PER_SLICE
   *
   * @param leaves LeafReaderContexts for this index
   * @return computed slices
   */
  public LeafSlice[] computeSlices(List<LeafReaderContext> leaves) {
    return IndexSearcher.slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
  }

  public void invokeAll(Collection<? extends Runnable> tasks) {
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

  public Executor getExecutor() {
    return executor;
  }
}
