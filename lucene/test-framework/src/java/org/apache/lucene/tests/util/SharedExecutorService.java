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
package org.apache.lucene.tests.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.lucene.util.NamedThreadFactory;

/** Lazily sets up and shuts down an {@link ExecutorService} shared by all tests in a suite. */
final class SharedExecutorService implements BeforeAfterCallback, Supplier<ExecutorService> {
  private int threadCount;
  // Just for sanity checks; we don't need full thread coordination here.
  private volatile boolean active;
  private ExecutorService executor;

  @Override
  public void before() {
    // Pick the thread count at start, for consistency.
    threadCount = TestUtil.nextInt(LuceneTestCaseParent.random(), 1, 2);
    active = true;
  }

  @Override
  public void after() {
    if (executor != null) {
      TestUtil.shutdownExecutorService(executor);
    }
    executor = null;
    active = false;
  }

  /** Returns the shared executor. Must be called within the before-after scope. */
  @Override
  public ExecutorService get() {
    synchronized (this) {
      if (!active) {
        throw new AssertionError("Shared executor is not available outside of the suite scope.");
      }

      if (executor == null) {
        executor =
            new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("LuceneTestCase"));
        // uncomment to intensify LUCENE-3840
        // executor.prestartAllCoreThreads();
      }
      return executor;
    }
  }
}
