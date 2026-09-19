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

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Ignores the ivfaster build pool's workers when checking for leaked threads.
 *
 * <p>WHY THESE ARE NOT LEAKS. {@link Parallel} holds ONE shared, eagerly created, process-lifetime
 * pool of daemon threads, because a split happens on nearly every build path and the alternative is
 * double-checked locking on a field each one touches. Being daemon threads they never hold JVM exit
 * open, and being a fixed pool their core workers are never reaped, so they outlive whichever suite
 * happened to trigger the first build — which is exactly what randomizedtesting reports as a
 * SUITE-scope leak.
 *
 * <p>WHY THIS IS A FILTER RATHER THAN A SHUTDOWN. The pool is deliberately shared across concurrent
 * merges and has no owner to close it; shutting it down per suite would defeat that design and
 * would not match how the codec runs in production. So the test harness is told these threads are
 * expected instead.
 *
 * <p>WHY IT IS FLAKY WITHOUT THIS. The threads are attributed to whichever suite first creates
 * them, and which suite that is depends on how the runner distributes suites across its JVMs, so
 * the failure moves between classes from run to run rather than reproducing on one.
 */
public final class IvfasterBuildThreadsFilter implements ThreadFilter {

  /** The prefix {@code Parallel}'s thread factory names its workers with. */
  private static final String PREFIX = "ivfaster-build-";

  @Override
  public boolean reject(Thread t) {
    final String name = t.getName();
    return name != null && name.startsWith(PREFIX);
  }
}
