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
package org.apache.lucene.index;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple demonstration of a MultiTenantCMSManager that divides a fixed thread budget equally among
 * all registered ConcurrentMergeSchedulers.
 */
public class MultiTenantCMSManager {

  private static final MultiTenantCMSManager INSTANCE = new MultiTenantCMSManager();
  private static final int coreCount = Runtime.getRuntime().availableProcessors();
  private static final int maxThreadCount = Math.max(1, coreCount / 2);

  private final Set<ConcurrentMergeScheduler> schedulers =
      Collections.synchronizedSet(new HashSet<>());

  private MultiTenantCMSManager() {}

  public static MultiTenantCMSManager getInstance() {
    return INSTANCE;
  }

  public void register(ConcurrentMergeScheduler cms) {
    schedulers.add(cms);
    updateBudgets();
  }

  public void unregister(ConcurrentMergeScheduler cms) {
    schedulers.remove(cms);
    updateBudgets();
  }

  private void updateBudgets() {
    int count = schedulers.size();
    if (count == 0) return;

    int share = Math.max(1, maxThreadCount / count);
    for (ConcurrentMergeScheduler cms : schedulers) {
      cms.setMaxMergesAndThreads(share + 5, share); // +5 to allow merge queuing
    }
  }

  // -----------------------------------------
  // 🧪 TESTING HOOKS
  // -----------------------------------------

  /** Used in tests to read the current registered CMS set */
  synchronized Set<ConcurrentMergeScheduler> getRegisteredSchedulersForTest() {
    return new HashSet<>(schedulers);
  }

  /** Used in tests to clear all registered CMS instances */
  synchronized void unregisterAllForTest() {
    schedulers.clear();
  }
}
