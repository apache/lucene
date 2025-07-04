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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Smoke-tests MultiTenantCMSManager’s register/unregister + budget math */
public class TestMultiTenantCMSManager {

  private MultiTenantCMSManager mgr;

  /** A dummy CMS that just remembers the last values passed to setMaxMergesAndThreads(...) */
  static class DummyCMS extends ConcurrentMergeScheduler {
    volatile int lastMaxMerges;
    volatile int lastMaxThreads;

    @Override
    public void setMaxMergesAndThreads(int maxMerges, int maxThreads) {
      super.setMaxMergesAndThreads(maxMerges, maxThreads);
      this.lastMaxMerges = maxMerges;
      this.lastMaxThreads = maxThreads;
    }
  }

  @Before
  public void setUp() {
    mgr = MultiTenantCMSManager.getInstance();
    mgr.unregisterAllForTest();
  }

  @After
  public void tearDown() {
    mgr.unregisterAllForTest();
  }

  @Test
  public void testSingleSchedulerGetsFullBudget() {
    DummyCMS cms = new DummyCMS();
    mgr.register(cms);

    // exactly one registered
    Set<ConcurrentMergeScheduler> regs = mgr.getRegisteredSchedulersForTest();
    assertEquals("should register exactly one", 1, regs.size());

    // compute what the manager should have done
    int cores = Runtime.getRuntime().availableProcessors();
    int maxThreadCount = Math.max(1, cores / 2);
    int share = Math.max(1, maxThreadCount / 1);
    int merges = share + 5;

    assertEquals("maxThreads == share", share, cms.lastMaxThreads);
    assertEquals("maxMerges == share + 5", merges, cms.lastMaxMerges);
  }

  @Test
  public void testTwoSchedulersSplitBudgetEqually() {
    DummyCMS cms1 = new DummyCMS();
    DummyCMS cms2 = new DummyCMS();

    mgr.register(cms1);
    mgr.register(cms2);

    Set<ConcurrentMergeScheduler> regs = mgr.getRegisteredSchedulersForTest();
    assertEquals("should register two", 2, regs.size());

    int cores = Runtime.getRuntime().availableProcessors();
    int maxThreadCount = Math.max(1, cores / 2);
    int share = Math.max(1, maxThreadCount / 2);
    int merges = share + 5;

    // both should get the same share
    assertEquals(share, cms1.lastMaxThreads);
    assertEquals(merges, cms1.lastMaxMerges);

    assertEquals(share, cms2.lastMaxThreads);
    assertEquals(merges, cms2.lastMaxMerges);
  }

  @Test
  public void testUnregisterClearsRegistry() {
    DummyCMS cms = new DummyCMS();
    mgr.register(cms);
    assertFalse("registry should not be empty", mgr.getRegisteredSchedulersForTest().isEmpty());

    mgr.unregister(cms);
    assertTrue(
        "registry should be empty after unregister",
        mgr.getRegisteredSchedulersForTest().isEmpty());
  }
}
