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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Unit tests for MultiTenantCMSManager: budgeting, dynamic rebalancing,
 * live IndexWriter integration, and auto-unregister behavior.
 */
public class TestMultiTenantCMSManager extends LuceneTestCase {

  /**
   * Subclass to count actual merges.
   */
  static class TrackingCMS extends ConcurrentMergeScheduler {
    private final AtomicInteger mergeCount = new AtomicInteger();

    @Override
    protected void doMerge(MergeScheduler.MergeSource mergeSource, MergePolicy.OneMerge merge)
        throws IOException {
      mergeCount.incrementAndGet();
      super.doMerge(mergeSource, merge);
    }

    int getMergeCount() {
      return mergeCount.get();
    }
  }

  /**
   * Test basic registration and budgeting across multiple schedulers.
   */
  public void testRegistrationAndThreadBudgeting() {
    MultiTenantCMSManager manager = MultiTenantCMSManager.getInstance();
    manager.unregisterAllForTest();

    final int numSchedulers = 4;
    ConcurrentMergeScheduler[] cmsArr = new ConcurrentMergeScheduler[numSchedulers];
    for (int i = 0; i < numSchedulers; i++) {
      cmsArr[i] = new ConcurrentMergeScheduler();
      manager.register(cmsArr[i]);
    }

    Set<ConcurrentMergeScheduler> registered = manager.getRegisteredSchedulersForTest();
    assertEquals("Unexpected registration count", numSchedulers, registered.size());

    int coreCount = Runtime.getRuntime().availableProcessors();
    int maxThreadCount = Math.max(1, coreCount / 2);
    int share = Math.max(1, maxThreadCount / numSchedulers);
    int expectedThreads = share;
    int expectedMerges = share + 5;

    for (ConcurrentMergeScheduler cms : cmsArr) {
      assertEquals("maxThreadCount mismatch", expectedThreads, cms.getMaxThreadCount());
      assertEquals("maxMergeCount mismatch", expectedMerges, cms.getMaxMergeCount());
    }

    for (ConcurrentMergeScheduler cms : cmsArr) {
      manager.unregister(cms);
    }
    assertTrue(
        "Manager should be empty after unregistering",
        manager.getRegisteredSchedulersForTest().isEmpty());
  }

  /**
   * Test that unregistering adjusts budgets dynamically.
   */
  public void testUnregisterRebalancing() {
    MultiTenantCMSManager manager = MultiTenantCMSManager.getInstance();
    manager.unregisterAllForTest();

    ConcurrentMergeScheduler a = new ConcurrentMergeScheduler();
    ConcurrentMergeScheduler b = new ConcurrentMergeScheduler();
    manager.register(a);
    manager.register(b);

    int coreCount = Runtime.getRuntime().availableProcessors();
    int maxThreadCount = Math.max(1, coreCount / 2);

    int share2 = Math.max(1, maxThreadCount / 2);
    assertEquals("Incorrect share with two schedulers", share2, a.getMaxThreadCount());
    assertEquals("Incorrect share with two schedulers", share2, b.getMaxThreadCount());

    manager.unregister(a);

    int share1 = Math.max(1, maxThreadCount);
    assertEquals("Incorrect share after unregistering one", share1, b.getMaxThreadCount());
    assertFalse(
        "Unregistered scheduler should be removed",
        manager.getRegisteredSchedulersForTest().contains(a));
  }

  /**
   * Integrate with a live IndexWriter and ensure merges occur under the manager.
   */
  public void testLiveIndexWriterIntegration() throws Exception {
    MultiTenantCMSManager manager = MultiTenantCMSManager.getInstance();
    manager.unregisterAllForTest();

    TrackingCMS cms = new TrackingCMS();
    manager.register(cms);

    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc =
        newIndexWriterConfig(analyzer).setMaxBufferedDocs(10).setMergeScheduler(cms);

    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new TextField("field", "value " + i, Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    assertTrue("Expected at least one merge", cms.getMergeCount() > 0);
    dir.close();
  }

  /**
   * Test that closing a CMS unregisters it from the manager.
   */
  public void testWrapperAutoUnregisterOnClose() throws IOException {
    MultiTenantCMSManager manager = MultiTenantCMSManager.getInstance();
    manager.unregisterAllForTest();

    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {
      @Override
      public void close() throws IOException {
        MultiTenantCMSManager.getInstance().unregister(this);
        super.close();
      }
    };
    manager.register(cms);
    assertTrue(
        "Should be registered",
        manager.getRegisteredSchedulersForTest().contains(cms));

    cms.close();
    assertFalse(
        "Should be unregistered after close",
        manager.getRegisteredSchedulersForTest().contains(cms));
  }
}
