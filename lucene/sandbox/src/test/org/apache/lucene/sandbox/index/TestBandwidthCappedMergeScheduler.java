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
package org.apache.lucene.sandbox.index;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;

/** Tests for {@link BandwidthCappedMergeScheduler}. */
public class TestBandwidthCappedMergeScheduler extends LuceneTestCase {

  public void testInvalidBandwidthConfiguration() throws Exception {
    // Test invalid constructor parameter
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new BandwidthCappedMergeScheduler(0.0);
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new BandwidthCappedMergeScheduler(-1.0);
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new BandwidthCappedMergeScheduler(Double.NaN);
        });

    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    // Test invalid setMaxMbPerSec values
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setMaxMbPerSec(0.0);
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setMaxMbPerSec(-1.0);
        });

    scheduler.close();
  }

  public void testToString() throws Exception {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(50.0);
    String str = scheduler.toString();

    assertTrue(str.contains("BandwidthCappedMergeScheduler"));
    assertTrue(str.contains("bandwidthMbPerSec"));
    assertTrue(str.contains("MB/s"));
    assertTrue(str.contains("50.0"));

    scheduler.close();
  }

  public void testBandwidthDistributionAmongMerges() throws IOException {
    Directory dir = newDirectory();

    // Track bandwidth distribution calls and the rates applied
    AtomicInteger updateCalls = new AtomicInteger(0);
    List<Double> appliedRates = Collections.synchronizedList(new ArrayList<>());

    BandwidthCappedMergeScheduler scheduler =
        new BandwidthCappedMergeScheduler(120.0) {
          @Override
          protected synchronized void updateMergeThreads() {
            super.updateMergeThreads();
            updateCalls.incrementAndGet();

            // Capture the rates being applied to active merge threads
            for (ConcurrentMergeScheduler.MergeThread mergeThread : mergeThreads) {
              if (mergeThread.isAlive()) {
                double rate = mergeThread.getRateLimiter().getMBPerSec();
                appliedRates.add(rate);
              }
            }
          }
        };

    scheduler.setMaxMergesAndThreads(3, 2); // Allow multiple concurrent merges

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    // Use a merge policy that creates more merges
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMergeFactor(2);
    config.setMergePolicy(mp);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      // Add many documents to trigger multiple merges
      for (int i = 0; i < 50; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(
            new TextField(
                "content",
                RandomStrings.randomRealisticUnicodeOfLength(random(), 200),
                Field.Store.YES));
        writer.addDocument(doc);

        if (i % 10 == 0) {
          writer.commit();
        }
      }

      writer.forceMerge(1);
    }

    // Verify bandwidth distribution was invoked
    assertTrue(
        "Bandwidth distribution should have been called during merge operations",
        updateCalls.get() > 0);

    // Verify that rate limits were applied to merge threads
    assertFalse("Rate limits should have been applied to merge threads", appliedRates.isEmpty());

    // Verify that applied rates respect the minimum threshold (0.1 MB/s)
    for (Double rate : appliedRates) {
      assertTrue("Applied rate should be at least 0.1 MB/s, got: " + rate, rate >= 0.1);
      assertTrue(
          "Applied rate should not exceed total bandwidth (120.0 MB/s), got: " + rate,
          rate <= 120.0);
    }

    // Verify scheduler maintains its configuration
    assertEquals("Max merge count should be 3", 3, scheduler.getMaxMergeCount());
    assertEquals("Max thread count should be 2", 2, scheduler.getMaxThreadCount());

    // Verify auto IO throttle remains disabled (core functionality)
    assertFalse(
        "Auto IO throttle should be disabled for bandwidth-capped scheduler",
        scheduler.getAutoIOThrottle());

    scheduler.close();
    dir.close();
  }

  public void testAutoIOThrottleDisabled() throws IOException {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    // Auto IO throttle should always be disabled
    assertFalse("Auto IO throttle should be disabled", scheduler.getAutoIOThrottle());

    // Enabling should be ignored
    scheduler.enableAutoIOThrottle();
    assertFalse("Auto IO throttle should still be disabled", scheduler.getAutoIOThrottle());

    // Disabling should work
    scheduler.disableAutoIOThrottle();
    assertFalse("Auto IO throttle should remain disabled", scheduler.getAutoIOThrottle());

    scheduler.close();
  }

  public void testInfoStreamOutput() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(50.0);

    List<String> messages = Collections.synchronizedList(new ArrayList<>());
    InfoStream infoStream =
        new InfoStream() {
          @Override
          public void close() {}

          @Override
          public void message(String component, String message) {
            if (component.equals("MS")) {
              messages.add(message);
            }
          }

          @Override
          public boolean isEnabled(String component) {
            return component.equals("MS");
          }
        };

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setInfoStream(infoStream);
    config.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 8; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(new TextField("content", "content " + i, Field.Store.YES));
        writer.addDocument(doc);
      }

      writer.commit();
    }

    // Should have some merge-related messages
    assertFalse("Should have some info stream messages", messages.isEmpty());

    scheduler.close();
    dir.close();
  }

  public void testThreadSafety() throws IOException, InterruptedException {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    final AtomicBoolean failed = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(3);

    // Create multiple threads that access bandwidth methods concurrently
    for (int i = 0; i < 3; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  startLatch.await();

                  for (int j = 0; j < 100; j++) {
                    double bandwidth = scheduler.getMaxMbPerSec();
                    assertTrue("Bandwidth should be positive", bandwidth > 0);

                    // Test setting bandwidth
                    scheduler.setMaxMbPerSec(50.0 + (j % 10));

                    Thread.yield();
                  }
                } catch (Exception e) {
                  failed.set(true);
                  e.printStackTrace();
                } finally {
                  doneLatch.countDown();
                }
              });
      t.start();
    }

    startLatch.countDown();
    assertTrue("Threads should complete within timeout", doneLatch.await(10, TimeUnit.SECONDS));
    assertFalse("No thread should have failed", failed.get());

    scheduler.close();
  }

  public void testMergeSchedulerInheritance() throws IOException {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    // Test that it properly inherits from ConcurrentMergeScheduler
    assertTrue(
        "Should be instance of ConcurrentMergeScheduler",
        scheduler instanceof ConcurrentMergeScheduler);
    assertTrue("Should be instance of MergeScheduler", scheduler instanceof MergeScheduler);

    // Test inherited methods work
    scheduler.setMaxMergesAndThreads(2, 1);
    assertEquals(2, scheduler.getMaxMergeCount());
    assertEquals(1, scheduler.getMaxThreadCount());

    scheduler.close();
  }

  public void testBandwidthTrackingMergeThread() throws IOException {
    Directory dir = newDirectory();
    final AtomicInteger mergeThreadCount = new AtomicInteger(0);

    BandwidthCappedMergeScheduler scheduler =
        new BandwidthCappedMergeScheduler(100.0) {
          @Override
          protected synchronized MergeThread getMergeThread(
              MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
            mergeThreadCount.incrementAndGet();
            return super.getMergeThread(mergeSource, merge);
          }
        };

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 10; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(
            new TextField(
                "content",
                RandomStrings.randomRealisticUnicodeOfLength(random(), 100),
                Field.Store.YES));
        writer.addDocument(doc);
      }

      writer.forceMerge(1);
    }

    // Should have created at least one merge thread
    assertTrue("Should have created merge threads", mergeThreadCount.get() > 0);

    scheduler.close();
    dir.close();
  }

  public void testLiveBandwidthUpdate() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(200.0);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      // Add documents to trigger merges
      for (int i = 0; i < 15; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(new TextField("content", "content " + i, Field.Store.YES));
        writer.addDocument(doc);
      }

      // Change bandwidth rate during operation
      scheduler.setMaxMbPerSec(50.0);
      assertEquals(50.0, scheduler.getMaxMbPerSec(), 0.001);

      writer.commit();
    }

    scheduler.close();
    dir.close();
  }
}
