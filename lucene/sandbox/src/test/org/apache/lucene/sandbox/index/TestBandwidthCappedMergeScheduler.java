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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.sandbox.index.BandwidthCappedMergeScheduler;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

/** Tests for {@link BandwidthCappedMergeScheduler}. */
public class TestBandwidthCappedMergeScheduler extends LuceneTestCase {

  public void testBasicFunctionality() throws Exception {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    // Test initial value
    assertEquals(100.0, scheduler.getMaxMbPerSec(), 0.001);

    scheduler.close();
    dir.close();
  }

  public void testBandwidthConfiguration() throws Exception {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(50.0);

    // Test setting valid bandwidth rates
    scheduler.setMaxMbPerSec(200.0);
    assertEquals(200.0, scheduler.getMaxMbPerSec(), 0.001);

    scheduler.setMaxMbPerSec(10.0);
    assertEquals(10.0, scheduler.getMaxMbPerSec(), 0.001);

    scheduler.close();
    dir.close();
  }

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

  public void testWithIndexWriter() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2); // Force frequent flushes

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      // Add some documents to potentially trigger merges
      for (int i = 0; i < 20; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(
            new TextField(
                "content",
                "content " + i + " " + RandomStrings.randomRealisticUnicodeOfLength(random(), 100),
                Field.Store.YES));
        writer.addDocument(doc);

        if (i % 5 == 0) {
          writer.commit(); // Force segments
        }
      }

      writer.forceMerge(1); // This should trigger merges
    }

    // The scheduler should have been used
    assertTrue("Scheduler should have been initialized", scheduler.getMaxMbPerSec() > 0);

    scheduler.close();
    dir.close();
  }

  public void testBandwidthDistributionAmongMerges() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler(100.0);
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
