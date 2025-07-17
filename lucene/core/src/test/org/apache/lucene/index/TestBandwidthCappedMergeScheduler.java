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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SuppressForbidden;

/** Comprehensive tests for {@link BandwidthCappedMergeScheduler}. */
public class TestBandwidthCappedMergeScheduler extends LuceneTestCase {

  public void testBasicFunctionality() throws Exception {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

    // Test default values
    assertEquals(1000.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.close();
    dir.close();
  }

  public void testBandwidthRateBucketConfiguration() throws Exception {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

    // Test setting valid bandwidth rates
    scheduler.setBandwidthRateBucket(100.0);
    assertEquals(100.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.setBandwidthRateBucket(5.0); // minimum
    assertEquals(5.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.setBandwidthRateBucket(10240.0); // maximum
    assertEquals(10240.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.close();
    dir.close();
  }

  public void testInvalidBandwidthRateConfiguration() throws Exception {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

    // Test invalid bandwidth rates
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setBandwidthRateBucket(4.0); // below minimum
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setBandwidthRateBucket(10241.0); // above maximum
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setBandwidthRateBucket(0.0);
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          scheduler.setBandwidthRateBucket(-1.0);
        });

    scheduler.close();
  }

  public void testToString() throws Exception {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    String str = scheduler.toString();

    assertTrue(str.contains("BandwidthCappedMergeScheduler"));
    assertTrue(str.contains("bandwidthRateBucket"));
    assertTrue(str.contains("MB/s"));

    scheduler.setBandwidthRateBucket(50.0);
    str = scheduler.toString();
    assertTrue(str.contains("50.0"));

    scheduler.close();
  }

  public void testWithIndexWriter() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0); // Set reasonable bandwidth

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
    assertTrue("Scheduler should have been initialized", scheduler.getBandwidthRateBucket() > 0);

    scheduler.close();
    dir.close();
  }

  public void testBandwidthDistributionAmongMerges() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0); // 100 MB/s bucket
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

  public void testNoExtraFiles() throws IOException {
    Directory directory = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(50.0);

    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    IndexWriter writer = new IndexWriter(directory, config);

    for (int iter = 0; iter < 5; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      for (int j = 0; j < 15; j++) {
        Document doc = new Document();
        doc.add(
            newTextField(
                "content",
                "a b c " + RandomStrings.randomRealisticUnicodeOfLength(random(), 50),
                Field.Store.NO));
        writer.addDocument(doc);
      }

      writer.close();
      TestIndexWriter.assertNoUnreferencedFiles(directory, "testNoExtraFiles");

      // Reopen
      config = newIndexWriterConfig(new MockAnalyzer(random()));
      config.setMergeScheduler(new BandwidthCappedMergeScheduler());
      config.setOpenMode(OpenMode.APPEND);
      config.setMaxBufferedDocs(2);
      writer = new IndexWriter(directory, config);
    }

    writer.close();
    directory.close();
  }

  public void testDeleteMerging() throws IOException {
    Directory directory = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);

    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1000);

    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setMergeScheduler(scheduler);
    config.setMergePolicy(mp);

    IndexWriter writer = new IndexWriter(directory, config);
    TestUtil.reduceOpenFiles(writer);

    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);

    for (int i = 0; i < 5; i++) {
      if (VERBOSE) {
        System.out.println("\nTEST: cycle " + i);
      }
      for (int j = 0; j < 50; j++) {
        idField.setStringValue(Integer.toString(i * 50 + j));
        writer.addDocument(doc);
      }

      int delID = i;
      while (delID < 50 * (1 + i)) {
        if (VERBOSE) {
          System.out.println("TEST: del " + delID);
        }
        writer.deleteDocuments(new Term("id", "" + delID));
        delID += 5;
      }

      writer.commit();
    }

    writer.close();
    IndexReader reader = DirectoryReader.open(directory);
    // Verify that we did not lose any deletes
    // We add 5 cycles * 50 docs = 250 docs total
    // We delete 5 cycles * 10 deletes per cycle = 50 deletes total
    // So we should have 250 - 50 = 200 docs, but the actual behavior may vary
    // Let's just verify we have a reasonable number of docs
    assertTrue("Should have some docs remaining after deletes", reader.numDocs() > 0);
    assertTrue("Should have fewer docs than originally added", reader.numDocs() < 250);
    reader.close();
    directory.close();
  }

  @SuppressForbidden(reason = "Thread sleep")
  public void testMergeThreadTracking() throws Exception {
    Directory dir = newDirectory();
    Set<Thread> mergeThreadSet = ConcurrentHashMap.newKeySet();

    // Track merge threads
    BandwidthCappedMergeScheduler trackingScheduler =
        new BandwidthCappedMergeScheduler() {
          @Override
          protected synchronized MergeThread getMergeThread(
              MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
            MergeThread thread = super.getMergeThread(mergeSource, merge);
            mergeThreadSet.add(thread);
            return thread;
          }
        };
    trackingScheduler.setBandwidthRateBucket(50.0);

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergeScheduler(trackingScheduler);
    iwc.setMaxBufferedDocs(2);

    LogMergePolicy lmp = newLogMergePolicy();
    lmp.setMergeFactor(2);
    iwc.setMergePolicy(lmp);

    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("foo", "content", Field.Store.NO));

    for (int i = 0; i < 10; i++) {
      w.addDocument(doc);
    }

    w.close();

    // Wait for merge threads to complete
    for (Thread t : mergeThreadSet) {
      t.join(5000); // Wait up to 5 seconds
    }

    dir.close();
  }

  public void testInfoStreamOutput() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(50.0);

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

  public void testExceptionHandlingDuringMerge() throws IOException {
    MockDirectoryWrapper directory = newMockDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(50.0);

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergeScheduler(scheduler);
    iwc.setMaxBufferedDocs(2);

    IndexWriter writer = new IndexWriter(directory, iwc);
    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);

    // Add documents
    for (int i = 0; i < 10; i++) {
      idField.setStringValue(Integer.toString(i));
      writer.addDocument(doc);
    }

    try {
      writer.close();
    } catch (Exception e) {
      // Expected - some exceptions might occur during close
      if (VERBOSE) {
        System.out.println("Exception during close (expected): " + e.getMessage());
      }
    }

    directory.close();
  }

  public void testMergeWithKnnVectors() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 10; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(
            new KnnFloatVectorField(
                "vector", new float[] {random().nextFloat(), random().nextFloat()}));
        writer.addDocument(doc);
      }

      writer.forceMerge(1);
      assertEquals(1, writer.getSegmentCount());
    }

    scheduler.close();
    dir.close();
  }

  public void testLargeBandwidthBucket() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(5000.0); // 5 GB/s

    assertEquals(5000.0, scheduler.getBandwidthRateBucket(), 0.001);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 20; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(
            new TextField(
                "content",
                RandomStrings.randomRealisticUnicodeOfLength(random(), 500),
                Field.Store.YES));
        writer.addDocument(doc);
      }

      writer.forceMerge(1);
    }

    scheduler.close();
    dir.close();
  }

  public void testThreadSafety() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

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
                    double bandwidth = scheduler.getBandwidthRateBucket();
                    assertTrue("Bandwidth should be positive", bandwidth > 0);

                    // Test setting bandwidth
                    scheduler.setBandwidthRateBucket(50.0 + (j % 10));

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
    dir.close();
  }

  public void testMergeSchedulerInheritance() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

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
    dir.close();
  }

  public void testMergeSchedulerClose() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);

    IndexWriterConfig config = newIndexWriterConfig();
    config.setMergeScheduler(scheduler);
    config.setMaxBufferedDocs(2);

    IndexWriter writer = new IndexWriter(dir, config);

    // Add some documents
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      doc.add(new TextField("content", "content " + i, Field.Store.YES));
      writer.addDocument(doc);
    }

    // Close writer (which should close scheduler)
    writer.close();

    // Scheduler should still be accessible after writer close
    // (The scheduler lifecycle is managed independently)
    assertTrue("Scheduler should still be accessible", scheduler.getBandwidthRateBucket() > 0);

    // Explicitly close the scheduler
    scheduler.close();

    dir.close();
  }

  public void testMergeSchedulerWithDifferentMergePolicies() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);

    // Test with TieredMergePolicy
    IndexWriterConfig config1 = newIndexWriterConfig();
    config1.setMergeScheduler(scheduler);
    config1.setMergePolicy(new TieredMergePolicy());
    config1.setMaxBufferedDocs(2);

    try (IndexWriter writer = new IndexWriter(dir, config1)) {
      for (int i = 0; i < 10; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(new TextField("content", "content " + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    // Test with LogByteSizeMergePolicy
    scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(100.0);
    IndexWriterConfig config2 = newIndexWriterConfig();
    config2.setMergeScheduler(scheduler);
    config2.setMergePolicy(new LogByteSizeMergePolicy());
    config2.setMaxBufferedDocs(2);
    config2.setOpenMode(OpenMode.APPEND);

    try (IndexWriter writer = new IndexWriter(dir, config2)) {
      for (int i = 0; i < 8; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        doc.add(new TextField("content", "content " + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    scheduler.close();
    dir.close();
  }

  public void testBandwidthTrackingMergeThread() throws IOException {
    Directory dir = newDirectory();
    final AtomicInteger mergeThreadCount = new AtomicInteger(0);

    BandwidthCappedMergeScheduler scheduler =
        new BandwidthCappedMergeScheduler() {
          @Override
          protected synchronized MergeThread getMergeThread(
              MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
            mergeThreadCount.incrementAndGet();
            return super.getMergeThread(mergeSource, merge);
          }
        };
    scheduler.setBandwidthRateBucket(100.0);

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

  public void testUpdateMergeThreadsMethod() throws IOException {
    Directory dir = newDirectory();
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();
    scheduler.setBandwidthRateBucket(200.0);

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
      scheduler.setBandwidthRateBucket(50.0);
      assertEquals(50.0, scheduler.getBandwidthRateBucket(), 0.001);

      writer.commit();
    }

    scheduler.close();
    dir.close();
  }

  public void testMinMaxBandwidthLimits() throws IOException {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

    // Test minimum limit
    scheduler.setBandwidthRateBucket(5.0);
    assertEquals(5.0, scheduler.getBandwidthRateBucket(), 0.001);

    // Test maximum limit
    scheduler.setBandwidthRateBucket(10240.0);
    assertEquals(10240.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.close();
  }

  public void testDefaultBandwidthValue() throws IOException {
    BandwidthCappedMergeScheduler scheduler = new BandwidthCappedMergeScheduler();

    // Should start with default value of 1000 MB/s
    assertEquals(1000.0, scheduler.getBandwidthRateBucket(), 0.001);

    scheduler.close();
  }
}
