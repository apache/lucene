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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.ThreadInterruptedException;

/** Tests merged segment warming on {@link IndexWriterConfig#setMergedSegmentWarmerExecutor}. */
public class TestIndexWriterAsyncWarmer extends LuceneTestCase {

  private ExecutorService executor;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // bounded, as documented
    executor =
        new ThreadPoolExecutor(
            1,
            2,
            1,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4),
            new NamedThreadFactory("TestIndexWriterAsyncWarmer"),
            new ThreadPoolExecutor.AbortPolicy());
  }

  @Override
  public void tearDown() throws Exception {
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    super.tearDown();
  }

  private static MergeScheduler newScheduler(boolean concurrent) {
    return concurrent ? new ConcurrentMergeScheduler() : new SerialMergeScheduler();
  }

  private static LogDocMergePolicy newMergePolicy(int mergeFactor) {
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMergeFactor(mergeFactor);
    mp.setTargetSearchConcurrency(1);
    return mp;
  }

  private IndexWriterConfig newConfig(boolean concurrent, IndexReaderWarmer warmer) {
    return newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(2)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setReaderPooling(true)
        .setMergeScheduler(newScheduler(concurrent))
        .setMergePolicy(newMergePolicy(2))
        .setMergedSegmentWarmerExecutor(executor)
        .setMergedSegmentWarmer(warmer);
  }

  private static void addDocs(IndexWriter w, int count) throws IOException {
    Document doc = new Document();
    doc.add(newStringField("foo", "bar", Field.Store.NO));
    for (int i = 0; i < count; i++) {
      w.addDocument(doc);
    }
  }

  @SuppressForbidden(reason = "Thread sleep")
  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }

  /** Warmer that blocks until the merge is aborted, as seen through {@code abortCheck}. */
  private static class BlockingWarmer implements IndexReaderWarmer {
    final CountDownLatch started = new CountDownLatch(1);
    final AtomicBoolean abortSeen = new AtomicBoolean();

    @Override
    public void warm(LeafReader reader) {
      throw new AssertionError("the abortCheck variant must be used");
    }

    @Override
    public void warm(LeafReader reader, IORunnable abortCheck) throws IOException {
      started.countDown();
      try {
        while (true) {
          abortCheck.run();
          sleep(5);
        }
      } catch (MergePolicy.MergeAbortedException e) {
        abortSeen.set(true);
        throw e;
      }
    }
  }

  /** Counts the merges that were published. */
  private static class RecordingWriter extends IndexWriter {
    final AtomicInteger publishedMerges = new AtomicInteger();

    RecordingWriter(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    @Override
    protected void mergeSuccess(MergePolicy.OneMerge merge) {
      // not called for merges that failed or were aborted
      publishedMerges.incrementAndGet();
    }
  }

  public void testAsyncWarmingConcurrentScheduler() throws Exception {
    doTestAsyncWarming(true);
  }

  public void testAsyncWarmingSerialScheduler() throws Exception {
    doTestAsyncWarming(false);
  }

  private void doTestAsyncWarming(boolean concurrent) throws Exception {
    AtomicInteger warmed = new AtomicInteger();
    AtomicInteger warmedOffMergeThread = new AtomicInteger();
    Thread testThread = Thread.currentThread();
    IndexReaderWarmer warmer =
        reader -> {
          sleep(5);
          assertEquals(
              reader.maxDoc(), newSearcher(reader).count(new TermQuery(new Term("foo", "bar"))));
          warmed.incrementAndGet();
          Thread t = Thread.currentThread();
          if (t != testThread && (t instanceof ConcurrentMergeScheduler.MergeThread) == false) {
            warmedOffMergeThread.incrementAndGet();
          }
        };
    try (Directory dir = newDirectory()) {
      IndexWriter w = new IndexWriter(dir, newConfig(concurrent, warmer));
      addDocs(w, 20);
      w.waitForMerges();
      assertTrue(warmed.get() > 0);
      assertEquals(warmed.get(), warmedOffMergeThread.get());
      assertNull(w.getTragicException());
      w.close();
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(20, r.numDocs());
        assertTrue(r.leaves().size() < 10);
      }
    }
  }

  public void testAsyncWarmingFailureIsTragic() throws Exception {
    IndexReaderWarmer warmer =
        _ -> {
          throw new IOException("warm failed");
        };
    try (Directory dir = newDirectory()) {
      // The warming may fail before merge() checks whether it completed, in which case the
      // exception is also thrown to the (concurrent) merge scheduler
      ConcurrentMergeScheduler scheduler =
          new ConcurrentMergeScheduler() {
            @Override
            protected void handleMergeException(Throwable exc) {}
          };
      IndexWriter w =
          new IndexWriter(
              dir, newConfig(random().nextBoolean(), warmer).setMergeScheduler(scheduler));
      try {
        addDocs(w, 4);
        w.waitForMerges();
        // the tragic event is recorded right after the merge is removed from the running merges
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (w.getTragicException() == null && System.nanoTime() < deadline) {
          sleep(10);
        }
        assertNotNull(w.getTragicException());
      } finally {
        w.rollback();
      }
    }
  }

  public void testRejectedExecutorWarmsSynchronously() throws Exception {
    AtomicInteger warmed = new AtomicInteger();
    AtomicInteger rejected = new AtomicInteger();
    Executor rejecting =
        _ -> {
          rejected.incrementAndGet();
          throw new RejectedExecutionException();
        };
    try (Directory dir = newDirectory()) {
      IndexWriter w =
          new IndexWriter(
              dir,
              newConfig(random().nextBoolean(), _ -> warmed.incrementAndGet())
                  .setMergedSegmentWarmerExecutor(rejecting));
      addDocs(w, 20);
      w.waitForMerges();
      assertTrue(rejected.get() > 0);
      assertEquals(rejected.get(), warmed.get());
      assertNull(w.getTragicException());
      w.close();
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(20, r.numDocs());
      }
    }
  }

  public void testBrokenExecutorDoesNotLeakReaders() throws Exception {
    AtomicInteger warmed = new AtomicInteger();
    Executor broken =
        _ -> {
          throw new IllegalStateException("broken executor");
        };
    try (Directory dir = newDirectory()) {
      ConcurrentMergeScheduler scheduler =
          new ConcurrentMergeScheduler() {
            @Override
            protected void handleMergeException(Throwable exc) {}
          };
      IndexWriter w =
          new IndexWriter(
              dir,
              newConfig(true, _ -> warmed.incrementAndGet())
                  .setMergeScheduler(scheduler)
                  .setMergedSegmentWarmerExecutor(broken));
      try {
        addDocs(w, 4);
        w.waitForMerges();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (w.getTragicException() == null && System.nanoTime() < deadline) {
          sleep(10);
        }
        assertTrue(w.getTragicException() instanceof IllegalStateException);
        assertEquals(0, warmed.get());
      } finally {
        // MockDirectoryWrapper checks on close that no reader of the merged segment is left open
        w.rollback();
      }
    }
  }

  public void testRollbackAbortsWarming() throws Exception {
    BlockingWarmer warmer = new BlockingWarmer();
    try (Directory dir = newDirectory()) {
      RecordingWriter w = new RecordingWriter(dir, newConfig(random().nextBoolean(), warmer));
      addDocs(w, 4);
      assertTrue(warmer.started.await(30, TimeUnit.SECONDS));
      // the merge is not published while it is warming
      assertEquals(2, w.getSegmentCount());
      w.rollback();
      assertTrue(warmer.abortSeen.get());
      assertEquals(0, w.publishedMerges.get());
      assertNull(w.getTragicException());
    }
  }

  public void testCloseWithoutCommitAbortsWarming() throws Exception {
    BlockingWarmer warmer = new BlockingWarmer();
    try (Directory dir = newDirectory()) {
      RecordingWriter w =
          new RecordingWriter(
              dir, newConfig(random().nextBoolean(), warmer).setCommitOnClose(false));
      addDocs(w, 4);
      assertTrue(warmer.started.await(30, TimeUnit.SECONDS));
      assertEquals(2, w.getSegmentCount());
      w.close();
      assertTrue(warmer.abortSeen.get());
      assertEquals(0, w.publishedMerges.get());
    }
  }

  public void testCloseWaitsForWarmingAndPublishesMerge() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean warmed = new AtomicBoolean();
    IndexReaderWarmer warmer =
        _ -> {
          started.countDown();
          sleep(300);
          warmed.set(true);
        };
    try (Directory dir = newDirectory()) {
      IndexWriter w = new IndexWriter(dir, newConfig(random().nextBoolean(), warmer));
      addDocs(w, 4);
      assertTrue(started.await(30, TimeUnit.SECONDS));
      w.close();
      // close() did not abort the merge: it waited for it, and published it
      assertTrue(warmed.get());
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(4, r.numDocs());
        assertEquals(1, r.leaves().size());
      }
    }
  }

  public void testCascadingMergesAreScheduledAfterAsyncPublication() throws Exception {
    for (boolean concurrent : new boolean[] {true, false}) {
      try (Directory dir = newDirectory()) {
        IndexWriter w =
            new IndexWriter(
                dir, newConfig(concurrent, _ -> sleep(30)).setMergePolicy(newMergePolicy(2)));
        addDocs(w, 16);
        // No more indexing, flush or commit: the only thing that can trigger the merges that
        // become possible once merges are published is the publication itself. Don't call
        // waitForMerges, which would trigger them.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (w.getSegmentCount() > 1 && System.nanoTime() < deadline) {
          sleep(10);
        }
        assertEquals("concurrent=" + concurrent, 1, w.getSegmentCount());
        assertNull(w.getTragicException());
        w.close();
      }
    }
  }
}
