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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Tests NRT readers opened while a commit is in progress, that is, after the commit started but
 * before its segments_N file is written.
 *
 * <p>Checks two things:
 *
 * <ul>
 *   <li>If a reader says it is {@link DirectoryReader#isCurrent() current}, it points at the latest
 *       commit, not at an older segments_N that may already be deleted.
 *   <li>Reopening a reader never returns a different reader with the same {@link
 *       DirectoryReader#getVersion() version}. {@link SearcherLifetimeManager} relies on this.
 * </ul>
 */
public class TestNRTReaderDuringCommit extends LuceneTestCase {

  /**
   * Pauses, and optionally fails, the file sync of the next commit. IndexWriter runs this sync
   * without holding its lock, so the test can open readers meanwhile.
   */
  private static final class BlockingSyncDirectory extends FilterDirectory {
    private final AtomicBoolean armed = new AtomicBoolean();
    private volatile boolean failSync;
    private volatile boolean syncBlocked;
    private volatile CountDownLatch syncStarted;
    private volatile CountDownLatch releaseSync;

    BlockingSyncDirectory(Directory in) {
      super(in);
    }

    void blockNextCommitSync(boolean fail) {
      blockNextCommitSync(fail, new CountDownLatch(1));
    }

    /**
     * Same as {@link #blockNextCommitSync(boolean)}, and counts down {@code syncStarted} once the
     * sync is paused.
     */
    void blockNextCommitSync(boolean fail, CountDownLatch syncStarted) {
      failSync = fail;
      this.syncStarted = syncStarted;
      releaseSync = new CountDownLatch(1);
      armed.set(true);
    }

    /** Don't pause the next sync, if it hasn't started yet. */
    void disarm() {
      armed.set(false);
    }

    boolean isSyncBlocked() {
      return syncBlocked;
    }

    boolean willFailSync() {
      return failSync;
    }

    void awaitSyncStarted() throws InterruptedException {
      assertTrue("commit never reached its file sync", syncStarted.await(30, TimeUnit.SECONDS));
    }

    void releaseSync() {
      releaseSync.countDown();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
      // pending_segments_N is synced separately, under the IndexWriter lock: don't block that one
      if (names.stream().noneMatch(n -> n.startsWith(IndexFileNames.PENDING_SEGMENTS))
          && armed.compareAndSet(true, false)) {
        syncBlocked = true;
        syncStarted.countDown();
        try {
          assertTrue(releaseSync.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        } finally {
          syncBlocked = false;
        }
        if (failSync) {
          throw new IOException("simulated sync failure");
        }
      }
      super.sync(names);
    }
  }

  /** Runs {@link IndexWriter#commit()} on another thread. */
  private static final class BackgroundCommit {
    private final Thread thread;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    BackgroundCommit(IndexWriter writer) {
      this(writer, new CountDownLatch(1));
    }

    /** Counts down {@code done} once the commit completed or failed. */
    BackgroundCommit(IndexWriter writer, CountDownLatch done) {
      thread =
          new Thread(
              () -> {
                try {
                  writer.commit();
                } catch (Throwable t) {
                  failure.set(t);
                } finally {
                  done.countDown();
                }
              });
      thread.start();
    }

    /** Waits for the commit and returns what it threw, if anything. */
    Throwable join() throws InterruptedException {
      thread.join();
      return failure.get();
    }
  }

  private static Document doc(String id, long value) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.NO));
    doc.add(new NumericDocValuesField("dv", value));
    return doc;
  }

  private static long dvValue(DirectoryReader reader, String id) throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      PostingsEnum postings = leaf.postings(new Term("id", id));
      if (postings == null) {
        continue;
      }
      for (int doc = postings.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = postings.nextDoc()) {
        if (leaf.getLiveDocs() == null || leaf.getLiveDocs().get(doc)) {
          NumericDocValues dv = leaf.getNumericDocValues("dv");
          assertTrue(dv.advanceExact(doc));
          return dv.longValue();
        }
      }
    }
    throw new AssertionError("no live doc with id=" + id);
  }

  private static String latestCommit(Directory dir) throws IOException {
    return SegmentInfos.readLatestCommit(dir).getSegmentsFileName();
  }

  private static List<String> segmentNames(DirectoryReader reader) {
    List<String> names = new ArrayList<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      names.add(((SegmentReader) ctx.reader()).getSegmentName());
    }
    return names;
  }

  /**
   * Returns a writer that never merges. Merges change the index, so they would make most tests here
   * depend on timing.
   */
  private IndexWriter newWriter(Directory dir) throws IOException {
    return new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE));
  }

  /**
   * Checks that {@code reader}, opened before the latest commit finished, is not current, and that
   * reopening it gives a reader with a higher version, which points at the latest commit and is
   * current. Closes {@code reader} and returns the new reader.
   */
  private static DirectoryReader assertStaleAndReopen(
      DirectoryReader reader, IndexWriter writer, Directory dir, int expectedNumDocs)
      throws IOException {
    String lastCommit = latestCommit(dir);
    assertNotEquals(lastCommit, reader.getIndexCommit().getSegmentsFileName());
    assertFalse(reader.isCurrent());

    DirectoryReader newReader =
        random().nextBoolean()
            ? DirectoryReader.openIfChanged(reader)
            : DirectoryReader.openIfChanged(reader, writer);
    assertNotNull(newReader);
    assertTrue(
        "reopened reader must have a higher version: old="
            + reader.getVersion()
            + " new="
            + newReader.getVersion(),
        newReader.getVersion() > reader.getVersion());
    assertEquals(lastCommit, newReader.getIndexCommit().getSegmentsFileName());
    assertEquals(expectedNumDocs, newReader.numDocs());
    assertTrue(newReader.isCurrent());
    assertNull(DirectoryReader.openIfChanged(newReader));
    reader.close();
    return newReader;
  }

  /** A reader opened during a commit's file sync must not be current once the commit finishes. */
  public void testReaderOpenedDuringCommitSync() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    writer.addDocument(doc("1", 1));

    dir.blockNextCommitSync(false);
    BackgroundCommit commit = new BackgroundCommit(writer);
    try {
      dir.awaitSyncStarted();
      // the commit's changes are visible, but its segments_N is not written yet
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
      assertEquals(2, reader.numDocs());
    } finally {
      dir.releaseSync();
      assertNull(commit.join());
    }

    reader = assertStaleAndReopen(reader, writer, dir, 2);
    IOUtils.close(reader, writer, dir);
  }

  /**
   * Same as {@link #testReaderOpenedDuringCommitSync}, but the reader is opened between {@link
   * IndexWriter#prepareCommit()} and {@link IndexWriter#commit()}.
   */
  public void testReaderOpenedBetweenPrepareCommitAndCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    writer.addDocument(doc("1", 1));

    writer.prepareCommit();
    DirectoryReader reader = DirectoryReader.open(writer);
    assertEquals(2, reader.numDocs());
    writer.commit();

    reader = assertStaleAndReopen(reader, writer, dir, 2);
    IOUtils.close(reader, writer, dir);
  }

  /**
   * Same as {@link #testReaderOpenedDuringCommitSync}, but the index also changes during the sync,
   * before the reader is opened, so the reader's version is higher than the commit's.
   */
  public void testReaderOpenedDuringCommitSyncAfterConcurrentChange() throws Exception {
    for (String change : new String[] {"add", "delete", "dvUpdate"}) {
      BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
      IndexWriter writer = newWriter(dir);
      writer.addDocument(doc("0", 0));
      writer.addDocument(doc("1", 1));
      writer.commit();
      DirectoryReader reader = DirectoryReader.open(writer);
      writer.addDocument(doc("2", 2));

      dir.blockNextCommitSync(false);
      BackgroundCommit commit = new BackgroundCommit(writer);
      int expectedNumDocs = 3;
      try {
        dir.awaitSyncStarted();
        switch (change) {
          case "add" -> {
            writer.addDocument(doc("3", 3));
            expectedNumDocs++;
          }
          case "delete" -> {
            writer.deleteDocuments(new Term("id", "0"));
            expectedNumDocs--;
          }
          case "dvUpdate" -> writer.updateNumericDocValue(new Term("id", "1"), "dv", 42);
          default -> throw new AssertionError(change);
        }
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer);
        assertNotNull(change, newReader);
        reader.close();
        reader = newReader;
        assertEquals(change, expectedNumDocs, reader.numDocs());
      } finally {
        dir.releaseSync();
        assertNull(change, commit.join());
      }

      reader = assertStaleAndReopen(reader, writer, dir, expectedNumDocs);
      if (change.equals("dvUpdate")) {
        assertEquals(42, dvValue(reader, "1"));
      }
      IOUtils.close(reader, writer, dir);
    }
  }

  /**
   * If the commit's file sync fails, a reader opened during it stays current, because the latest
   * commit didn't change. Once a later commit succeeds, it is not current anymore.
   */
  public void testReaderOpenedDuringFailedCommitSync() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    String firstCommit = latestCommit(dir);
    DirectoryReader reader = DirectoryReader.open(writer);
    writer.addDocument(doc("1", 1));

    dir.blockNextCommitSync(true);
    BackgroundCommit commit = new BackgroundCommit(writer);
    try {
      dir.awaitSyncStarted();
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
    } finally {
      dir.releaseSync();
      Throwable failure = commit.join();
      assertTrue(String.valueOf(failure), failure instanceof IOException);
    }

    assertEquals(firstCommit, latestCommit(dir));
    assertEquals(firstCommit, reader.getIndexCommit().getSegmentsFileName());
    assertTrue(reader.isCurrent());
    assertNull(DirectoryReader.openIfChanged(reader, writer));

    writer.commit();
    reader = assertStaleAndReopen(reader, writer, dir, 2);
    IOUtils.close(reader, writer, dir);
  }

  /**
   * A searcher opened during a commit and one opened after it must have different versions, or
   * {@link SearcherLifetimeManager#record} rejects the second one. TestSearcherManager sometimes
   * hits this by chance; this test hits it every time.
   */
  public void testSearcherLifetimeManagerWithRefreshDuringCommit() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    SearcherManager searcherManager = new SearcherManager(writer, null);
    SearcherLifetimeManager lifetimeManager = new SearcherLifetimeManager();
    writer.addDocument(doc("1", 1));

    dir.blockNextCommitSync(false);
    BackgroundCommit commit = new BackgroundCommit(writer);
    IndexSearcher duringCommit;
    long tokenDuringCommit;
    try {
      dir.awaitSyncStarted();
      assertTrue(searcherManager.maybeRefresh());
      duringCommit = searcherManager.acquire();
      tokenDuringCommit = lifetimeManager.record(duringCommit);
      searcherManager.release(duringCommit);
    } finally {
      dir.releaseSync();
      assertNull(commit.join());
    }

    assertTrue(searcherManager.maybeRefresh());
    IndexSearcher afterCommit = searcherManager.acquire();
    try {
      assertNotSame(duringCommit, afterCommit);
      assertEquals(
          latestCommit(dir),
          ((DirectoryReader) afterCommit.getIndexReader()).getIndexCommit().getSegmentsFileName());
      long tokenAfterCommit = lifetimeManager.record(afterCommit);
      assertNotEquals(tokenDuringCommit, tokenAfterCommit);
    } finally {
      searcherManager.release(afterCommit);
    }
    IOUtils.close(lifetimeManager, searcherManager, writer, dir);
  }

  /**
   * A commit that changes nothing must not make readers look out of date. Also, a reader opened
   * right after a commit has the commit's version, which {@link DirectoryReader#isCurrent()} relies
   * on once the writer is closed.
   */
  public void testNoSpuriousChanges() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    assertEquals(SegmentInfos.readLatestCommit(dir).getVersion(), reader.getVersion());

    // a commit changes which commit the reader should point at, so the reader is out of date
    writer.addDocument(doc("1", 1));
    DirectoryReader changed = DirectoryReader.openIfChanged(reader);
    assertNotNull(changed);
    reader.close();
    reader = changed;
    writer.commit();
    reader = assertStaleAndReopen(reader, writer, dir, 2);
    assertEquals(SegmentInfos.readLatestCommit(dir).getVersion(), reader.getVersion());

    // a commit that changes nothing doesn't
    writer.commit();
    assertTrue(reader.isCurrent());
    assertNull(DirectoryReader.openIfChanged(reader));

    // still true after a reader was opened during an earlier commit
    writer.addDocument(doc("2", 2));
    dir.blockNextCommitSync(false);
    BackgroundCommit commit = new BackgroundCommit(writer);
    try {
      dir.awaitSyncStarted();
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
    } finally {
      dir.releaseSync();
      assertNull(commit.join());
    }
    reader = assertStaleAndReopen(reader, writer, dir, 3);
    writer.commit();
    assertTrue(reader.isCurrent());
    assertNull(DirectoryReader.openIfChanged(reader));

    IOUtils.close(reader, writer, dir);
  }

  /**
   * If a reader was opened during a commit, the version is bumped when the commit finishes, so
   * readers opened afterwards have a higher version than the commit. Once the writer is closed,
   * {@link DirectoryReader#isCurrent()} compares the reader's version with the latest commit's, so
   * it reports such a reader as not current even though it is. That's safe: its data is correct,
   * and a new reader opened from the directory has the same commit and segments.
   */
  public void testReaderAfterCommitWithReaderOpenedDuringCommitOnceWriterIsClosed()
      throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    writer.addDocument(doc("1", 1));

    dir.blockNextCommitSync(false);
    BackgroundCommit commit = new BackgroundCommit(writer);
    try {
      dir.awaitSyncStarted();
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
    } finally {
      dir.releaseSync();
      assertNull(commit.join());
    }
    reader = assertStaleAndReopen(reader, writer, dir, 2);
    String lastCommit = latestCommit(dir);
    assertTrue(reader.getVersion() > SegmentInfos.readLatestCommit(dir).getVersion());

    writer.close();
    assertEquals("closing without changes must not commit", lastCommit, latestCommit(dir));
    assertFalse(reader.isCurrent());

    DirectoryReader fromCommit = DirectoryReader.open(dir);
    assertEquals(lastCommit, fromCommit.getIndexCommit().getSegmentsFileName());
    assertEquals(lastCommit, reader.getIndexCommit().getSegmentsFileName());
    assertEquals(segmentNames(reader), segmentNames(fromCommit));
    assertEquals(reader.numDocs(), fromCommit.numDocs());
    assertTrue(fromCommit.isCurrent());

    IOUtils.close(fromCommit, reader, dir);
  }

  /**
   * A reader opened during a commit must not be current once the commit finished and the writer is
   * closed either. Then {@link DirectoryReader#isCurrent()} checks the reader against the latest
   * commit on disk instead of asking the writer. Covers both a reader opened during the commit's
   * file sync and one opened between {@link IndexWriter#prepareCommit()} and {@link
   * IndexWriter#commit()}.
   */
  public void testReaderOpenedDuringCommitOnceWriterIsClosed() throws Exception {
    for (boolean twoPhaseCommit : new boolean[] {false, true}) {
      BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
      IndexWriter writer = newWriter(dir);
      writer.addDocument(doc("0", 0));
      writer.commit();
      DirectoryReader reader = DirectoryReader.open(writer);
      writer.addDocument(doc("1", 1));

      if (twoPhaseCommit) {
        writer.prepareCommit();
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        assertNotNull(newReader);
        reader.close();
        reader = newReader;
        writer.commit();
      } else {
        dir.blockNextCommitSync(false);
        BackgroundCommit commit = new BackgroundCommit(writer);
        try {
          dir.awaitSyncStarted();
          DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
          assertNotNull(newReader);
          reader.close();
          reader = newReader;
        } finally {
          dir.releaseSync();
          assertNull(commit.join());
        }
      }
      writer.close();

      String lastCommit = latestCommit(dir);
      assertNotEquals(lastCommit, reader.getIndexCommit().getSegmentsFileName());
      assertFalse(
          "reader on "
              + reader.getIndexCommit().getSegmentsFileName()
              + " must not be current, the latest commit is "
              + lastCommit
              + " (twoPhaseCommit="
              + twoPhaseCommit
              + ")",
          reader.isCurrent());

      DirectoryReader fromCommit = DirectoryReader.open(dir);
      assertEquals(lastCommit, fromCommit.getIndexCommit().getSegmentsFileName());
      assertEquals(2, fromCommit.numDocs());
      assertTrue(fromCommit.isCurrent());
      IOUtils.close(fromCommit, reader, dir);
    }
  }

  /**
   * Randomly mixes changes, reopens and commits. The test thread pauses each commit (in its file
   * sync, or between prepareCommit and commit), and sometimes makes it fail. Checks that reopened
   * readers have higher versions, and that when no commit is in progress, a current reader points
   * at the latest commit.
   */
  public void testRandomCommitsAndReopens() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    // randomized config, including merges on commit and on getReader:
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);

    BackgroundCommit syncingCommit = null; // blocked in its file sync
    boolean prepared = false; // prepareCommit was called, but not commit yet
    int reopensDuringCommit = 0;
    try {
      int numSteps = atLeast(300);
      for (int step = 0; step < numSteps; step++) {
        boolean commitInProgress = syncingCommit != null || prepared;
        int action = random().nextInt(10);
        if (action < 3) {
          String id = Integer.toString(random().nextInt(20));
          switch (random().nextInt(4)) {
            case 0 -> writer.deleteDocuments(new Term("id", id));
            case 1 -> writer.updateNumericDocValue(new Term("id", id), "dv", random().nextLong());
            default -> writer.updateDocument(new Term("id", id), doc(id, random().nextLong()));
          }
        } else if (action < 8) {
          // reopening without a change covers readers that only differ by the commit they point at
          DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
          if (newReader != null) {
            assertTrue(
                "reopened reader must have a higher version: old="
                    + reader.getVersion()
                    + " new="
                    + newReader.getVersion(),
                newReader.getVersion() > reader.getVersion());
            reader.close();
            reader = newReader;
            if (commitInProgress) {
              reopensDuringCommit++;
            }
          }
          if (commitInProgress == false && reader.isCurrent()) {
            assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
          }
        } else if (commitInProgress == false) {
          if (random().nextInt(4) == 0) {
            writer.prepareCommit();
            prepared = true;
          } else {
            // returns once the commit is blocked in its file sync, or done if it had nothing to
            // sync
            CountDownLatch blockedOrDone = new CountDownLatch(1);
            dir.blockNextCommitSync(random().nextInt(5) == 0, blockedOrDone);
            BackgroundCommit commit = new BackgroundCommit(writer, blockedOrDone);
            assertTrue(blockedOrDone.await(30, TimeUnit.SECONDS));
            if (dir.isSyncBlocked()) {
              syncingCommit = commit;
            } else {
              dir.disarm();
              assertNull(commit.join());
            }
          }
        } else if (prepared) {
          writer.commit();
          prepared = false;
        } else {
          boolean fail = dir.willFailSync();
          dir.releaseSync();
          Throwable failure = syncingCommit.join();
          syncingCommit = null;
          if (fail) {
            assertTrue(String.valueOf(failure), failure instanceof IOException);
          } else {
            assertNull(failure);
          }
        }
      }
    } finally {
      dir.disarm();
      if (syncingCommit != null) {
        dir.releaseSync();
        syncingCommit.join();
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: " + reopensDuringCommit + " reopens during a commit");
    }

    // wait for merges, so that nothing changes anymore after the final commit
    writer.forceMerge(1);
    writer.commit();
    for (DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        newReader != null;
        newReader = DirectoryReader.openIfChanged(reader)) {
      assertTrue(newReader.getVersion() > reader.getVersion());
      reader.close();
      reader = newReader;
    }
    assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
    assertTrue(reader.isCurrent());
    IOUtils.close(reader, writer, dir);
  }
}
