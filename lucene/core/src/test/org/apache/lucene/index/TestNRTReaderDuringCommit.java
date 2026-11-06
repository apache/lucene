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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Tests NRT readers opened while a commit is in progress, that is, after the commit started but
 * before it wrote its segments_N file.
 *
 * <p>Checks that:
 *
 * <ul>
 *   <li>A reader that says it is {@link DirectoryReader#isCurrent() current} uses the latest
 *       commit, not an older segments_N file that may already be deleted.
 *   <li>Reopening a reader never returns a different reader with the same {@link
 *       DirectoryReader#getVersion() version}. {@link SearcherLifetimeManager} relies on this.
 *   <li>After a writer is closed normally (not rolled back), the next writer on the same index
 *       never gives a reader a version that an earlier reader had, with different documents.
 * </ul>
 */
public class TestNRTReaderDuringCommit extends LuceneTestCase {

  /**
   * Pauses, and optionally fails, the file sync of the next commit. IndexWriter runs this sync
   * without holding its lock, so the test can open readers meanwhile.
   */
  static final class BlockingSyncDirectory extends FilterDirectory {
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
  static final class BackgroundCommit {
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

  static Document doc(String id, long value) {
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

  static String latestCommit(Directory dir) throws IOException {
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
   * Describes the documents a reader sees: the id and doc value of each live document, sorted by
   * id. Doesn't depend on how the documents are split into segments, because a reader can see a
   * merge without getting a new version (for example, a merge when opening a reader, see {@link
   * IndexWriterConfig#setMaxFullFlushMergeWaitMillis}).
   */
  static String contents(DirectoryReader reader) throws IOException {
    TreeMap<String, Long> docs = new TreeMap<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      Bits liveDocs = leaf.getLiveDocs();
      String[] ids = new String[leaf.maxDoc()];
      Terms terms = leaf.terms("id");
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum postings = null;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
          postings = termsEnum.postings(postings, PostingsEnum.NONE);
          for (int doc = postings.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = postings.nextDoc()) {
            ids[doc] = term.utf8ToString();
          }
        }
      }
      NumericDocValues dv = leaf.getNumericDocValues("dv");
      for (int doc = 0; doc < leaf.maxDoc(); doc++) {
        if (liveDocs == null || liveDocs.get(doc)) {
          assertTrue(dv.advanceExact(doc));
          assertNull("duplicate id " + ids[doc], docs.put(ids[doc], dv.longValue()));
        }
      }
    }
    return docs.toString();
  }

  /**
   * Records which contents {@code reader} has for its version, and fails if a reader with the same
   * version but different contents was seen before.
   */
  private static void checkVersionNotReused(Map<Long, String> seen, DirectoryReader reader)
      throws IOException {
    String contents = contents(reader);
    String previous = seen.putIfAbsent(reader.getVersion(), contents);
    if (previous != null && previous.equals(contents) == false) {
      throw new AssertionError(
          "version "
              + reader.getVersion()
              + " was reused for different contents: before="
              + previous
              + " now="
              + contents);
    }
  }

  /**
   * Opens a reader while a commit of the pending changes is in progress, either during the commit's
   * file sync or between {@link IndexWriter#prepareCommit()} and {@link IndexWriter#commit()}, and
   * then lets the commit finish. Closes {@code reader} and returns the reader opened during the
   * commit.
   */
  private static DirectoryReader reopenDuringCommit(
      BlockingSyncDirectory dir, IndexWriter writer, DirectoryReader reader, boolean twoPhaseCommit)
      throws Exception {
    DirectoryReader newReader;
    if (twoPhaseCommit) {
      writer.prepareCommit();
      newReader = DirectoryReader.openIfChanged(reader);
      writer.commit();
    } else {
      dir.blockNextCommitSync(false);
      BackgroundCommit commit = new BackgroundCommit(writer);
      try {
        dir.awaitSyncStarted();
        newReader = DirectoryReader.openIfChanged(reader);
      } finally {
        dir.releaseSync();
        assertNull(commit.join());
      }
    }
    assertNotNull(newReader);
    reader.close();
    return newReader;
  }

  /**
   * Returns a reader of {@code writer} whose version is higher than the latest commit's, because
   * another reader was opened during that commit. Makes no changes after that commit.
   */
  private static DirectoryReader readerAheadOfLatestCommit(
      BlockingSyncDirectory dir, IndexWriter writer, boolean twoPhaseCommit) throws Exception {
    writer.addDocument(doc("0", 0));
    writer.addDocument(doc("1", 1));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    writer.addDocument(doc("2", 2));
    reader = reopenDuringCommit(dir, writer, reader, twoPhaseCommit);
    reader = assertStaleAndReopen(reader, writer, dir, 3);
    assertTrue(reader.getVersion() > SegmentInfos.readLatestCommit(dir).getVersion());
    return reader;
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
   * If a reader was opened during a commit, the writer increases the version when the commit
   * finishes. This doesn't count as an uncommitted change, so code that commits whenever there are
   * uncommitted changes doesn't keep committing an index that doesn't change. But if a reader got
   * the new version, closing the writer commits, so the next writer doesn't reuse that version. If
   * no reader got it, closing the writer doesn't commit.
   */
  public void testCloseCommitsBumpedVersionOnlyIfAReaderGotIt() throws Exception {
    for (boolean readerGotBumpedVersion : new boolean[] {false, true}) {
      BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
      IndexWriter writer = newWriter(dir);
      writer.addDocument(doc("0", 0));
      writer.commit();
      DirectoryReader reader = DirectoryReader.open(writer);
      writer.addDocument(doc("1", 1));
      reader = reopenDuringCommit(dir, writer, reader, random().nextBoolean());
      assertFalse(writer.hasUncommittedChanges());
      // a commit without changes still doesn't commit anything
      String lastCommit = latestCommit(dir);
      writer.commit();
      assertEquals(lastCommit, latestCommit(dir));
      if (readerGotBumpedVersion) {
        reader = assertStaleAndReopen(reader, writer, dir, 2);
        assertTrue(reader.getVersion() > SegmentInfos.readLatestCommit(dir).getVersion());
        assertFalse(writer.hasUncommittedChanges());
      }

      writer.close();
      SegmentInfos latest = SegmentInfos.readLatestCommit(dir);
      if (readerGotBumpedVersion) {
        assertNotEquals(lastCommit, latest.getSegmentsFileName());
        assertTrue(latest.getVersion() > reader.getVersion());
      } else {
        assertEquals("closing without changes must not commit", lastCommit, latestCommit(dir));
      }
      assertFalse(reader.isCurrent());

      DirectoryReader fromCommit = DirectoryReader.open(dir);
      assertEquals(latest.getSegmentsFileName(), fromCommit.getIndexCommit().getSegmentsFileName());
      assertEquals(segmentNames(reader), segmentNames(fromCommit));
      assertEquals(contents(reader), contents(fromCommit));
      assertTrue(fromCommit.isCurrent());
      IOUtils.close(fromCommit, reader, dir);
    }
  }

  /**
   * Once the writer is closed, {@link DirectoryReader#isCurrent()} compares the reader with the
   * latest commit on disk instead of asking the writer. A reader opened during a commit must not be
   * current then either. Covers both a reader opened during the commit's file sync and one opened
   * between {@link IndexWriter#prepareCommit()} and {@link IndexWriter#commit()}.
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
   * A writer can give a reader a version that is higher than the latest commit's, if another reader
   * was opened during that commit. Closing the writer without other changes must still save that
   * version. Otherwise the next writer starts from the latest commit's version, and its first
   * change could give a reader the same version as the earlier writer's reader, with different
   * documents.
   */
  public void testNextWriterDoesNotReuseVersion() throws Exception {
    for (boolean twoPhaseCommit : new boolean[] {false, true}) {
      for (boolean commitBeforeClose : new boolean[] {false, true}) {
        for (String change :
            new String[] {"add", "delete", "dvUpdate", "deleteAll", "addIndexes"}) {
          String context =
              "twoPhaseCommit="
                  + twoPhaseCommit
                  + " commitBeforeClose="
                  + commitBeforeClose
                  + " change="
                  + change;
          BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
          IndexWriter writer = newWriter(dir);
          DirectoryReader first = readerAheadOfLatestCommit(dir, writer, twoPhaseCommit);
          if (commitBeforeClose) {
            writer.commit();
          }
          writer.close();

          writer = newWriter(dir);
          switch (change) {
            case "add" -> writer.addDocument(doc("3", 3));
            case "delete" -> writer.deleteDocuments(new Term("id", "0"));
            case "dvUpdate" -> writer.updateNumericDocValue(new Term("id", "1"), "dv", 42);
            case "deleteAll" -> writer.deleteAll();
            case "addIndexes" -> {
              try (Directory other = newDirectory()) {
                try (IndexWriter otherWriter = newWriter(other)) {
                  otherWriter.addDocument(doc("3", 3));
                }
                writer.addIndexes(other);
              }
            }
            default -> throw new AssertionError(change);
          }
          DirectoryReader second = DirectoryReader.open(writer);
          assertNotEquals(context, contents(first), contents(second));
          assertTrue(
              context
                  + ": the next writer's reader must have a higher version: first="
                  + first.getVersion()
                  + " second="
                  + second.getVersion(),
              second.getVersion() > first.getVersion());
          IOUtils.close(first, second, writer, dir);
        }
      }
    }
  }

  /**
   * Same as {@link #testNextWriterDoesNotReuseVersion}, but the next writer opens the index with
   * {@link OpenMode#CREATE}, which empties the index and increases the version only once.
   */
  public void testNextWriterInCreateModeDoesNotReuseVersion() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    IndexWriter writer = newWriter(dir);
    DirectoryReader first = readerAheadOfLatestCommit(dir, writer, random().nextBoolean());
    writer.close();

    writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(OpenMode.CREATE));
    DirectoryReader second = DirectoryReader.open(writer);
    assertEquals(0, second.numDocs());
    assertTrue(
        "the next writer's reader must have a higher version: first="
            + first.getVersion()
            + " second="
            + second.getVersion(),
        second.getVersion() > first.getVersion());
    IOUtils.close(first, second, writer, dir);
  }

  /**
   * After a writer is closed normally, the next writer's first reader sees the same contents as the
   * earlier writer's last reader, so it must not have a lower version: versions shouldn't go back
   * unless changes were rolled back or lost.
   */
  public void testNextWriterDoesNotGoBackInVersion() throws Exception {
    // the next writer's reader is either opened directly, or by reopening the earlier writer's
    // reader with the next writer, which always returns a new reader
    for (boolean reopenWithNextWriter : new boolean[] {false, true}) {
      BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
      IndexWriter writer = newWriter(dir);
      DirectoryReader first = readerAheadOfLatestCommit(dir, writer, random().nextBoolean());
      writer.close();

      writer = newWriter(dir);
      DirectoryReader second =
          reopenWithNextWriter
              ? DirectoryReader.openIfChanged(first, writer)
              : DirectoryReader.open(writer);
      assertNotNull(second);
      assertEquals(contents(first), contents(second));
      assertTrue(
          "the next writer's reader must not have a lower version: first="
              + first.getVersion()
              + " second="
              + second.getVersion()
              + " reopenWithNextWriter="
              + reopenWithNextWriter,
          second.getVersion() >= first.getVersion());
      IOUtils.close(first, second, writer, dir);
    }
  }

  /**
   * On the given trigger (a commit or opening a reader), merges all segments that aren't already
   * being merged, so the commit or the new reader waits for that merge.
   */
  static final class MergeOnTriggerPolicy extends FilterMergePolicy {
    private final MergeTrigger trigger;

    MergeOnTriggerPolicy(MergePolicy in, MergeTrigger trigger) {
      super(in);
      this.trigger = trigger;
    }

    @Override
    public MergeSpecification findFullFlushMerges(
        MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
        throws IOException {
      if (mergeTrigger != trigger) {
        return super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
      }
      List<SegmentCommitInfo> toMerge = new ArrayList<>();
      for (SegmentCommitInfo info : segmentInfos) {
        if (mergeContext.getMergingSegments().contains(info) == false) {
          toMerge.add(info);
        }
      }
      if (toMerge.size() < 2) {
        return null;
      }
      MergeSpecification spec = new MergeSpecification();
      spec.add(new OneMerge(toMerge));
      return spec;
    }
  }

  /**
   * With {@link IndexWriterConfig#setMaxFullFlushMergeWaitMillis}, a commit waits for merges
   * without holding the writer's lock, after it picked what to commit and before it writes any
   * segments_N file. A reader opened while it waits must not be current once the commit finished,
   * whether it was opened before or after the merge finished.
   */
  public void testReaderOpenedWhileCommitWaitsForMerges() throws Exception {
    for (boolean afterMerge : new boolean[] {false, true}) {
      CountDownLatch paused = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      IndexWriterEventListener listener =
          new IndexWriterEventListener() {
            @Override
            public void beginMergeOnFullFlush(MergePolicy.MergeSpecification merge) {
              if (afterMerge == false) {
                pause();
              }
            }

            @Override
            public void endMergeOnFullFlush(MergePolicy.MergeSpecification merge) {
              if (afterMerge) {
                pause();
              }
            }

            private void pause() {
              paused.countDown();
              try {
                assertTrue(resume.await(30, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                throw new ThreadInterruptedException(e);
              }
            }
          };
      Directory dir = newDirectory();
      IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig(new MockAnalyzer(random()))
                  .setMergePolicy(
                      new MergeOnTriggerPolicy(NoMergePolicy.INSTANCE, MergeTrigger.COMMIT))
                  .setMergeScheduler(new ConcurrentMergeScheduler())
                  .setMaxFullFlushMergeWaitMillis(30_000)
                  .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                  .setRAMBufferSizeMB(100)
                  .setIndexWriterEventListener(listener));
      writer.addDocument(doc("0", 0));
      writer.commit();
      DirectoryReader reader = DirectoryReader.open(writer);
      writer.addDocument(doc("1", 1));
      writer.flush();
      writer.addDocument(doc("2", 2));

      BackgroundCommit commit = new BackgroundCommit(writer);
      try {
        assertTrue(paused.await(30, TimeUnit.SECONDS));
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        assertNotNull(newReader);
        reader.close();
        reader = newReader;
        assertEquals(3, reader.numDocs());
        // after the merge, the writer and so the reader have the merged segment:
        assertEquals("afterMerge=" + afterMerge, afterMerge ? 1 : 3, reader.leaves().size());
      } finally {
        resume.countDown();
        assertNull(commit.join());
      }
      // the commit includes the merge:
      assertEquals(1, SegmentInfos.readLatestCommit(dir).size());

      reader = assertStaleAndReopen(reader, writer, dir, 3);
      IOUtils.close(reader, writer, dir);
    }
  }

  /**
   * A commit whose only change is new commit user data still writes a new segments_N file, so a
   * reader opened during it must not be current afterwards, and reopening it gives the same
   * documents with a higher version. Setting commit user data without increasing the version
   * doesn't make a reader out of date, because NRT readers don't see it.
   */
  public void testReaderOpenedDuringCommitOfOnlyCommitData() throws Exception {
    for (boolean doIncrementVersion : new boolean[] {false, true}) {
      BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
      IndexWriter writer = newWriter(dir);
      writer.addDocument(doc("0", 0));
      writer.commit();
      DirectoryReader reader = DirectoryReader.open(writer);
      String docs = contents(reader);

      writer.setLiveCommitData(Map.of("key", "value").entrySet(), doIncrementVersion);
      assertEquals(doIncrementVersion == false, reader.isCurrent());
      if (doIncrementVersion) {
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        assertNotNull(newReader);
        reader.close();
        reader = newReader;
      }
      // the commit itself increments the version, so the reader opened during it is new:
      reader = reopenDuringCommit(dir, writer, reader, random().nextBoolean());
      assertEquals("value", SegmentInfos.readLatestCommit(dir).getUserData().get("key"));
      reader = assertStaleAndReopen(reader, writer, dir, 1);
      assertEquals(docs, contents(reader));
      IOUtils.close(reader, writer, dir);
    }
  }

  /**
   * Once a writer is closed, a new writer can be opened on the commit of one of its NRT readers.
   * Here that commit isn't the latest one: either the reader was opened during a commit, or it got
   * the version that the writer increased to after that commit, so closing the writer committed
   * again. If the deletion policy deleted the reader's commit, opening a writer on it must fail
   * with a clear error. Otherwise it must work, and continue after the earlier writer's versions.
   */
  public void testWriterOpenedOnCommitOfReaderOpenedDuringCommit() throws Exception {
    for (boolean keepAllCommits : new boolean[] {false, true}) {
      for (boolean readerAfterCommit : new boolean[] {false, true}) {
        String context =
            "keepAllCommits=" + keepAllCommits + " readerAfterCommit=" + readerAfterCommit;
        BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
        IndexWriterConfig config =
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
        if (keepAllCommits) {
          config.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        }
        IndexWriter writer = new IndexWriter(dir, config);
        writer.addDocument(doc("0", 0));
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);
        writer.addDocument(doc("1", 1));
        reader = reopenDuringCommit(dir, writer, reader, random().nextBoolean());
        if (readerAfterCommit) {
          reader = assertStaleAndReopen(reader, writer, dir, 2);
        }
        long maxVersion = reader.getVersion();
        writer.close();
        maxVersion = Math.max(maxVersion, SegmentInfos.readLatestCommit(dir).getVersion());
        assertNotEquals(context, latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());

        IndexWriterConfig nextConfig =
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setIndexCommit(reader.getIndexCommit());
        if (keepAllCommits) {
          nextConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        }
        if (keepAllCommits == false) {
          IllegalArgumentException e =
              expectThrows(IllegalArgumentException.class, () -> new IndexWriter(dir, nextConfig));
          assertTrue(context + ": " + e.getMessage(), e.getMessage().contains("is stale"));
        } else {
          try (IndexWriter next = new IndexWriter(dir, nextConfig);
              DirectoryReader nextReader = DirectoryReader.open(next)) {
            assertEquals(context, contents(reader), contents(nextReader));
            assertTrue(
                context
                    + ": the next writer's reader must have a higher version: max="
                    + maxVersion
                    + " next="
                    + nextReader.getVersion(),
                nextReader.getVersion() > maxVersion);
          }
        }
        IOUtils.close(reader, dir);
      }
    }
  }

  /**
   * Randomly mixes changes, reopens and commits. The test thread pauses each commit (in its file
   * sync, or between prepareCommit and commit), and sometimes makes it fail. Checks that reopened
   * readers have higher versions, that when no commit is in progress, a current reader points at
   * the latest commit, and that readers with the same version have the same contents, also across
   * writers when the test closes the writer and opens a new one.
   */
  public void testRandomCommitsAndReopens() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    // randomized config, including merges on commit and on getReader:
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    Map<Long, String> versions = new HashMap<>();
    checkVersionNotReused(versions, reader);

    BackgroundCommit syncingCommit = null; // blocked in its file sync
    boolean prepared = false; // prepareCommit was called, but not commit yet
    int reopensDuringCommit = 0;
    int writerRestarts = 0;
    try {
      int numSteps = atLeast(300);
      for (int step = 0; step < numSteps; step++) {
        boolean commitInProgress = syncingCommit != null || prepared;
        int action = random().nextInt(11);
        if (action < 3) {
          String id = Integer.toString(random().nextInt(20));
          switch (random().nextInt(4)) {
            case 0 -> writer.deleteDocuments(new Term("id", id));
            case 1 -> {
              try {
                writer.updateNumericDocValue(new Term("id", id), "dv", random().nextLong());
              } catch (IllegalArgumentException e) {
                // a new writer doesn't know the field if its commit has no documents
                assertTrue(e.getMessage(), e.getMessage().contains("doesn't exist"));
              }
            }
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
            checkVersionNotReused(versions, reader);
            if (commitInProgress) {
              reopensDuringCommit++;
            }
          }
          if (commitInProgress == false && reader.isCurrent()) {
            assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
          }
        } else if (action == 10 && commitInProgress == false) {
          // close the writer normally (it commits pending changes) and open a new one
          writer.close();
          reader.close();
          writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
          reader = DirectoryReader.open(writer);
          checkVersionNotReused(versions, reader);
          writerRestarts++;
        } else if (commitInProgress == false) {
          if (random().nextInt(4) == 0) {
            writer.prepareCommit();
            prepared = true;
          } else {
            // returns once the commit is paused in its file sync, or is done because it had nothing
            // to sync
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
      System.out.println(
          "TEST: "
              + reopensDuringCommit
              + " reopens during a commit, "
              + writerRestarts
              + " writer restarts");
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
      checkVersionNotReused(versions, reader);
    }
    assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
    assertTrue(reader.isCurrent());
    IOUtils.close(reader, writer, dir);
  }
}
