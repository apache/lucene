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

import static org.apache.lucene.index.TestNRTReaderDuringCommit.contents;
import static org.apache.lucene.index.TestNRTReaderDuringCommit.doc;
import static org.apache.lucene.index.TestNRTReaderDuringCommit.latestCommit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.TestNRTReaderDuringCommit.BackgroundCommit;
import org.apache.lucene.index.TestNRTReaderDuringCommit.BlockingSyncDirectory;
import org.apache.lucene.index.TestNRTReaderDuringCommit.MergeOnTriggerPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Randomly changes, reopens and commits an index, pauses commits so that readers are opened while
 * they are in progress, and closes, rolls back and reopens the writer. Checks these properties of
 * reader versions ({@link DirectoryReader#getVersion()}) and {@link DirectoryReader#isCurrent()}:
 *
 * <ol>
 *   <li>Readers with the same version see the same documents, also across writers. Only a rollback
 *       (or closing a writer that doesn't commit on close) forks the history: then versions above
 *       the latest commit's may be used again, for different documents.
 *   <li>Versions don't go back: reopening a reader gives a higher version, a writer's readers have
 *       at least the version of earlier readers, and after a writer is closed normally, so do the
 *       next writer's readers.
 *   <li>A current reader points at the latest commit, or, if its writer was opened on another
 *       commit and didn't commit yet, at that commit.
 *   <li>A current reader can't be reopened, and a reader that isn't current can be reopened, if not
 *       right away then on the next try.
 *   <li>A writer without uncommitted changes doesn't commit anything.
 *   <li>Once the index doesn't change anymore, refreshing, and committing when there are
 *       uncommitted changes, stops committing.
 *   <li>Opening a writer on an NRT reader's commit either works, or fails because that commit was
 *       deleted.
 * </ol>
 */
public class TestIndexVersionInvariants extends LuceneTestCase {

  /** Every version a reader had, and the documents it saw. */
  private static final class VersionHistory {
    private final Map<Long, String> contents = new HashMap<>();
    private long maxVersion = -1;

    /** Fails if a reader with the same version saw different documents, and records this one. */
    synchronized void record(DirectoryReader reader) throws IOException {
      String docs = contents(reader);
      String previous = contents.putIfAbsent(reader.getVersion(), docs);
      if (previous != null && previous.equals(docs) == false) {
        throw new AssertionError(
            "version "
                + reader.getVersion()
                + " was reused for different documents: before="
                + previous
                + " now="
                + docs);
      }
      maxVersion = Math.max(maxVersion, reader.getVersion());
    }

    synchronized long maxVersion() {
      return maxVersion;
    }

    /**
     * The writer's changes since the latest commit were discarded, so versions above the latest
     * commit's may be used again.
     */
    synchronized void fork(Directory dir) throws IOException {
      long committed = SegmentInfos.readLatestCommit(dir).getVersion();
      contents.keySet().removeIf(version -> version > committed);
      maxVersion = committed;
    }
  }

  /**
   * Pauses the next commit while it waits for its merges ({@link
   * IndexWriterConfig#setMaxFullFlushMergeWaitMillis}), before or after the merges ran. The commit
   * doesn't hold the writer's lock then.
   */
  private static final class MergeWaitPause implements IndexWriterEventListener {
    private final AtomicBoolean armed = new AtomicBoolean();
    private volatile boolean afterMerges;
    private volatile boolean paused;
    private volatile CountDownLatch pausedOrDone;
    private volatile CountDownLatch resume;

    void pauseNextCommit(boolean afterMerges, CountDownLatch pausedOrDone) {
      this.afterMerges = afterMerges;
      this.pausedOrDone = pausedOrDone;
      resume = new CountDownLatch(1);
      armed.set(true);
    }

    void disarm() {
      armed.set(false);
    }

    boolean isPaused() {
      return paused;
    }

    void resume() {
      resume.countDown();
    }

    @Override
    public void beginMergeOnFullFlush(MergePolicy.MergeSpecification merge) {
      if (afterMerges == false) {
        maybePause();
      }
    }

    @Override
    public void endMergeOnFullFlush(MergePolicy.MergeSpecification merge) {
      if (afterMerges) {
        maybePause();
      }
    }

    private void maybePause() {
      if (armed.compareAndSet(true, false)) {
        paused = true;
        pausedOrDone.countDown();
        try {
          assertTrue(resume.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        } finally {
          paused = false;
        }
      }
    }
  }

  /** Writer settings that the test picks randomly for each writer. */
  private record WriterOptions(
      boolean keepAllCommits, boolean mergeOnCommit, boolean commitOnClose) {}

  private WriterOptions randomOptions() {
    return new WriterOptions(random().nextBoolean(), random().nextBoolean(), rarely() == false);
  }

  /** A randomized config, including merges on commit and on getReader. */
  private IndexWriterConfig newConfig(WriterOptions options, IndexWriterEventListener listener) {
    IndexWriterConfig config =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setCommitOnClose(options.commitOnClose())
            .setIndexDeletionPolicy(
                options.keepAllCommits()
                    ? NoDeletionPolicy.INSTANCE
                    : new KeepOnlyLastCommitDeletionPolicy())
            .setIndexWriterEventListener(listener);
    if (options.mergeOnCommit()) {
      config
          .setMergePolicy(new MergeOnTriggerPolicy(config.getMergePolicy(), MergeTrigger.COMMIT))
          .setMaxFullFlushMergeWaitMillis(30_000);
    }
    return config;
  }

  public void testRandomOperations() throws Exception {
    BlockingSyncDirectory dir = new BlockingSyncDirectory(newDirectory());
    MergeWaitPause pause = new MergeWaitPause();
    VersionHistory history = new VersionHistory();
    WriterOptions options = randomOptions();
    IndexWriter writer = new IndexWriter(dir, newConfig(options, pause));
    writer.addDocument(doc("0", 0));
    writer.commit();
    DirectoryReader reader = DirectoryReader.open(writer);
    history.record(reader);

    // set while the writer was opened on a commit other than the latest one and didn't commit yet:
    String openedOnCommit = null;
    String latestCommitAtOpen = null;
    BackgroundCommit pausedCommit = null;
    boolean pausedInMergeWait = false;
    boolean prepared = false; // prepareCommit was called, but not commit yet
    int nextAddedIndex = 0;
    int reopensDuringCommit = 0;
    int writerRestarts = 0;
    int forks = 0;
    try {
      int numSteps = atLeast(300);
      for (int step = 0; step < numSteps; step++) {
        boolean commitInProgress = pausedCommit != null || prepared;
        int action = random().nextInt(100);
        if (action < 30) {
          nextAddedIndex = change(random(), writer, commitInProgress, step, nextAddedIndex);
        } else if (action < 60) {
          DirectoryReader newReader = reopen(writer, reader, history);
          if (newReader != reader) {
            reader = newReader;
            if (commitInProgress) {
              reopensDuringCommit++;
            }
          }
        } else if (action < 65) {
          // a reader on the latest commit
          try (DirectoryReader fromCommit = DirectoryReader.open(dir)) {
            history.record(fromCommit);
            assertEquals(latestCommit(dir), fromCommit.getIndexCommit().getSegmentsFileName());
          }
        } else if (action < 85) {
          if (commitInProgress == false) {
            int kind = random().nextInt(10);
            if (kind < 2) {
              writer.commit();
            } else if (kind < 4) {
              writer.prepareCommit();
              prepared = true;
            } else {
              // returns once the commit is paused, or done if it didn't get there
              CountDownLatch pausedOrDone = new CountDownLatch(1);
              pausedInMergeWait = options.mergeOnCommit() && random().nextBoolean();
              if (pausedInMergeWait) {
                pause.pauseNextCommit(random().nextBoolean(), pausedOrDone);
              } else {
                dir.blockNextCommitSync(random().nextInt(5) == 0, pausedOrDone);
              }
              BackgroundCommit commit = new BackgroundCommit(writer, pausedOrDone);
              assertTrue(pausedOrDone.await(30, TimeUnit.SECONDS));
              if (dir.isSyncBlocked() || pause.isPaused()) {
                pausedCommit = commit;
              } else {
                dir.disarm();
                pause.disarm();
                assertNull(commit.join());
              }
            }
          } else if (prepared) {
            if (random().nextInt(5) == 0) {
              // rolling back discards the prepared commit and the writer
              writer.rollback();
              prepared = false;
              history.fork(dir);
              forks++;
              writer = new IndexWriter(dir, newConfig(options = randomOptions(), pause));
              openedOnCommit = null;
              reader = openFirstReader(writer, reader, history, null, -1);
              writerRestarts++;
            } else {
              writer.commit();
              prepared = false;
            }
          } else {
            finishPausedCommit(dir, pause, pausedCommit, pausedInMergeWait);
            pausedCommit = null;
          }
        } else if (action < 93) {
          if (commitInProgress == false) {
            if (openedOnCommit != null && latestCommit(dir).equals(latestCommitAtOpen) == false) {
              openedOnCommit = null;
            }
            String expectedCommit = openedOnCommit != null ? openedOnCommit : latestCommit(dir);
            reader = checkQuiescent(dir, writer, reader, history, expectedCommit);
          }
        } else if (commitInProgress == false) {
          // close or roll back the writer, and open a new one
          boolean readerWasCurrent = reader.isCurrent();
          boolean fork;
          if (random().nextInt(4) == 0) {
            writer.rollback();
            fork = true;
          } else {
            writer.close();
            fork = options.commitOnClose() == false;
          }
          if (fork) {
            history.fork(dir);
            forks++;
          } else {
            assertTrue(
                "closing the writer must commit every version its readers had: committed="
                    + SegmentInfos.readLatestCommit(dir).getVersion()
                    + " max="
                    + history.maxVersion,
                SegmentInfos.readLatestCommit(dir).getVersion() >= history.maxVersion);
          }
          long maxVersionBefore = fork ? -1 : history.maxVersion;
          options = randomOptions();
          IndexWriterConfig config = newConfig(options, pause);
          String expectedDocs = null;
          boolean fromReader = false;
          openedOnCommit = null;
          latestCommitAtOpen = latestCommit(dir);
          List<IndexCommit> commits = DirectoryReader.listCommits(dir);
          int mode = random().nextInt(10);
          if (mode == 0) {
            config.setOpenMode(OpenMode.CREATE);
            expectedDocs = "{}";
          } else if (mode <= 2 && commits.size() > 1) {
            IndexCommit commit = commits.get(random().nextInt(commits.size()));
            config.setIndexCommit(commit);
            openedOnCommit = commit.getSegmentsFileName();
            try (DirectoryReader fromCommit = DirectoryReader.open(commit)) {
              expectedDocs = contents(fromCommit);
            }
          } else if (mode <= 4 && fork == false && readerWasCurrent) {
            // the new writer continues from the earlier writer's generation, so its readers point
            // at the latest commit
            config.setIndexCommit(reader.getIndexCommit());
            expectedDocs = contents(reader);
            fromReader = true;
          }
          try {
            writer = new IndexWriter(dir, config);
          } catch (IllegalArgumentException e) {
            if (fromReader == false) {
              throw e;
            }
            // the reader's commit was deleted
            assertTrue(e.getMessage(), e.getMessage().contains("is stale"));
            String segmentsFile = reader.getIndexCommit().getSegmentsFileName();
            for (IndexCommit commit : DirectoryReader.listCommits(dir)) {
              assertNotEquals(segmentsFile, commit.getSegmentsFileName());
            }
            writer = new IndexWriter(dir, newConfig(options, pause));
            expectedDocs = null;
            openedOnCommit = null;
          }
          reader = openFirstReader(writer, reader, history, expectedDocs, maxVersionBefore);
          writerRestarts++;
        }
      }

      // finish the commit in progress
      if (pausedCommit != null) {
        finishPausedCommit(dir, pause, pausedCommit, pausedInMergeWait);
        pausedCommit = null;
      }
      if (prepared) {
        writer.commit();
        prepared = false;
      }

      reader = checkSettles(dir, writer, reader, history);

      writer.close();
      if (options.commitOnClose()) {
        try (DirectoryReader fromCommit = DirectoryReader.open(dir)) {
          history.record(fromCommit);
          assertTrue(fromCommit.getVersion() >= history.maxVersion);
        }
      } else {
        history.fork(dir);
      }
      // every commit that is still around
      for (IndexCommit commit : DirectoryReader.listCommits(dir)) {
        try (DirectoryReader fromCommit = DirectoryReader.open(commit)) {
          history.record(fromCommit);
        }
      }
    } finally {
      dir.disarm();
      pause.disarm();
      if (pausedCommit != null) {
        dir.releaseSync();
        pause.resume();
        pausedCommit.join();
      }
      IOUtils.closeWhileHandlingException(reader);
      if (writer.isOpen()) {
        writer.rollback();
      }
      dir.close();
    }
    if (VERBOSE) {
      System.out.println(
          "TEST: "
              + reopensDuringCommit
              + " reopens during a commit, "
              + writerRestarts
              + " writer restarts, "
              + forks
              + " forks");
    }
  }

  /**
   * Indexes, refreshes and commits on separate threads, like an application does, so readers are
   * often opened while a commit is in progress, and closes or rolls back the writer between rounds.
   * Checks properties 1 and 2 for every searcher the refreshing thread gets, which it also records
   * in a {@link SearcherLifetimeManager}: that rejects a searcher with the same version as an
   * earlier one. Once all threads stopped, also checks property 3.
   */
  public void testConcurrentIndexingRefreshesAndCommits() throws Exception {
    Directory dir = newDirectory();
    VersionHistory history = new VersionHistory();
    try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
      writer.addDocument(doc("0", 0));
    }
    long minVersion = -1; // the next writer's readers must have at least this version
    int rounds = atLeast(3);
    for (int round = 0; round < rounds; round++) {
      // keeping all commits would keep every one of the many commits here
      WriterOptions options = new WriterOptions(false, random().nextBoolean(), true);
      IndexWriter writer =
          new IndexWriter(dir, newConfig(options, IndexWriterEventListener.NO_OP_LISTENER));
      SearcherManager searcherManager = new SearcherManager(writer, null);
      SearcherLifetimeManager lifetimeManager = new SearcherLifetimeManager();
      AtomicBoolean stop = new AtomicBoolean();
      AtomicReference<Throwable> failure = new AtomicReference<>();
      List<Thread> indexers = new ArrayList<>();
      List<Thread> others = new ArrayList<>();
      int numIndexers = 2;
      int opsPerIndexer = atLeast(200);
      for (int i = 0; i < numIndexers; i++) {
        Random threadRandom = new Random(random().nextLong());
        indexers.add(
            new Thread(
                () -> {
                  try {
                    int nextAddedIndex = 0;
                    for (int op = 0; op < opsPerIndexer && failure.get() == null; op++) {
                      // only changes that don't wait for merges, which a commit can hold up:
                      nextAddedIndex = change(threadRandom, writer, true, op, nextAddedIndex);
                    }
                  } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                  }
                }));
      }
      long firstVersion = minVersion;
      others.add(
          new Thread(
              () -> {
                try {
                  long lastVersion = firstVersion;
                  IndexSearcher lastSearcher = null;
                  while (stop.get() == false && failure.get() == null) {
                    searcherManager.maybeRefresh();
                    IndexSearcher searcher = searcherManager.acquire();
                    try {
                      if (searcher == lastSearcher) {
                        continue;
                      }
                      lastSearcher = searcher;
                      DirectoryReader reader = (DirectoryReader) searcher.getIndexReader();
                      assertTrue(
                          "versions must not go back: before="
                              + lastVersion
                              + " now="
                              + reader.getVersion(),
                          reader.getVersion() >= lastVersion);
                      lastVersion = reader.getVersion();
                      history.record(reader);
                      lifetimeManager.record(searcher);
                    } finally {
                      searcherManager.release(searcher);
                    }
                  }
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
      Random committerRandom = new Random(random().nextLong());
      others.add(
          new Thread(
              () -> {
                try {
                  while (stop.get() == false && failure.get() == null) {
                    if (committerRandom.nextBoolean() || writer.hasUncommittedChanges()) {
                      writer.commit();
                    }
                  }
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
      for (Thread thread : indexers) {
        thread.start();
      }
      for (Thread thread : others) {
        thread.start();
      }
      for (Thread thread : indexers) {
        thread.join();
      }
      stop.set(true);
      for (Thread thread : others) {
        thread.join();
      }
      if (failure.get() != null) {
        IOUtils.closeWhileHandlingException(lifetimeManager, searcherManager);
        writer.rollback();
        dir.close();
        throw new AssertionError("round " + round + " failed", failure.get());
      }

      // one more refresh, after everything stopped: the reader saw the last commit, so if it is
      // current, it must point at it
      writer.waitForMerges();
      searcherManager.maybeRefresh();
      IndexSearcher searcher = searcherManager.acquire();
      try {
        DirectoryReader reader = (DirectoryReader) searcher.getIndexReader();
        history.record(reader);
        lifetimeManager.record(searcher);
        if (reader.isCurrent()) {
          assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
        }
      } finally {
        searcherManager.release(searcher);
      }
      IOUtils.close(lifetimeManager, searcherManager);

      if (random().nextInt(4) == 0) {
        writer.rollback();
        history.fork(dir);
        minVersion = -1;
      } else {
        writer.close();
        long committed = SegmentInfos.readLatestCommit(dir).getVersion();
        assertTrue(
            "closing the writer must commit every version its readers had: committed="
                + committed
                + " max="
                + history.maxVersion(),
            committed >= history.maxVersion());
        minVersion = history.maxVersion();
      }
    }
    dir.close();
  }

  /** Lets a paused commit continue, and checks that it fails if and only if it should. */
  private static void finishPausedCommit(
      BlockingSyncDirectory dir,
      MergeWaitPause pause,
      BackgroundCommit commit,
      boolean pausedInMergeWait)
      throws InterruptedException {
    boolean fail = pausedInMergeWait == false && dir.willFailSync();
    if (pausedInMergeWait) {
      pause.resume();
    } else {
      dir.releaseSync();
    }
    Throwable failure = commit.join();
    if (fail) {
      assertTrue(String.valueOf(failure), failure instanceof IOException);
    } else {
      assertNull(failure);
    }
  }

  /** Makes a random change, and returns the next id for documents added with addIndexes. */
  private static int change(
      Random random, IndexWriter writer, boolean commitInProgress, int step, int nextAddedIndex)
      throws IOException {
    String id = Integer.toString(random.nextInt(30));
    int kind = random.nextInt(100);
    if (kind < 40) {
      writer.updateDocument(new Term("id", id), doc(id, random.nextLong()));
    } else if (kind < 55) {
      writer.deleteDocuments(new Term("id", id));
    } else if (kind < 60) {
      writer.deleteDocuments(new TermQuery(new Term("id", id)));
    } else if (kind < 75) {
      try {
        writer.updateNumericDocValue(new Term("id", id), "dv", random.nextLong());
      } catch (IllegalArgumentException e) {
        // deleteAll and OpenMode.CREATE remove the field until a document adds it again
        assertTrue(e.getMessage(), e.getMessage().contains("doesn't exist"));
      }
    } else if (kind < 82) {
      writer.flush();
    } else if (kind < 88) {
      writer.setLiveCommitData(
          Map.of("step", Integer.toString(step)).entrySet(), random.nextBoolean());
    } else if (commitInProgress) {
      // the remaining changes wait for merges, which a paused commit can hold up
      writer.flush();
    } else if (kind < 93) {
      writer.forceMerge(1 + random.nextInt(3));
    } else if (kind < 95) {
      writer.deleteAll();
    } else {
      try (Directory other = new ByteBuffersDirectory()) {
        try (IndexWriter otherWriter = new IndexWriter(other, new IndexWriterConfig())) {
          String addedId = "added" + nextAddedIndex++;
          otherWriter.addDocument(doc(addedId, random.nextLong()));
        }
        writer.addIndexes(other);
      }
    }
    return nextAddedIndex;
  }

  /**
   * Reopens {@code reader} in a random way, and returns the new reader, or the same if unchanged.
   */
  private static DirectoryReader reopen(
      IndexWriter writer, DirectoryReader reader, VersionHistory history) throws IOException {
    DirectoryReader newReader;
    boolean reopened = true;
    switch (random().nextInt(4)) {
      case 0, 1 -> newReader = DirectoryReader.openIfChanged(reader);
      case 2 -> newReader = DirectoryReader.openIfChanged(reader, writer);
      default -> {
        newReader = DirectoryReader.open(writer);
        reopened = false;
      }
    }
    if (newReader == null) {
      return reader;
    }
    if (reopened) {
      assertTrue(
          "reopened reader must have a higher version: old="
              + reader.getVersion()
              + " new="
              + newReader.getVersion(),
          newReader.getVersion() > reader.getVersion());
    }
    assertTrue(
        "a writer's reader must not have a lower version than earlier readers: new="
            + newReader.getVersion()
            + " max="
            + history.maxVersion,
        newReader.getVersion() >= history.maxVersion);
    history.record(newReader);
    reader.close();
    return newReader;
  }

  /**
   * Opens the first reader of a new writer, closing the earlier writer's reader. If {@code
   * minVersion} isn't -1, the new reader must have at least that version.
   */
  private static DirectoryReader openFirstReader(
      IndexWriter writer,
      DirectoryReader oldReader,
      VersionHistory history,
      String expectedDocs,
      long minVersion)
      throws IOException {
    oldReader.close();
    DirectoryReader reader = DirectoryReader.open(writer);
    if (expectedDocs != null) {
      assertEquals(expectedDocs, contents(reader));
    }
    assertTrue(
        "the next writer's reader must not have a lower version: min="
            + minVersion
            + " new="
            + reader.getVersion(),
        reader.getVersion() >= minVersion);
    history.record(reader);
    return reader;
  }

  /**
   * Checks properties that only hold while no commit is in progress and no merge is running, and
   * returns the reader, reopened if it wasn't current.
   */
  private static DirectoryReader checkQuiescent(
      Directory dir,
      IndexWriter writer,
      DirectoryReader reader,
      VersionHistory history,
      String expectedCommit)
      throws IOException {
    writer.waitForMerges();
    // a writer without uncommitted changes doesn't commit anything:
    if (writer.hasUncommittedChanges() == false) {
      String lastCommit = latestCommit(dir);
      writer.commit();
      assertEquals(lastCommit, latestCommit(dir));
    }

    // merges on commit may have run
    writer.waitForMerges();
    boolean current = reader.isCurrent();
    if (current) {
      assertEquals(expectedCommit, reader.getIndexCommit().getSegmentsFileName());
    }
    DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
    if (current) {
      assertNull("a current reader can't be reopened", newReader);
      return reader;
    } else if (newReader == null) {
      if (reader.isCurrent()) {
        return reader;
      }
      // A merge on getReader gives a reader with the version from before the merge, which
      // openIfChanged then discards as unchanged, and applying the merge changes the writer's
      // version afterwards. The next reopen must not be unchanged too:
      newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull("a reader that isn't current must not stay unchanged", newReader);
    }
    assertTrue(newReader.getVersion() > reader.getVersion());
    history.record(newReader);
    reader.close();
    return newReader;
  }

  /**
   * Once the index doesn't change anymore, refreshes, and commits if there are uncommitted changes,
   * until neither does anything. A reader is opened while each commit is in progress, like an
   * application that commits and refreshes on separate threads. This must stop after a few rounds.
   */
  private static DirectoryReader checkSettles(
      BlockingSyncDirectory dir, IndexWriter writer, DirectoryReader reader, VersionHistory history)
      throws Exception {
    // fewer segments, so merges stop soon
    writer.forceMerge(1);
    for (int round = 0; ; round++) {
      assertTrue("the index keeps changing after " + round + " rounds", round < 10);
      writer.waitForMerges();
      boolean committed = false;
      if (writer.hasUncommittedChanges()) {
        String lastCommit = latestCommit(dir);
        CountDownLatch pausedOrDone = new CountDownLatch(1);
        dir.blockNextCommitSync(false, pausedOrDone);
        BackgroundCommit commit = new BackgroundCommit(writer, pausedOrDone);
        try {
          assertTrue(pausedOrDone.await(30, TimeUnit.SECONDS));
          DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
          if (newReader != null) {
            history.record(newReader);
            reader.close();
            reader = newReader;
          }
        } finally {
          dir.disarm();
          dir.releaseSync();
          assertNull(commit.join());
        }
        committed = latestCommit(dir).equals(lastCommit) == false;
      }
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      if (newReader != null) {
        history.record(newReader);
        reader.close();
        reader = newReader;
      } else if (committed == false) {
        return reader;
      }
    }
  }
}
