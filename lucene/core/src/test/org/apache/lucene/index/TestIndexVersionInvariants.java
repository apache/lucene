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

import static org.apache.lucene.index.TestNRTReaderDuringCommit.SOFT_DELETES_FIELD;
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
import org.apache.lucene.document.NumericDocValuesField;
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
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Randomly changes, reopens and commits an index, pauses commits so that readers are opened while
 * they are in progress, and closes, rolls back and reopens the writer. Checks these rules for
 * reader versions ({@link DirectoryReader#getVersion()}) and {@link DirectoryReader#isCurrent()}:
 *
 * <ol>
 *   <li>Readers with the same version see the same documents, also across writers. The only
 *       exception is after a rollback, or after closing a writer that doesn't commit on close: the
 *       writer's uncommitted changes are lost, so versions above the latest commit's may be used
 *       again, for different documents.
 *   <li>Versions never go down: reopening a reader gives a higher version, each reader of a writer
 *       has at least the version of its earlier readers, and after a writer is closed normally, so
 *       do the next writer's readers.
 *   <li>A current reader uses the latest commit, or, if its writer was opened on an older commit
 *       and didn't commit yet, that commit.
 *   <li>A current reader can't be reopened. A reader that isn't current can be reopened, if not
 *       right away then on the next try.
 *   <li>A writer without uncommitted changes doesn't commit anything.
 *   <li>Once the index stops changing, refreshing, and committing when there are uncommitted
 *       changes, soon stop writing new commits.
 *   <li>Opening a writer on an NRT reader's commit either works, or fails because that commit was
 *       deleted.
 * </ol>
 */
public class TestIndexVersionInvariants extends LuceneTestCase {

  /** How long to wait for a commit to pause, and how long a paused commit waits to be resumed. */
  private static final int TIMEOUT_SECONDS = 30;

  /** Documents get ids below this, so that most changes hit an existing document. */
  private static final int NUM_IDS = 30;

  /** Once the index stops changing, commits must stop within this many rounds. */
  private static final int MAX_ROUNDS_UNTIL_NO_COMMIT = 10;

  /** A choice that the test makes at random, picked as often as its weight says. */
  private interface Weighted {
    int weight();
  }

  /** Picks one of {@code choices} at random, according to their weights. */
  private static <T extends Weighted> T pick(Random random, T[] choices) {
    int totalWeight = 0;
    for (T choice : choices) {
      totalWeight += choice.weight();
    }
    int target = random.nextInt(totalWeight);
    for (T choice : choices) {
      target -= choice.weight();
      if (target < 0) {
        return choice;
      }
    }
    throw new AssertionError("unreachable");
  }

  /** Returns true one time in {@code n}, on average. */
  private static boolean oneIn(Random random, int n) {
    return random.nextInt(n) == 0;
  }

  /** The steps of {@link #testRandomOperations}. */
  private enum Step implements Weighted {
    /** Changes the index, see {@link Change}. */
    CHANGE(30),
    /** Reopens the NRT reader. */
    REOPEN(30),
    /** Opens a reader on the latest commit, without the writer. */
    OPEN_FROM_DIRECTORY(5),
    /** Starts a commit, or finishes the commit in progress. */
    COMMIT(20),
    /** Checks the rules that only hold while no commit is in progress. */
    CHECK_WHEN_IDLE(8),
    /** Closes or rolls back the writer, and opens a new one. */
    RESTART_WRITER(7);

    private final int weight;

    Step(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** The changes that {@link #change} makes. */
  private enum Change implements Weighted {
    UPDATE_DOCUMENT(40, false),
    DELETE_BY_TERM(15, false),
    DELETE_BY_QUERY(5, false),
    UPDATE_DOC_VALUE(15, false),
    /** Soft deletes the document and adds a new one, if the index uses soft deletes. */
    SOFT_UPDATE_DOCUMENT(15, false),
    FLUSH(7, false),
    SET_COMMIT_DATA(6, false),
    FORCE_MERGE(5, true),
    DELETE_ALL(2, true),
    ADD_INDEXES(5, true);

    private final int weight;

    /**
     * The change waits for merges to finish. A commit that is paused while it waits for merges
     * holds them up, so the test can't make such a change then.
     */
    final boolean waitsForMerges;

    Change(int weight, boolean waitsForMerges) {
      this.weight = weight;
      this.waitsForMerges = waitsForMerges;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** How {@link Step#COMMIT} starts a commit. */
  private enum CommitKind implements Weighted {
    /** Commits right away. */
    NOW(2),
    /** Calls prepareCommit; a later step commits or rolls back. */
    PREPARE(2),
    /** Commits on another thread, and pauses the commit until a later step lets it finish. */
    PAUSED(6);

    private final int weight;

    CommitKind(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** How {@link Step#REOPEN} gets a new NRT reader. */
  private enum ReopenKind implements Weighted {
    OPEN_IF_CHANGED(2),
    OPEN_IF_CHANGED_WITH_WRITER(1),
    /** Opens a new reader from the writer, instead of reopening the current one. */
    OPEN_NEW(1);

    private final int weight;

    ReopenKind(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** Which commit {@link Step#RESTART_WRITER} opens the next writer on. */
  private enum NextWriter implements Weighted {
    LATEST_COMMIT(5),
    /** {@link OpenMode#CREATE}, which empties the index. */
    CREATE(1),
    /** A random commit that is still around, with {@link IndexWriterConfig#setIndexCommit}. */
    RANDOM_COMMIT(2),
    /** The commit of the current NRT reader, which continues from the earlier writer. */
    READER_COMMIT(2);

    private final int weight;

    NextWriter(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** The commit that {@link #testRandomOperations} started and didn't finish yet. */
  private enum PendingCommit {
    NONE,
    /** prepareCommit was called, but not commit yet. */
    PREPARED,
    /** Paused in its file sync, where it doesn't hold the writer's lock. */
    PAUSED_IN_SYNC,
    /** Paused while it waits for merges, where it doesn't hold the writer's lock. */
    PAUSED_WAITING_FOR_MERGES
  }

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
     * Called when the writer's uncommitted changes were lost, so versions above the latest commit's
     * may be used again, for different documents.
     */
    synchronized void forgetUncommittedVersions(Directory dir) throws IOException {
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
          assertTrue(resume.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
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

  private static WriterOptions randomOptions() {
    return new WriterOptions(random().nextBoolean(), random().nextBoolean(), rarely() == false);
  }

  /** A randomized config, including merges on commit and on getReader. */
  private static IndexWriterConfig newConfig(
      WriterOptions options, boolean softDeletes, IndexWriterEventListener listener) {
    IndexWriterConfig config =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setCommitOnClose(options.commitOnClose())
            .setIndexDeletionPolicy(
                options.keepAllCommits()
                    ? NoDeletionPolicy.INSTANCE
                    : new KeepOnlyLastCommitDeletionPolicy())
            .setIndexWriterEventListener(listener);
    if (softDeletes) {
      config.setSoftDeletesField(SOFT_DELETES_FIELD);
    }
    if (options.mergeOnCommit()) {
      config
          .setMergePolicy(new MergeOnTriggerPolicy(config.getMergePolicy(), MergeTrigger.COMMIT))
          .setMaxFullFlushMergeWaitMillis(TIMEOUT_SECONDS * 1000);
    }
    return config;
  }

  /**
   * Runs random steps (see {@link Step}) from a single thread, and checks the rules after each one.
   */
  public void testRandomOperations() throws Exception {
    RandomOperations test = new RandomOperations(new BlockingSyncDirectory(newDirectory()));
    boolean success = false;
    try {
      int numSteps = atLeast(300);
      for (int step = 0; step < numSteps; step++) {
        test.step(step);
      }
      test.finish();
      success = true;
    } finally {
      test.close(success);
    }
    if (VERBOSE) {
      System.out.println("TEST: " + test.stats());
    }
  }

  /**
   * The state of {@link #testRandomOperations}: the current writer and reader, and the commit in
   * progress, if any. Each kind of step is one method.
   */
  private static final class RandomOperations {
    private final BlockingSyncDirectory dir;

    /** Whether the index uses soft deletes. All writers on an index must agree on this. */
    private final boolean softDeletes = random().nextBoolean();

    private final MergeWaitPause mergeWaitPause = new MergeWaitPause();
    private final VersionHistory history = new VersionHistory();
    private WriterOptions options = randomOptions();
    private IndexWriter writer;
    private DirectoryReader reader;

    private PendingCommit pendingCommit = PendingCommit.NONE;

    /** The commit running on another thread, while {@link #pendingCommit} is paused. */
    private BackgroundCommit pausedCommit;

    /**
     * The commit that the writer was opened on with {@link IndexWriterConfig#setIndexCommit}, or
     * null. Until the writer commits, a current reader uses this commit, which may not be the
     * latest one.
     */
    private String openedOnCommit;

    /** The latest commit when the writer was opened, to tell whether it committed since. */
    private String latestCommitAtOpen;

    /** The next id for documents that addIndexes adds. */
    private int nextAddedId;

    // printed with -Dtests.verbose=true, to see that the test covers these cases:
    private int reopensDuringCommit;
    private int writerRestarts;
    private int lostChanges;

    RandomOperations(BlockingSyncDirectory dir) throws IOException {
      this.dir = dir;
      writer = new IndexWriter(dir, newConfig(options, softDeletes, mergeWaitPause));
      writer.addDocument(doc("0", 0));
      writer.commit();
      reader = DirectoryReader.open(writer);
      history.record(reader);
    }

    String stats() {
      return reopensDuringCommit
          + " reopens during a commit, "
          + writerRestarts
          + " writer restarts, "
          + lostChanges
          + " times uncommitted changes were lost";
    }

    private boolean commitInProgress() {
      return pendingCommit != PendingCommit.NONE;
    }

    void step(int step) throws Exception {
      switch (pick(random(), Step.values())) {
        case CHANGE ->
            nextAddedId = change(random(), writer, commitInProgress() == false, step, nextAddedId);
        case REOPEN -> reopen();
        case OPEN_FROM_DIRECTORY -> openFromDirectory();
        case COMMIT -> {
          switch (pendingCommit) {
            case NONE -> startCommit();
            case PREPARED -> finishPreparedCommit();
            case PAUSED_IN_SYNC, PAUSED_WAITING_FOR_MERGES -> finishPausedCommit();
          }
        }
        case CHECK_WHEN_IDLE -> {
          if (commitInProgress() == false) {
            checkWhenIdle();
          }
        }
        case RESTART_WRITER -> {
          if (commitInProgress() == false) {
            restartWriter(oneIn(random(), 4));
          }
        }
      }
    }

    /** Reopens the reader, and checks that the new reader has a higher version (rule 2). */
    private void reopen() throws IOException {
      ReopenKind kind = pick(random(), ReopenKind.values());
      DirectoryReader newReader =
          switch (kind) {
            case OPEN_IF_CHANGED -> DirectoryReader.openIfChanged(reader);
            case OPEN_IF_CHANGED_WITH_WRITER -> DirectoryReader.openIfChanged(reader, writer);
            case OPEN_NEW -> DirectoryReader.open(writer);
          };
      if (newReader == null) {
        return;
      }
      if (kind != ReopenKind.OPEN_NEW) {
        assertTrue(
            "a reopened reader must have a higher version: old="
                + reader.getVersion()
                + " new="
                + newReader.getVersion(),
            newReader.getVersion() > reader.getVersion());
      }
      assertTrue(
          "a writer's reader must not have a lower version than earlier readers: new="
              + newReader.getVersion()
              + " max="
              + history.maxVersion(),
          newReader.getVersion() >= history.maxVersion());
      history.record(newReader);
      reader.close();
      reader = newReader;
      if (commitInProgress()) {
        reopensDuringCommit++;
      }
    }

    private void openFromDirectory() throws IOException {
      try (DirectoryReader fromCommit = DirectoryReader.open(dir)) {
        history.record(fromCommit);
        assertEquals(latestCommit(dir), fromCommit.getIndexCommit().getSegmentsFileName());
      }
    }

    private void startCommit() throws Exception {
      switch (pick(random(), CommitKind.values())) {
        case NOW -> writer.commit();
        case PREPARE -> {
          writer.prepareCommit();
          pendingCommit = PendingCommit.PREPARED;
        }
        case PAUSED -> startPausedCommit();
      }
    }

    /**
     * Starts a commit on another thread, and pauses it either in its file sync or while it waits
     * for merges. A commit paused in its file sync sometimes fails once it continues.
     */
    private void startPausedCommit() throws Exception {
      // counted down once the commit is paused, or once it is done if it didn't get there
      CountDownLatch pausedOrDone = new CountDownLatch(1);
      boolean inMergeWait = options.mergeOnCommit() && random().nextBoolean();
      if (inMergeWait) {
        mergeWaitPause.pauseNextCommit(random().nextBoolean(), pausedOrDone);
      } else {
        dir.blockNextCommitSync(oneIn(random(), 5), pausedOrDone);
      }
      BackgroundCommit commit = new BackgroundCommit(writer, pausedOrDone);
      assertTrue(pausedOrDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      if (dir.isSyncBlocked() || mergeWaitPause.isPaused()) {
        pausedCommit = commit;
        pendingCommit =
            inMergeWait ? PendingCommit.PAUSED_WAITING_FOR_MERGES : PendingCommit.PAUSED_IN_SYNC;
      } else {
        // the commit had nothing to sync, or no merges to wait for
        dir.disarm();
        mergeWaitPause.disarm();
        assertNull(commit.join());
      }
    }

    private void finishPreparedCommit() throws IOException {
      if (oneIn(random(), 5)) {
        // discards the prepared commit, and closes the writer
        restartWriter(true);
      } else {
        writer.commit();
        pendingCommit = PendingCommit.NONE;
      }
    }

    /** Lets the paused commit continue, and checks that it fails if and only if it should. */
    private void finishPausedCommit() throws InterruptedException {
      boolean inSync = pendingCommit == PendingCommit.PAUSED_IN_SYNC;
      boolean shouldFail = inSync && dir.willFailSync();
      if (inSync) {
        dir.releaseSync();
      } else {
        mergeWaitPause.resume();
      }
      Throwable failure = pausedCommit.join();
      pausedCommit = null;
      pendingCommit = PendingCommit.NONE;
      if (shouldFail) {
        assertTrue(String.valueOf(failure), failure instanceof IOException);
      } else {
        assertNull(failure);
      }
    }

    /**
     * Checks rules 3, 4 and 5, which only hold while no commit is in progress and no merge is
     * running. Reopens the reader if it isn't current.
     */
    private void checkWhenIdle() throws IOException {
      writer.waitForMerges();
      // rule 5
      if (writer.hasUncommittedChanges() == false) {
        String lastCommit = latestCommit(dir);
        writer.commit();
        assertEquals("a commit without changes must not commit", lastCommit, latestCommit(dir));
      }
      // merges on commit may have started
      writer.waitForMerges();

      // rule 3
      if (openedOnCommit != null && latestCommit(dir).equals(latestCommitAtOpen) == false) {
        // the writer committed since it was opened
        openedOnCommit = null;
      }
      String expectedCommit = openedOnCommit != null ? openedOnCommit : latestCommit(dir);
      boolean current = reader.isCurrent();
      if (current) {
        assertEquals(expectedCommit, reader.getIndexCommit().getSegmentsFileName());
      }

      // rule 4
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      if (current) {
        assertNull("a current reader can't be reopened", newReader);
        return;
      }
      if (newReader == null) {
        if (reader.isCurrent()) {
          return;
        }
        // When opening a reader merges segments, the new reader has the version from before the
        // merge, so openIfChanged discards it as unchanged. Once the merge is applied, the
        // writer's version changes, so the next try must return a new reader:
        newReader = DirectoryReader.openIfChanged(reader);
        assertNotNull("a reader that isn't current must not stay unchanged", newReader);
      }
      assertTrue(newReader.getVersion() > reader.getVersion());
      history.record(newReader);
      reader.close();
      reader = newReader;
    }

    /**
     * Closes or rolls back the writer, and opens a new one with random options, see {@link
     * NextWriter}. Checks rules 1, 2 and 7.
     */
    private void restartWriter(boolean rollback) throws IOException {
      boolean readerWasCurrent = reader.isCurrent();
      boolean changesLost;
      if (rollback) {
        writer.rollback();
        pendingCommit = PendingCommit.NONE;
        changesLost = true;
      } else {
        writer.close();
        changesLost = options.commitOnClose() == false;
      }
      long minVersion; // the next writer's first reader must have at least this version
      if (changesLost) {
        history.forgetUncommittedVersions(dir);
        minVersion = -1;
        lostChanges++;
      } else {
        assertCommittedEveryVersion(dir, history);
        minVersion = history.maxVersion();
      }

      options = randomOptions();
      IndexWriterConfig config = newConfig(options, softDeletes, mergeWaitPause);
      openedOnCommit = null;
      latestCommitAtOpen = latestCommit(dir);
      String expectedDocs = null; // the documents the next writer's first reader must see, if known
      List<IndexCommit> commits = DirectoryReader.listCommits(dir);
      NextWriter next = pick(random(), NextWriter.values());
      if (next == NextWriter.RANDOM_COMMIT && commits.size() < 2) {
        next = NextWriter.LATEST_COMMIT;
      }
      // Only from a current reader, whose commit is the latest one: then the new writer continues
      // from the earlier writer's generation, and its readers use the latest commit.
      // With soft deletes, a writer opened on the commit of an NRT reader that saw soft deletes
      // that weren't written yet fails with "softDeleteCount doesn't match", a separate bug.
      if (next == NextWriter.READER_COMMIT
          && (changesLost || readerWasCurrent == false || softDeletes)) {
        next = NextWriter.LATEST_COMMIT;
      }
      switch (next) {
        case LATEST_COMMIT -> {}
        case CREATE -> {
          config.setOpenMode(OpenMode.CREATE);
          expectedDocs = "{}";
        }
        case RANDOM_COMMIT -> {
          IndexCommit commit = commits.get(random().nextInt(commits.size()));
          config.setIndexCommit(commit);
          openedOnCommit = commit.getSegmentsFileName();
          try (DirectoryReader fromCommit = DirectoryReader.open(commit)) {
            expectedDocs = contents(fromCommit);
          }
        }
        case READER_COMMIT -> {
          config.setIndexCommit(reader.getIndexCommit());
          expectedDocs = contents(reader);
        }
      }

      try {
        writer = new IndexWriter(dir, config);
      } catch (IllegalArgumentException e) {
        if (next != NextWriter.READER_COMMIT) {
          throw e;
        }
        // rule 7: the reader's commit was deleted
        assertTrue(e.getMessage(), e.getMessage().contains("is stale"));
        String segmentsFile = reader.getIndexCommit().getSegmentsFileName();
        for (IndexCommit commit : DirectoryReader.listCommits(dir)) {
          assertNotEquals(segmentsFile, commit.getSegmentsFileName());
        }
        writer = new IndexWriter(dir, newConfig(options, softDeletes, mergeWaitPause));
        expectedDocs = null;
      }
      writerRestarts++;

      reader.close();
      reader = DirectoryReader.open(writer);
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
    }

    /**
     * Finishes the commit in progress, checks that commits stop once the index stops changing (rule
     * 6), and closes the writer. Then checks every commit that is still around.
     */
    void finish() throws Exception {
      switch (pendingCommit) {
        case NONE -> {}
        case PREPARED -> {
          writer.commit();
          pendingCommit = PendingCommit.NONE;
        }
        case PAUSED_IN_SYNC, PAUSED_WAITING_FOR_MERGES -> finishPausedCommit();
      }

      checkStopsCommitting();

      writer.close();
      if (options.commitOnClose()) {
        assertCommittedEveryVersion(dir, history);
      } else {
        history.forgetUncommittedVersions(dir);
      }
      for (IndexCommit commit : DirectoryReader.listCommits(dir)) {
        try (DirectoryReader fromCommit = DirectoryReader.open(commit)) {
          history.record(fromCommit);
        }
      }
    }

    /**
     * Once the index stops changing, refreshes, and commits if there are uncommitted changes, until
     * a round does neither. Opens a reader while each commit is in progress, like an application
     * that commits and refreshes on separate threads. Fails if this doesn't stop soon.
     */
    private void checkStopsCommitting() throws Exception {
      // fewer segments, so merges stop soon
      writer.forceMerge(1);
      for (int round = 0; ; round++) {
        assertTrue(
            "the index keeps changing after " + round + " rounds",
            round < MAX_ROUNDS_UNTIL_NO_COMMIT);
        writer.waitForMerges();
        boolean committed = false;
        if (writer.hasUncommittedChanges()) {
          String lastCommit = latestCommit(dir);
          CountDownLatch pausedOrDone = new CountDownLatch(1);
          dir.blockNextCommitSync(false, pausedOrDone);
          BackgroundCommit commit = new BackgroundCommit(writer, pausedOrDone);
          try {
            assertTrue(pausedOrDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            reopenIfChanged();
          } finally {
            dir.disarm();
            dir.releaseSync();
            assertNull(commit.join());
          }
          committed = latestCommit(dir).equals(lastCommit) == false;
        }
        if (reopenIfChanged() == false && committed == false) {
          return;
        }
      }
    }

    private boolean reopenIfChanged() throws IOException {
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      if (newReader == null) {
        return false;
      }
      history.record(newReader);
      reader.close();
      reader = newReader;
      return true;
    }

    /**
     * Lets a paused commit finish, and closes the reader, the writer and the directory. After a
     * failure, closes them without throwing, so that the test reports the failure and not files
     * left open because of it.
     */
    void close(boolean success) throws Exception {
      dir.disarm();
      mergeWaitPause.disarm();
      if (pausedCommit != null) {
        if (pendingCommit == PendingCommit.PAUSED_IN_SYNC) {
          dir.releaseSync();
        } else {
          mergeWaitPause.resume();
        }
        pausedCommit.join();
      }
      IOUtils.closeWhileHandlingException(reader);
      if (success) {
        if (writer.isOpen()) {
          writer.rollback();
        }
        dir.close();
      } else {
        IOUtils.closeWhileHandlingException(writer::rollback, dir);
      }
    }
  }

  /**
   * Indexes, refreshes and commits on separate threads, like an application does, so readers are
   * often opened while a commit is in progress. Closes or rolls back the writer between rounds.
   * Checks rules 1 and 2 for every searcher that the refreshing thread gets, and also records each
   * searcher in a {@link SearcherLifetimeManager}, which rejects a searcher with the same version
   * as an earlier one. Once all threads have stopped, also checks rule 3.
   */
  public void testConcurrentIndexingRefreshesAndCommits() throws Exception {
    Directory dir = newDirectory();
    VersionHistory history = new VersionHistory();
    try (IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setSoftDeletesField(SOFT_DELETES_FIELD))) {
      writer.addDocument(doc("0", 0));
    }
    long minVersion = -1; // the next writer's readers must have at least this version
    int rounds = atLeast(3);
    for (int round = 0; round < rounds; round++) {
      // keeping all commits would keep every one of the many commits here
      WriterOptions options = new WriterOptions(false, random().nextBoolean(), true);
      IndexWriter writer =
          new IndexWriter(dir, newConfig(options, true, IndexWriterEventListener.NO_OP_LISTENER));
      try (SearcherManager searcherManager = new SearcherManager(writer, null);
          SearcherLifetimeManager lifetimeManager = new SearcherLifetimeManager()) {
        Refresher refresher = new Refresher(searcherManager, lifetimeManager, history, minVersion);
        int opsPerIndexer = atLeast(200);
        List<Task> indexers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
          indexers.add(indexer(new Random(random().nextLong()), writer, opsPerIndexer));
        }
        runThreads(
            indexers, List.of(refresher, committer(new Random(random().nextLong()), writer)));

        // one more refresh, after everything stopped: if the reader is current, it must use the
        // last commit
        writer.waitForMerges();
        refresher.run();
        IndexSearcher searcher = searcherManager.acquire();
        try {
          DirectoryReader reader = (DirectoryReader) searcher.getIndexReader();
          if (reader.isCurrent()) {
            assertEquals(latestCommit(dir), reader.getIndexCommit().getSegmentsFileName());
          }
        } finally {
          searcherManager.release(searcher);
        }
      } catch (Throwable t) {
        writer.rollback();
        IOUtils.closeWhileHandlingException(dir);
        throw new AssertionError("round " + round + " failed", t);
      }

      if (oneIn(random(), 4)) {
        writer.rollback();
        history.forgetUncommittedVersions(dir);
        minVersion = -1;
      } else {
        writer.close();
        assertCommittedEveryVersion(dir, history);
        minVersion = history.maxVersion();
      }
    }
    dir.close();
  }

  /** Work for a thread of {@link #runThreads}. */
  private interface Task {
    void run() throws Exception;
  }

  /**
   * Runs each of {@code workers} once on its own thread, and each of {@code loops} on its own
   * thread over and over until all workers are done. Throws the first failure.
   */
  private static void runThreads(List<Task> workers, List<Task> loops) throws Exception {
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> workerThreads = new ArrayList<>();
    for (Task worker : workers) {
      workerThreads.add(
          new Thread(
              () -> {
                try {
                  worker.run();
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
    }
    List<Thread> loopThreads = new ArrayList<>();
    for (Task loop : loops) {
      loopThreads.add(
          new Thread(
              () -> {
                try {
                  while (stop.get() == false && failure.get() == null) {
                    loop.run();
                  }
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
    }
    for (Thread thread : workerThreads) {
      thread.start();
    }
    for (Thread thread : loopThreads) {
      thread.start();
    }
    for (Thread thread : workerThreads) {
      thread.join();
    }
    stop.set(true);
    for (Thread thread : loopThreads) {
      thread.join();
    }
    if (failure.get() != null) {
      throw new AssertionError(failure.get());
    }
  }

  /** Makes {@code numOps} random changes, none of which wait for merges. */
  private static Task indexer(Random random, IndexWriter writer, int numOps) {
    return () -> {
      int nextAddedId = 0;
      for (int op = 0; op < numOps; op++) {
        // a concurrent commit can hold up merges for a while
        nextAddedId = change(random, writer, false, op, nextAddedId);
      }
    };
  }

  /** Commits, either at random or because there are uncommitted changes. */
  private static Task committer(Random random, IndexWriter writer) {
    return () -> {
      if (random.nextBoolean() || writer.hasUncommittedChanges()) {
        writer.commit();
      }
    };
  }

  /** Refreshes, and checks rules 1 and 2 for each new searcher. */
  private static final class Refresher implements Task {
    private final SearcherManager searcherManager;
    private final SearcherLifetimeManager lifetimeManager;
    private final VersionHistory history;
    private long lastVersion;
    private IndexSearcher lastSearcher;

    Refresher(
        SearcherManager searcherManager,
        SearcherLifetimeManager lifetimeManager,
        VersionHistory history,
        long minVersion) {
      this.searcherManager = searcherManager;
      this.lifetimeManager = lifetimeManager;
      this.history = history;
      this.lastVersion = minVersion;
    }

    @Override
    public void run() throws IOException {
      searcherManager.maybeRefresh();
      IndexSearcher searcher = searcherManager.acquire();
      try {
        if (searcher == lastSearcher) {
          return;
        }
        lastSearcher = searcher;
        DirectoryReader reader = (DirectoryReader) searcher.getIndexReader();
        assertTrue(
            "versions must not go down: before=" + lastVersion + " now=" + reader.getVersion(),
            reader.getVersion() >= lastVersion);
        lastVersion = reader.getVersion();
        history.record(reader);
        lifetimeManager.record(searcher);
      } finally {
        searcherManager.release(searcher);
      }
    }
  }

  /**
   * Checks that after a writer was closed normally, the latest commit has at least every version
   * that the writer's readers had (rule 2).
   */
  private static void assertCommittedEveryVersion(Directory dir, VersionHistory history)
      throws IOException {
    long committed = SegmentInfos.readLatestCommit(dir).getVersion();
    assertTrue(
        "closing the writer must commit every version its readers had: committed="
            + committed
            + " max="
            + history.maxVersion(),
        committed >= history.maxVersion());
  }

  /**
   * Makes a random change, see {@link Change}, and returns the next id for documents that
   * addIndexes adds. If {@code mayWaitForMerges} is false, flushes instead of making a change that
   * waits for merges.
   */
  private static int change(
      Random random, IndexWriter writer, boolean mayWaitForMerges, int step, int nextAddedId)
      throws IOException {
    String id = Integer.toString(random.nextInt(NUM_IDS));
    Change change = pick(random, Change.values());
    if (change.waitsForMerges && mayWaitForMerges == false) {
      change = Change.FLUSH;
    }
    switch (change) {
      case UPDATE_DOCUMENT -> writer.updateDocument(new Term("id", id), doc(id, random.nextLong()));
      case SOFT_UPDATE_DOCUMENT -> {
        if (writer.getConfig().getSoftDeletesField() != null) {
          writer.softUpdateDocument(
              new Term("id", id),
              doc(id, random.nextLong()),
              new NumericDocValuesField(SOFT_DELETES_FIELD, 1));
        } else {
          writer.updateDocument(new Term("id", id), doc(id, random.nextLong()));
        }
      }
      case DELETE_BY_TERM -> writer.deleteDocuments(new Term("id", id));
      case DELETE_BY_QUERY -> writer.deleteDocuments(new TermQuery(new Term("id", id)));
      case UPDATE_DOC_VALUE -> {
        try {
          writer.updateNumericDocValue(new Term("id", id), "dv", random.nextLong());
        } catch (IllegalArgumentException e) {
          // the writer doesn't know the field after deleteAll or OpenMode.CREATE, or if it was
          // opened on a commit without documents, until a document adds it again
          assertTrue(e.getMessage(), e.getMessage().contains("doesn't exist"));
        }
      }
      case FLUSH -> writer.flush();
      case SET_COMMIT_DATA ->
          writer.setLiveCommitData(
              Map.of("step", Integer.toString(step)).entrySet(), random.nextBoolean());
      case FORCE_MERGE -> writer.forceMerge(TestUtil.nextInt(random, 1, 3));
      case DELETE_ALL -> writer.deleteAll();
      case ADD_INDEXES -> {
        try (Directory other = new ByteBuffersDirectory()) {
          try (IndexWriter otherWriter = new IndexWriter(other, new IndexWriterConfig())) {
            String addedId = "added" + nextAddedId++;
            otherWriter.addDocument(doc(addedId, random.nextLong()));
          }
          writer.addIndexes(other);
        }
      }
    }
    return nextAddedId;
  }
}
