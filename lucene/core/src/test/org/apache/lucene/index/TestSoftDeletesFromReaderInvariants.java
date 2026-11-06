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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Opens writers on the commits of readers in an index with soft deletes, and checks that each new
 * writer starts with exactly the documents its reader saw.
 *
 * <p>{@link #testRandomOperations} runs random steps (see {@link Step}) and keeps a model of the
 * documents of each id. It checks these rules:
 *
 * <ol>
 *   <li>The writer has the documents of the model.
 *   <li>A writer opened on a reader's commit starts with the documents the reader saw, and {@link
 *       IndexWriter#getDocStats} counts them.
 *   <li>An NRT reader's live docs are the documents that are neither hard deleted nor soft deleted,
 *       and its numDocs counts them.
 *   <li>CheckIndex passes on each commit. It checks the soft delete count of every segment.
 * </ol>
 */
public class TestSoftDeletesFromReaderInvariants extends LuceneTestCase {

  private static final String SOFT_DELETES_FIELD = "soft_delete";

  /** At most this many changes are made after a reader is opened and before the writer closes. */
  private static final int MAX_CHANGES_AFTER_READER = 4;

  /** An undelete gives up after this many tries, see {@link #undelete}. */
  private static final int MAX_UNDELETE_ATTEMPTS = 100;

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

  /** The steps of {@link #testRandomOperations}. */
  private enum Step implements Weighted {
    /** Changes the index, see {@link Change}. */
    CHANGE(75),
    FLUSH(4),
    /** Commits, and runs CheckIndex (rule 4). */
    COMMIT(4),
    /** Checks that the writer has the documents of the model (rules 1 and 3). */
    CHECK(4),
    /** Force merges down to a few segments, or merges away deleted documents. */
    MERGE(2),
    /** Opens a reader, see {@link ReaderKind}, and opens a new writer on its commit (rule 2). */
    OPEN_WRITER_FROM_READER(11);

    private final int weight;

    Step(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** The changes that {@link Step#CHANGE} makes. All but ADD and ADD_TOMBSTONE pick an id. */
  private enum Change implements Weighted {
    /** Adds a document with a new id. */
    ADD(30),
    /** Adds a new version of the id, and soft deletes the old ones. */
    SOFT_UPDATE(20),
    /** Soft deletes all versions of the id, with updateDocValues. */
    SOFT_DELETE(12),
    /**
     * Undeletes the versions of the id that aren't hard deleted, see {@link #undelete}. Only when
     * merges keep soft deleted documents, otherwise this is a HARD_DELETE.
     */
    UNDELETE(6),
    /** Hard deletes all versions of the id. */
    HARD_DELETE(7),
    /** Adds a document with a new id that is soft deleted when it is added. */
    ADD_TOMBSTONE(25);

    private final int weight;

    Change(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** The reader that {@link Step#OPEN_WRITER_FROM_READER} opens the next writer from. */
  private enum ReaderKind implements Weighted {
    /**
     * An NRT reader that applies all deletes, and may write them. Then the test makes a few more
     * changes, and closes or rolls back the writer. Those changes are lost.
     */
    NRT(3),
    /** Like NRT, but the reader doesn't apply all deletes, so it may see deleted documents. */
    NRT_WITHOUT_APPLYING_DELETES(1),
    /** The latest commit, opened from disk. */
    LATEST_COMMIT(2),
    /** A random commit, opened from disk. NoDeletionPolicy keeps all commits. */
    RANDOM_COMMIT(2),
    /**
     * An NRT reader that is reopened on the latest commit after the writer closes. It reuses the
     * segment readers of the NRT reader, but it isn't NRT.
     */
    NRT_REOPENED_ON_COMMIT(2);

    private final int weight;

    ReaderKind(int weight) {
      this.weight = weight;
    }

    @Override
    public int weight() {
      return weight;
    }
  }

  /** The number of documents of each id. */
  private static final class Docs {
    /** Documents that aren't hard deleted. They may be soft deleted. */
    final Map<String, Integer> versions = new TreeMap<>();

    /** Documents that are neither hard deleted nor soft deleted. */
    final Map<String, Integer> live = new TreeMap<>();

    Docs copy() {
      Docs copy = new Docs();
      copy.versions.putAll(versions);
      copy.live.putAll(live);
      return copy;
    }

    int numDocs() {
      return live.values().stream().mapToInt(Integer::intValue).sum();
    }

    /** Removes ids without documents, so two models with the same documents are equal. */
    void normalize() {
      versions.values().removeIf(v -> v == 0);
      live.values().removeIf(v -> v == 0);
    }

    @Override
    public String toString() {
      Docs copy = copy();
      copy.normalize();
      return "versions=" + copy.versions + " live=" + copy.live;
    }
  }

  /** Runs random steps (see {@link Step}) from a single thread, and checks the rules. */
  public void testRandomOperations() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomOperations test = new RandomOperations(dir);
      int numSteps = atLeast(300);
      for (int step = 0; step < numSteps; step++) {
        test.step();
      }
      test.finish();
      if (VERBOSE) {
        System.out.println("TEST: " + test.stats());
      }
    }
  }

  /**
   * The state of {@link #testRandomOperations}: the current writer, the model of its documents, and
   * the model of each commit. Each kind of step is one method.
   */
  private static final class RandomOperations {
    private final Directory dir;

    /**
     * Whether merges keep soft deleted documents. Only then can soft deleted documents be
     * undeleted, and only then does a merge keep the versions of an id.
     */
    private final boolean retainAll = random().nextBoolean();

    /** The model of each commit, by generation. */
    private final Map<Long, Docs> commits = new TreeMap<>();

    private IndexWriter writer;

    /** The documents of the writer, including changes that aren't committed yet. */
    private Docs model = new Docs();

    private int nextId;

    // printed with -Dtests.verbose=true, to see that the test covers these cases:
    private int writersFromReaders;
    private int undeletes;

    RandomOperations(Directory dir) throws IOException {
      this.dir = dir;
      writer = new IndexWriter(dir, newConfig());
      writer.commit();
      recordCommit();
    }

    String stats() {
      return writersFromReaders
          + " writers opened from readers, "
          + undeletes
          + " undeletes, retainAll="
          + retainAll;
    }

    private IndexWriterConfig newConfig() {
      MergePolicy mergePolicy = newMergePolicy();
      if (retainAll) {
        mergePolicy =
            new SoftDeletesRetentionMergePolicy(
                SOFT_DELETES_FIELD, () -> MatchAllDocsQuery.INSTANCE, mergePolicy);
      }
      return newIndexWriterConfig()
          .setSoftDeletesField(SOFT_DELETES_FIELD)
          .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE)
          .setMergePolicy(mergePolicy);
    }

    void step() throws IOException {
      switch (pick(random(), Step.values())) {
        case CHANGE -> change();
        case FLUSH -> writer.flush();
        case COMMIT -> {
          writer.commit();
          recordCommit();
          TestUtil.checkIndex(dir);
        }
        case CHECK -> checkWriter(false);
        case MERGE -> {
          if (random().nextBoolean()) {
            writer.forceMerge(TestUtil.nextInt(random(), 1, 3));
          } else {
            writer.forceMergeDeletes();
          }
        }
        case OPEN_WRITER_FROM_READER -> openWriterFromReader();
      }
    }

    /** Checks the writer and its documents once more, also after merging down to one segment. */
    void finish() throws IOException {
      checkWriter(false);
      writer.forceMerge(1);
      checkWriter(false);
      writer.close();
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertDocs(model, readDocs(reader));
      }
    }

    /** Makes a random change, see {@link Change}, to the writer and the model. */
    private void change() throws IOException {
      List<String> ids = new ArrayList<>(model.versions.keySet());
      Change change = ids.isEmpty() ? Change.ADD : pick(random(), Change.values());
      if (change == Change.UNDELETE && retainAll == false) {
        // merges may have dropped the soft deleted documents
        change = Change.HARD_DELETE;
      }
      String id = ids.isEmpty() ? null : ids.get(random().nextInt(ids.size()));
      Field softDelete = new NumericDocValuesField(SOFT_DELETES_FIELD, 1);
      switch (change) {
        case ADD -> {
          String newId = "d" + nextId++;
          writer.addDocument(doc(newId));
          model.versions.put(newId, 1);
          model.live.put(newId, 1);
        }
        case SOFT_UPDATE -> {
          writer.softUpdateDocument(new Term("id", id), doc(id), softDelete);
          model.versions.merge(id, 1, Integer::sum);
          model.live.put(id, 1);
        }
        case SOFT_DELETE -> {
          writer.updateDocValues(new Term("id", id), softDelete);
          model.live.put(id, 0);
        }
        case UNDELETE -> {
          undelete(writer, id);
          model.live.put(id, model.versions.getOrDefault(id, 0));
          undeletes++;
        }
        case HARD_DELETE -> {
          writer.deleteDocuments(new Term("id", id));
          model.versions.put(id, 0);
          model.live.put(id, 0);
        }
        case ADD_TOMBSTONE -> {
          String newId = "t" + nextId++;
          Document tombstone = doc(newId);
          tombstone.add(softDelete);
          writer.addDocument(tombstone);
          model.versions.put(newId, 1);
          model.live.put(newId, 0);
        }
      }
    }

    /** Makes up to {@link #MAX_CHANGES_AFTER_READER} random changes. */
    private void changesAfterReader() throws IOException {
      int numChanges = random().nextInt(MAX_CHANGES_AFTER_READER + 1);
      for (int i = 0; i < numChanges; i++) {
        change();
      }
    }

    /**
     * Opens a reader, see {@link ReaderKind}, and a new writer on the reader's commit. Then checks
     * that the new writer has the documents the reader saw (rule 2).
     */
    private void openWriterFromReader() throws IOException {
      // each of these closes the writer, and sets the model to the documents the reader saw
      DirectoryReader reader =
          switch (pick(random(), ReaderKind.values())) {
            case NRT -> openNrtReader(true);
            case NRT_WITHOUT_APPLYING_DELETES -> openNrtReader(false);
            case LATEST_COMMIT -> openLatestCommit();
            case RANDOM_COMMIT -> openRandomCommit();
            case NRT_REOPENED_ON_COMMIT -> reopenNrtReaderOnCommit();
          };
      writer = new IndexWriter(dir, newConfig().setIndexCommit(reader.getIndexCommit()));
      reader.close();
      writersFromReaders++;
      checkWriter(true);
    }

    private DirectoryReader openNrtReader(boolean applyAllDeletes) throws IOException {
      boolean writeAllDeletes = applyAllDeletes && random().nextBoolean();
      DirectoryReader reader = DirectoryReader.open(writer, applyAllDeletes, writeAllDeletes);
      Docs seen;
      if (applyAllDeletes) {
        seen = model.copy();
        assertDocs(seen, readDocs(reader));
      } else {
        // the reader may still see documents that are deleted
        seen = readDocs(reader);
      }
      changesAfterReader();
      if (random().nextBoolean()) {
        writer.rollback();
      } else {
        closeWriter();
      }
      model = seen;
      return reader;
    }

    private DirectoryReader openLatestCommit() throws IOException {
      closeWriter();
      DirectoryReader reader = DirectoryReader.open(dir);
      assertDocs(model, readDocs(reader));
      return reader;
    }

    private DirectoryReader openRandomCommit() throws IOException {
      closeWriter();
      List<IndexCommit> commitPoints = DirectoryReader.listCommits(dir);
      IndexCommit commit = commitPoints.get(random().nextInt(commitPoints.size()));
      DirectoryReader reader = DirectoryReader.open(commit);
      model = commits.get(commit.getGeneration()).copy();
      assertDocs(model, readDocs(reader));
      return reader;
    }

    private DirectoryReader reopenNrtReaderOnCommit() throws IOException {
      DirectoryReader nrtReader = DirectoryReader.open(writer);
      // the writer commits these changes when it closes, so they aren't lost
      changesAfterReader();
      closeWriter();
      List<IndexCommit> commitPoints = DirectoryReader.listCommits(dir);
      DirectoryReader reader =
          DirectoryReader.openIfChanged(nrtReader, commitPoints.get(commitPoints.size() - 1));
      if (reader == null) {
        reader = nrtReader;
      } else {
        nrtReader.close();
      }
      assertNull(((StandardDirectoryReader) reader).writer);
      assertDocs(model, readDocs(reader));
      return reader;
    }

    /** Closes the writer, which commits. */
    private void closeWriter() throws IOException {
      writer.close();
      recordCommit();
    }

    private void recordCommit() throws IOException {
      commits.put(SegmentInfos.readLatestCommit(dir).getGeneration(), model.copy());
    }

    /**
     * Checks that the writer has the documents of the model (rule 1). With {@code checkDocStats},
     * also checks {@link IndexWriter#getDocStats}. That only works before the writer buffers any
     * changes: soft deletes that a merge carries over aren't counted until they are written.
     */
    private void checkWriter(boolean checkDocStats) throws IOException {
      if (checkDocStats) {
        assertEquals(model.numDocs(), writer.getDocStats().numDocs);
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertDocs(model, readDocs(reader));
      }
    }

    /** Compares the versions of each id only if merges keep soft deleted documents. */
    private void assertDocs(Docs expected, Docs actual) {
      Docs a = expected.copy();
      Docs b = actual.copy();
      a.normalize();
      b.normalize();
      if (retainAll) {
        assertEquals("versions", a.versions, b.versions);
      }
      assertEquals("live", a.live, b.live);
    }
  }

  private static Document doc(String id) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    return doc;
  }

  /**
   * Reads the documents of each id from the hard live docs and the soft deletes field. For an NRT
   * reader, also checks that its live docs and numDocs agree with them (rule 3).
   */
  private static Docs readDocs(DirectoryReader reader) throws IOException {
    boolean nrt = ((StandardDirectoryReader) reader).writer != null;
    Docs docs = new Docs();
    int numLive = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      SegmentReader segmentReader = (SegmentReader) ctx.reader();
      Bits hardLiveDocs = segmentReader.getHardLiveDocs();
      FixedBitSet softDeleted = new FixedBitSet(segmentReader.maxDoc());
      DocIdSetIterator it =
          FieldExistsQuery.getDocValuesDocIdSetIterator(SOFT_DELETES_FIELD, segmentReader);
      if (it != null) {
        softDeleted.or(it);
      }
      Bits liveDocs = segmentReader.getLiveDocs();
      StoredFields storedFields = segmentReader.storedFields();
      int segmentNumLive = 0;
      for (int doc = 0; doc < segmentReader.maxDoc(); doc++) {
        boolean hardLive = hardLiveDocs == null || hardLiveDocs.get(doc);
        boolean live = hardLive && softDeleted.get(doc) == false;
        if (nrt) {
          boolean readerLive = liveDocs == null || liveDocs.get(doc);
          assertEquals("segment=" + segmentReader + " doc=" + doc, live, readerLive);
        }
        if (hardLive) {
          String id = storedFields.document(doc).get("id");
          docs.versions.merge(id, 1, Integer::sum);
          if (live) {
            docs.live.merge(id, 1, Integer::sum);
            segmentNumLive++;
          }
        }
      }
      if (nrt) {
        assertEquals("segment=" + segmentReader, segmentNumLive, segmentReader.numDocs());
      }
      numLive += segmentNumLive;
    }
    if (nrt) {
      assertEquals(numLive, reader.numDocs());
    }
    return docs;
  }

  /**
   * Undeletes the versions of id that aren't hard deleted. Lucene has no undelete by term:
   * updateDocValues skips documents that are already soft deleted, so this uses tryUpdateDocValue.
   */
  private static void undelete(IndexWriter writer, String id) throws IOException {
    int attempts = 0;
    while (tryUndelete(writer, id) == false) {
      // a merge replaced a segment of the reader, so try again with a new reader
      if (++attempts == MAX_UNDELETE_ATTEMPTS) {
        fail("couldn't undelete " + id + ": merges kept replacing its segments");
      }
    }
    // Write the reset right away. Until it's written, a later soft delete of the same document
    // still sees the document as deleted, and is lost.
    writer.flush();
  }

  private static boolean tryUndelete(IndexWriter writer, String id) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      for (LeafReaderContext ctx : reader.leaves()) {
        SegmentReader segmentReader = (SegmentReader) ctx.reader();
        Bits hardLiveDocs = segmentReader.getHardLiveDocs();
        // the postings include deleted documents, so skip the hard deleted ones
        PostingsEnum postings = segmentReader.postings(new Term("id", id));
        if (postings == null) {
          continue;
        }
        for (int doc = postings.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = postings.nextDoc()) {
          if ((hardLiveDocs == null || hardLiveDocs.get(doc))
              && writer.tryUpdateDocValue(
                      segmentReader, doc, new NumericDocValuesField(SOFT_DELETES_FIELD, null))
                  == -1) {
            return false;
          }
        }
      }
      return true;
    }
  }
}
