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
 * Randomly opens writers from the commits of NRT readers and readers opened from disk, with soft
 * deletes, and checks that the new writer has exactly the documents the reader saw.
 */
public class TestSoftDeletesFromReaderInvariants extends LuceneTestCase {

  private static final String SOFT = "soft_delete";

  /**
   * Whether merges keep soft deleted docs. Only then can soft deleted docs be undeleted, and only
   * then do the versions of an id stay the same across merges.
   */
  private boolean retainAll;

  /** Per id: docs that aren't hard deleted, and docs that are neither hard nor soft deleted. */
  private static final class State {
    final Map<String, Integer> versions = new TreeMap<>();
    final Map<String, Integer> live = new TreeMap<>();

    State copy() {
      State s = new State();
      s.versions.putAll(versions);
      s.live.putAll(live);
      return s;
    }

    int numDocs() {
      return live.values().stream().mapToInt(Integer::intValue).sum();
    }

    void normalize() {
      versions.values().removeIf(v -> v == 0);
      live.values().removeIf(v -> v == 0);
    }

    @Override
    public String toString() {
      State a = copy();
      a.normalize();
      return "versions=" + a.versions + " live=" + a.live;
    }
  }

  private static IndexWriterConfig iwc(boolean retainAll) {
    MergePolicy mp = newMergePolicy();
    if (retainAll) {
      mp = new SoftDeletesRetentionMergePolicy(SOFT, () -> MatchAllDocsQuery.INSTANCE, mp);
    }
    return newIndexWriterConfig()
        .setSoftDeletesField(SOFT)
        .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE)
        .setMergePolicy(mp);
  }

  private static Document doc(String id) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    return doc;
  }

  /**
   * Reads the state from hard live docs and soft deletes doc values, and checks that an NRT
   * reader's live docs and numDocs agree with them.
   */
  private static State readState(DirectoryReader r) throws IOException {
    boolean nrt = ((StandardDirectoryReader) r).writer != null;
    State s = new State();
    int totalLive = 0;
    for (LeafReaderContext ctx : r.leaves()) {
      SegmentReader sr = (SegmentReader) ctx.reader();
      Bits hard = sr.getHardLiveDocs();
      FixedBitSet soft = new FixedBitSet(sr.maxDoc());
      DocIdSetIterator it = FieldExistsQuery.getDocValuesDocIdSetIterator(SOFT, sr);
      if (it != null) {
        soft.or(it);
      }
      Bits liveDocs = sr.getLiveDocs();
      StoredFields stored = sr.storedFields();
      int segLive = 0;
      for (int doc = 0; doc < sr.maxDoc(); doc++) {
        boolean hardLive = hard == null || hard.get(doc);
        boolean live = hardLive && soft.get(doc) == false;
        if (nrt) {
          boolean readerLive = liveDocs == null || liveDocs.get(doc);
          assertEquals("seg=" + sr + " doc=" + doc, live, readerLive);
        }
        if (hardLive) {
          String id = stored.document(doc).get("id");
          s.versions.merge(id, 1, Integer::sum);
          if (live) {
            s.live.merge(id, 1, Integer::sum);
            segLive++;
          }
        }
      }
      if (nrt) {
        assertEquals("seg=" + sr, segLive, sr.numDocs());
      }
      totalLive += segLive;
    }
    if (nrt) {
      assertEquals(totalLive, r.numDocs());
    }
    return s;
  }

  private void assertState(State expected, State actual) {
    State a = expected.copy(), b = actual.copy();
    a.normalize();
    b.normalize();
    if (retainAll) {
      assertEquals("versions", a.versions, b.versions);
    }
    assertEquals("live", a.live, b.live);
  }

  private void assertWriter(IndexWriter w, State expected, boolean checkDocStats)
      throws IOException {
    // getDocStats is checked before anything is buffered: it can be off by one later on, also
    // before this change
    if (checkDocStats) {
      assertEquals(expected.numDocs(), w.getDocStats().numDocs);
    }
    try (DirectoryReader r = DirectoryReader.open(w)) {
      assertState(expected, readState(r));
    }
  }

  private static long latestGen(Directory dir) throws IOException {
    return SegmentInfos.readLatestCommit(dir).getGeneration();
  }

  private int nextId;

  /**
   * Undeletes the versions of id that aren't hard deleted. Lucene has no undelete by term:
   * updateDocValues skips documents that are already soft deleted, so this uses tryUpdateDocValue.
   */
  private static void undelete(IndexWriter w, String id) throws IOException {
    while (tryUndelete(w, id) == false) {
      // a merge replaced a segment of the reader, so try again with a new reader
    }
    // Write the reset right away. Until it's written, a later soft delete of the same document
    // still sees the document as deleted, and is lost.
    w.flush();
  }

  private static boolean tryUndelete(IndexWriter w, String id) throws IOException {
    try (DirectoryReader r = DirectoryReader.open(w)) {
      for (LeafReaderContext ctx : r.leaves()) {
        SegmentReader sr = (SegmentReader) ctx.reader();
        Bits hardLiveDocs = sr.getHardLiveDocs();
        // the postings include deleted documents, so skip the hard deleted ones
        PostingsEnum postings = sr.postings(new Term("id", id));
        if (postings == null) {
          continue;
        }
        for (int doc = postings.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = postings.nextDoc()) {
          if ((hardLiveDocs == null || hardLiveDocs.get(doc))
              && w.tryUpdateDocValue(sr, doc, new NumericDocValuesField(SOFT, null)) == -1) {
            return false;
          }
        }
      }
      return true;
    }
  }

  /** Applies a random update op to w and the model. */
  private void randomUpdate(IndexWriter w, State m) throws IOException {
    List<String> ids = new ArrayList<>(m.versions.keySet());
    String id = ids.isEmpty() ? null : ids.get(random().nextInt(ids.size()));
    Field softDelete = new NumericDocValuesField(SOFT, 1);
    int x = random().nextInt(100);
    if (id == null || x < 30) {
      String newId = "d" + nextId++;
      w.addDocument(doc(newId));
      m.versions.put(newId, 1);
      m.live.put(newId, 1);
    } else if (x < 50) {
      w.softUpdateDocument(new Term("id", id), doc(id), softDelete);
      m.versions.merge(id, 1, Integer::sum);
      m.live.put(id, 1);
    } else if (x < 62) {
      w.updateDocValues(new Term("id", id), softDelete);
      m.live.put(id, 0);
    } else if (retainAll && x < 68) {
      undelete(w, id);
      m.live.put(id, m.versions.getOrDefault(id, 0));
    } else if (x < 75) {
      w.deleteDocuments(new Term("id", id));
      m.versions.put(id, 0);
      m.live.put(id, 0);
    } else {
      String newId = "t" + nextId++;
      Document tombstone = doc(newId);
      tombstone.add(softDelete);
      w.addDocument(tombstone);
      m.versions.put(newId, 1);
      m.live.put(newId, 0);
    }
  }

  public void testRandom() throws Exception {
    retainAll = random().nextBoolean();
    Map<Long, State> commits = new TreeMap<>();
    try (Directory dir = newDirectory()) {
      IndexWriter w = new IndexWriter(dir, iwc(retainAll));
      State m = new State();
      w.commit();
      commits.put(latestGen(dir), m.copy());
      int numOps = atLeast(300);
      int reopens = 0;
      for (int op = 0; op < numOps; op++) {
        int x = random().nextInt(100);
        if (x < 75) {
          randomUpdate(w, m);
        } else if (x < 79) {
          w.flush();
        } else if (x < 83) {
          w.commit();
          commits.put(latestGen(dir), m.copy());
          TestUtil.checkIndex(dir);
        } else if (x < 87) {
          assertWriter(w, m, false);
        } else if (x < 89) {
          if (random().nextBoolean()) {
            w.forceMerge(TestUtil.nextInt(random(), 1, 3));
          } else {
            w.forceMergeDeletes();
          }
        } else {
          reopens++;
          int mode = random().nextInt(5);
          DirectoryReader r;
          State expected;
          if (mode <= 1) {
            // NRT reader
            boolean applyAllDeletes = mode == 0 || random().nextBoolean();
            boolean writeAllDeletes = applyAllDeletes && random().nextBoolean();
            r = DirectoryReader.open(w, applyAllDeletes, writeAllDeletes);
            if (applyAllDeletes) {
              expected = m.copy();
              assertState(expected, readState(r));
            } else {
              expected = readState(r);
            }
            // changes after the reader are lost
            int lost = random().nextInt(5);
            for (int i = 0; i < lost; i++) {
              randomUpdate(w, m);
            }
            if (random().nextBoolean()) {
              w.rollback();
            } else {
              w.close();
              commits.put(latestGen(dir), m.copy());
            }
          } else if (mode == 2) {
            // latest commit from disk
            w.close();
            commits.put(latestGen(dir), m.copy());
            r = DirectoryReader.open(dir);
            expected = m.copy();
            assertState(expected, readState(r));
          } else if (mode == 3) {
            // an older commit from disk
            w.close();
            commits.put(latestGen(dir), m.copy());
            List<IndexCommit> all = DirectoryReader.listCommits(dir);
            IndexCommit commit = all.get(random().nextInt(all.size()));
            r = DirectoryReader.open(commit);
            expected = commits.get(commit.getGeneration()).copy();
            assertState(expected, readState(r));
          } else {
            // a disk reader that reuses the segments of an NRT reader
            DirectoryReader nrt = DirectoryReader.open(w);
            int more = random().nextInt(5);
            for (int i = 0; i < more; i++) {
              randomUpdate(w, m);
            }
            w.close();
            commits.put(latestGen(dir), m.copy());
            List<IndexCommit> all = DirectoryReader.listCommits(dir);
            r = DirectoryReader.openIfChanged(nrt, all.get(all.size() - 1));
            if (r == null) {
              r = nrt;
            } else {
              nrt.close();
            }
            assertNull(((StandardDirectoryReader) r).writer);
            expected = m.copy();
            assertState(expected, readState(r));
          }
          w = new IndexWriter(dir, iwc(retainAll).setIndexCommit(r.getIndexCommit()));
          r.close();
          m = expected;
          assertWriter(w, m, true);
        }
      }
      if (VERBOSE) {
        System.out.println("reopens=" + reopens + " retainAll=" + retainAll);
      }
      assertWriter(w, m, false);
      w.forceMerge(1);
      assertWriter(w, m, false);
      w.close();
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertState(m, readState(r));
      }
    }
  }
}
