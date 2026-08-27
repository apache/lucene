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
package org.apache.lucene.misc.index;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestBalancedSegmentsMergePolicy extends LuceneTestCase {

  private static final int DOCS = 20000;
  private static final int PARTS = 4;

  /**
   * The property the policy exists for: forcing a merge to N segments gives N segments of about the
   * same size, where an ordinary forced merge says nothing about how the documents are shared.
   */
  public void testForcedMergeIsBalanced() throws Exception {
    int[] balanced = forceMergeInto(true);
    int[] ordinary = forceMergeInto(false);

    assertEquals("expected " + PARTS + " segments", PARTS, balanced.length);
    int total = 0;
    int largest = 0;
    for (int docs : balanced) {
      total += docs;
      largest = Math.max(largest, docs);
    }
    assertEquals("documents were lost", DOCS, total);
    // An even share, give or take rounding.
    assertTrue(
        "largest segment holds " + largest + " of " + total + ", expected about " + total / PARTS,
        largest <= (double) total / PARTS * 1.25 + 1);

    int ordinaryLargest = 0;
    for (int docs : ordinary) {
      ordinaryLargest = Math.max(ordinaryLargest, docs);
    }
    assertTrue(
        "expected a more even split than an ordinary forced merge, got "
            + largest
            + " against "
            + ordinaryLargest,
        largest <= ordinaryLargest);
  }

  /** Forcing a merge again does nothing, rather than rewriting the index to no effect. */
  public void testSecondForcedMergeIsANoOp() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config(true))) {
        index(w);
        w.forceMerge(PARTS, true);
      }
      final long filesAfterFirst;
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        filesAfterFirst = r.leaves().stream().mapToLong(c -> c.reader().maxDoc()).sum();
        assertEquals(PARTS, r.leaves().size());
      }
      try (IndexWriter w = new IndexWriter(dir, config(true))) {
        w.forceMerge(PARTS, true);
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(PARTS, r.leaves().size());
        assertEquals(
            filesAfterFirst, r.leaves().stream().mapToLong(c -> c.reader().maxDoc()).sum());
      }
    }
  }

  /** forceMerge(1) still means one segment. */
  public void testForceMergeToOneIsUnchanged() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config(true))) {
        index(w);
        w.forceMerge(1, true);
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(1, r.leaves().size());
        assertEquals(DOCS, r.numDocs());
      }
    }
  }

  /** A merge may have a single input: forcing a one-segment index to n splits it. */
  public void testSplitsASingleSegment() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config(true))) {
        index(w);
        w.forceMerge(1, true);
        w.forceMerge(PARTS, true);
      }
      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(PARTS, r.leaves().size());
        assertEquals(DOCS, r.numDocs());
        for (LeafReaderContext ctx : r.leaves()) {
          assertEquals(DOCS / PARTS, ctx.reader().maxDoc());
        }
      }
    }
  }

  /** Balancing is planned as several independent merges, not one merge of everything. */
  public void testPlansSeveralMerges() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config(NoMergePolicy.INSTANCE))) {
        for (int s = 0; s < 40; s++) {
          for (int i = 0; i < 500; i++) {
            Document d = new Document();
            d.add(new StringField("id", s + "-" + i, Field.Store.YES));
            w.addDocument(d);
          }
          w.flush();
        }
      }
      MergePolicy policy = new BalancedSegmentsMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, config(policy))) {
        w.forceMerge(PARTS, true);
      }
      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals(PARTS, r.leaves().size());
        assertEquals(20000, r.numDocs());
        for (LeafReaderContext ctx : r.leaves()) {
          assertEquals(5000, ctx.reader().maxDoc());
        }
      }
    }
  }

  /** Deleted documents do not count towards a share, so the outputs are even in live documents. */
  public void testSharesLiveDocumentsEvenly() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config(true))) {
        index(w);
        // Delete most of the first half, so equal ranges of documents would be very uneven.
        for (int i = 0; i < DOCS / 2; i++) {
          if (i % 4 != 0) {
            w.deleteDocuments(new Term("id", "" + i));
          }
        }
        w.commit();
        w.forceMerge(PARTS, true);
      }
      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        int total = r.numDocs();
        int largest = 0;
        for (LeafReaderContext ctx : r.leaves()) {
          largest = Math.max(largest, ctx.reader().numDocs());
        }
        assertTrue(
            "largest holds " + largest + " of " + total,
            largest <= (double) total / r.leaves().size() * 1.25 + 1);
      }
    }
  }

  private int[] forceMergeInto(boolean balanced) throws Exception {
    try (Directory dir = newDirectory()) {
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, config(balanced))) {
        expected.addAll(index(w));
        w.forceMerge(PARTS, true);
      }
      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        Set<String> found = new HashSet<>();
        for (LeafReaderContext ctx : r.leaves()) {
          StoredFields stored = ctx.reader().storedFields();
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            found.add(stored.document(doc).get("id"));
          }
        }
        assertEquals("documents were lost or duplicated", expected, found);
        return r.leaves().stream().mapToInt(c -> c.reader().maxDoc()).toArray();
      }
    }
  }

  private static Set<String> index(IndexWriter w) throws Exception {
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < DOCS; i++) {
      Document d = new Document();
      d.add(new StringField("id", "" + i, Field.Store.YES));
      w.addDocument(d);
      ids.add("" + i);
    }
    w.commit();
    return ids;
  }

  private static IndexWriterConfig config(boolean balanced) {
    return config(
        balanced
            ? new BalancedSegmentsMergePolicy(new TieredMergePolicy())
            : new TieredMergePolicy());
  }

  private static IndexWriterConfig config(MergePolicy policy) {
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Many flushes, so a forced merge has something uneven to work with. No index sort: the policy
    // shares documents out by position and needs none.
    iwc.setMaxBufferedDocs(1000);
    iwc.setMergePolicy(policy);
    return iwc;
  }
}
