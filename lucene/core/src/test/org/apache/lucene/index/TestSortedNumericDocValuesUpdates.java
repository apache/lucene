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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Tests in-place updates of SORTED_NUMERIC doc-values. An update sets a single value per matched
 * document, replacing whatever value(s) that document had; the field itself may be single- or
 * multi-valued, and documents not matched by the update keep their existing values. A single-valued
 * column is stored as a numeric column and reuses the numeric update machinery; a multi-valued
 * column is preserved and merged per-doc.
 */
public class TestSortedNumericDocValuesUpdates extends LuceneTestCase {

  private Document doc(int id, long... values) {
    Document doc = new Document();
    doc.add(new StringField("id", "doc-" + id, Store.NO));
    for (long v : values) {
      doc.add(new SortedNumericDocValuesField("val", v));
    }
    return doc;
  }

  private IndexWriterConfig conf() {
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // exercise both the classic dense rewrite and the sparse-delta overlay
    conf.setMaxDocValuesOverlays(random().nextBoolean() ? 0 : 1 + random().nextInt(4));
    return conf;
  }

  public void testSimpleReplace() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 3));
    writer.addDocument(doc(1, 5));
    writer.commit();

    writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 9);
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> values = collect(reader);
      assertArrayEquals(new long[] {9}, values.get("doc-0"));
      assertArrayEquals(new long[] {5}, values.get("doc-1"));
    }
    IOUtils.close(writer, dir);
  }

  public void testUpdateViaFieldsApi() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1));
    writer.commit();

    writer.updateDocValues(new Term("id", "doc-0"), new SortedNumericDocValuesField("val", 42));
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertArrayEquals(new long[] {42}, collect(reader).get("doc-0"));
    }
    IOUtils.close(writer, dir);
  }

  public void testMultipleFieldsSameNameLastWins() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1));
    writer.commit();
    // two same-named single-valued sorted-numeric fields in one update behave like numeric updates:
    // each is applied and the last one wins (this is not a multi-valued update).
    writer.updateDocValues(
        new Term("id", "doc-0"),
        new SortedNumericDocValuesField("val", 1),
        new SortedNumericDocValuesField("val", 2));
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertArrayEquals(new long[] {2}, collect(reader).get("doc-0"));
    }
    IOUtils.close(writer, dir);
  }

  public void testUpdateOverMultiValuedBase() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1, 2, 3)); // genuinely multi-valued base
    writer.addDocument(doc(1, 7, 8));
    writer.commit();

    // updating doc-0 to a single value succeeds even though the column is multi-valued; doc-1 keeps
    // its whole value set.
    writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 99);
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> values = collect(reader);
      assertArrayEquals(new long[] {99}, values.get("doc-0"));
      assertArrayEquals(new long[] {7, 8}, values.get("doc-1"));
    }
    IOUtils.close(writer, dir);
  }

  public void testUpdateKeepsOtherDocsMultiValuedAcrossMerge() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = conf();
    conf.setMaxBufferedDocs(2);
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < 6; i++) {
      writer.addDocument(doc(i, i * 10L, i * 10L + 1, i * 10L + 2)); // 3 values each
      if (i % 2 == 1) {
        writer.commit();
      }
    }
    // update a few docs to a single value; the rest must keep their 3-value sets, through a merge.
    writer.updateSortedNumericDocValue(new Term("id", "doc-1"), "val", 500);
    writer.updateSortedNumericDocValue(new Term("id", "doc-4"), "val", 501);
    writer.forceMerge(1);
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> values = collect(reader);
      for (int i = 0; i < 6; i++) {
        if (i == 1) {
          assertArrayEquals(new long[] {500}, values.get("doc-1"));
        } else if (i == 4) {
          assertArrayEquals(new long[] {501}, values.get("doc-4"));
        } else {
          assertArrayEquals(new long[] {i * 10L, i * 10L + 1, i * 10L + 2}, values.get("doc-" + i));
        }
      }
    }
    IOUtils.close(writer, dir);
  }

  public void testDenseRewriteOverMultiValuedBase() throws Exception {
    // maxDocValuesOverlays == 0 forces a dense full-column rewrite on every update. With a
    // multi-valued base that rewrite goes through ReadersAndUpdates.MergedSortedNumericDocValues:
    // the matched doc collapses to the single update value while every other doc keeps its whole
    // (sorted) value set read from the base column.
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random())).setMaxDocValuesOverlays(0))) {
      w.addDocument(doc(0, 5, 1, 3)); // multi-valued, unsorted input
      w.addDocument(doc(1, 9, 8));
      w.addDocument(doc(2, 2)); // single-valued
      w.commit();

      w.updateSortedNumericDocValue(new Term("id", "doc-1"), "val", 42);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        Map<String, long[]> values = collect(reader);
        assertArrayEquals(new long[] {1, 3, 5}, values.get("doc-0")); // untouched, sorted
        assertArrayEquals(new long[] {42}, values.get("doc-1")); // replaced with a single value
        assertArrayEquals(new long[] {2}, values.get("doc-2")); // untouched
      }
      // overlays are disabled, so the update produced a dense column, not a sparse delta.
      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      assertTrue(sis.info(0).getDocValuesOverlays().isEmpty());
      TestUtil.checkIndex(dir);
    }
  }

  public void testFoldToDenseOverMultiValuedBase() throws Exception {
    // Drive a fold-to-dense rewrite over a genuinely multi-valued base. With
    // maxDocValuesOverlays==1
    // the first round writes a sparse delta; once its coverage crosses
    // FOLD_TO_DENSE_COVERAGE_RATIO (0.5) the next update folds the field back to a single dense
    // column, reading the multi-valued base through MergedSortedNumericDocValues. Docs never
    // updated
    // must keep their full value sets.
    int numDocs = 10;
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxDocValuesOverlays(1)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numDocs; i++) {
        w.addDocument(doc(i, i, i + 100)); // two values each; sorted view is {i, i+100}
      }
      w.commit();

      // Round 1: update 7 of 10 docs (> 50% coverage) so the delta will trigger a fold on the next
      // write. Docs 7, 8, 9 are never touched and stay multi-valued in the base.
      Map<String, long[]> expected = new HashMap<>();
      for (int i = 0; i < numDocs; i++) {
        expected.put("doc-" + i, new long[] {i, i + 100L});
      }
      for (int i = 0; i <= 6; i++) {
        w.updateSortedNumericDocValue(new Term("id", "doc-" + i), "val", 1000L + i);
        expected.put("doc-" + i, new long[] {1000L + i});
      }
      w.commit();

      // Round 2: a single further update now folds to a dense column over the multi-valued base.
      w.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 2000L);
      expected.put("doc-0", new long[] {2000L});
      w.commit();

      // The fold collapses the field back to one dense column, clearing the sparse overlay.
      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, sis.size());
      assertTrue(
          "fold-to-dense should have cleared the overlay",
          sis.info(0).getDocValuesOverlays().isEmpty());

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Map<String, long[]> values = collect(reader);
        for (Map.Entry<String, long[]> e : expected.entrySet()) {
          assertArrayEquals(e.getKey(), e.getValue(), values.get(e.getKey()));
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testUpdateThenDelete() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1));
    writer.addDocument(doc(1, 3));
    writer.commit();

    writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 100);
    writer.deleteDocuments(new Term("id", "doc-1"));
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> values = collect(reader);
      assertArrayEquals(new long[] {100}, values.get("doc-0"));
      assertFalse(values.containsKey("doc-1"));
    }
    IOUtils.close(writer, dir);
  }

  public void testNRTReopenSeesUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1));
    try (DirectoryReader r1 = DirectoryReader.open(writer)) {
      assertArrayEquals(new long[] {1}, collect(r1).get("doc-0"));
      writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 8);
      try (DirectoryReader r2 = DirectoryReader.openIfChanged(r1)) {
        assertNotNull(r2);
        assertArrayEquals(new long[] {8}, collect(r2).get("doc-0"));
      }
    }
    IOUtils.close(writer, dir);
  }

  public void testStackedUpdatesNewestWins() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = conf();
    conf.setMaxDocValuesOverlays(4); // force the overlay path with several stacked deltas
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0, 1));
    writer.addDocument(doc(1, 2));
    writer.commit();
    for (int gen = 0; gen < 5; gen++) {
      writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", gen * 10L);
      writer.commit();
    }
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertArrayEquals(new long[] {40}, collect(reader).get("doc-0"));
      assertArrayEquals(new long[] {2}, collect(reader).get("doc-1"));
    }
    IOUtils.close(writer, dir);
  }

  public void testAcrossSegmentsAndMerge() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = conf();
    conf.setMaxBufferedDocs(2);
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < 10; i++) {
      writer.addDocument(doc(i, i));
      if (i % 2 == 1) {
        writer.commit();
      }
    }
    for (int i = 0; i < 10; i++) {
      writer.updateSortedNumericDocValue(new Term("id", "doc-" + i), "val", i * 100L);
    }
    writer.forceMerge(1);
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> values = collect(reader);
      for (int i = 0; i < 10; i++) {
        assertArrayEquals("doc-" + i, new long[] {i * 100L}, values.get("doc-" + i));
      }
    }
    IOUtils.close(writer, dir);
  }

  public void testRejectWrongType() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    writer.addDocument(doc(0, 1));
    writer.commit();
    // "val" is SORTED_NUMERIC; trying to update it as NUMERIC must fail
    expectThrows(
        IllegalArgumentException.class,
        () -> writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 5L));
    IOUtils.close(writer, dir);
  }

  public void testRejectIndexSortField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = conf();
    conf.setIndexSort(new Sort(new SortedNumericSortField("val", SortField.Type.LONG)));
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0, 1));
    writer.commit();
    expectThrows(
        IllegalArgumentException.class,
        () -> writer.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 5L));
    IOUtils.close(writer, dir);
  }

  public void testFoldsToDenseOnFullCorpusUpdates() throws Exception {
    // Mirrors TestIncrementalDocValuesUpdates#testFoldsToDenseOnFullCorpusUpdates for the
    // single-valued sorted-numeric column, which reuses the numeric overlay/fold machinery: once
    // the
    // stacked deltas cover the whole corpus the field folds back to a single dense column.
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random())).setMaxDocValuesOverlays(2))) {
      int numDocs = 40;
      for (int i = 0; i < numDocs; i++) {
        w.addDocument(doc(i, i));
      }
      long expected = 0;
      for (int round = 0; round < 12; round++) {
        expected = 1000L + round;
        for (int i = 0; i < numDocs; i++) {
          w.updateSortedNumericDocValue(new Term("id", "doc-" + i), "val", expected + i);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        Map<String, long[]> values = collect(reader);
        for (int i = 0; i < numDocs; i++) {
          assertArrayEquals(new long[] {expected + i}, values.get("doc-" + i));
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testSparseFoldOverDenseBase() throws Exception {
    // Mirrors TestIncrementalDocValuesUpdates#testSparseFoldOverDenseBase: after a fold-to-dense
    // the
    // field's base becomes a dense generation (!= -1), and further updates must keep stacking a
    // sparse overlay over that dense base rather than clearing it. The singleton wrap/unwrap of the
    // sorted-numeric fold must preserve this.
    int numDocs = 10;
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxDocValuesOverlays(1)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numDocs; i++) {
        w.addDocument(doc(i, i));
      }
      w.commit();
      // Update every doc twice: the second round's coverage reaches maxDoc and folds to a dense
      // column, so the field's base becomes a dense generation rather than the core column.
      for (int round = 1; round <= 2; round++) {
        for (int i = 0; i < numDocs; i++) {
          w.updateSortedNumericDocValue(new Term("id", "doc-" + i), "val", 100L * round + i);
        }
        w.commit();
      }
      // Now keep updating a single doc across two more folds over the dense base.
      for (int round = 0; round < 2; round++) {
        w.updateSortedNumericDocValue(new Term("id", "doc-0"), "val", 999L + round);
        w.commit();
      }
      // The last fold over the dense base must stay sparse: an overlay whose base is a dense
      // generation (!= -1) with a folded delta, not a full-column rewrite that clears the overlay.
      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, sis.size());
      Map<Integer, long[]> overlays = sis.info(0).getDocValuesOverlays();
      assertFalse("expected a sparse overlay over the dense base", overlays.isEmpty());
      long[] packed = overlays.values().iterator().next();
      assertTrue("base should be a dense generation, not the core column", packed[0] != -1);
      assertTrue("expected at least one delta over the dense base", packed.length >= 2);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Map<String, long[]> values = collect(reader);
        assertArrayEquals(new long[] {1000L}, values.get("doc-0"));
        for (int i = 1; i < numDocs; i++) {
          assertArrayEquals(new long[] {200L + i}, values.get("doc-" + i));
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf());
    int numDocs = atLeast(50);
    Map<String, long[]> expected = new HashMap<>();
    for (int i = 0; i < numDocs; i++) {
      // start single-valued so every doc can be updated in place
      long v = random().nextInt(1000);
      writer.addDocument(doc(i, v));
      expected.put("doc-" + i, new long[] {v});
      if (rarely()) {
        writer.commit();
      }
    }
    int numUpdates = atLeast(100);
    for (int u = 0; u < numUpdates; u++) {
      int id = random().nextInt(numDocs);
      long v = random().nextInt(1000);
      writer.updateSortedNumericDocValue(new Term("id", "doc-" + id), "val", v);
      expected.put("doc-" + id, new long[] {v});
      if (rarely()) {
        writer.commit();
      }
      if (rarely()) {
        writer.forceMerge(1 + random().nextInt(3));
      }
    }
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> actual = collect(reader);
      for (Map.Entry<String, long[]> e : expected.entrySet()) {
        assertArrayEquals(e.getKey(), e.getValue(), actual.get(e.getKey()));
      }
    }
    IOUtils.close(writer, dir);
  }

  public void testRandomMultiValued() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // Low overlay budget (0..2) so stacked deltas frequently fold to a dense rewrite over the
    // multi-valued base, exercising MergedSortedNumericDocValues against the model.
    conf.setMaxDocValuesOverlays(random().nextInt(3));
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = atLeast(50);
    Map<String, long[]> expected = new HashMap<>();
    for (int i = 0; i < numDocs; i++) {
      // Seed genuinely multi-valued docs (1..4 values). Many docs are never updated below, so they
      // keep their multi-valued sets and force the dense rewrites to read a multi-valued base.
      long[] vals = new long[1 + random().nextInt(4)];
      for (int j = 0; j < vals.length; j++) {
        vals[j] = random().nextInt(1000);
      }
      writer.addDocument(doc(i, vals));
      long[] sorted = vals.clone();
      Arrays.sort(sorted); // a sorted-numeric column returns its values ascending, keeping dups
      expected.put("doc-" + i, sorted);
      if (rarely()) {
        writer.commit();
      }
    }
    int numUpdates = atLeast(100);
    for (int u = 0; u < numUpdates; u++) {
      // Only update a subset of docs so a good fraction stay multi-valued in the base.
      int id = random().nextInt(numDocs);
      long v = random().nextInt(1000);
      writer.updateSortedNumericDocValue(new Term("id", "doc-" + id), "val", v);
      expected.put("doc-" + id, new long[] {v}); // an update replaces the whole set with one value
      if (rarely()) {
        writer.commit();
      }
      if (rarely()) {
        writer.forceMerge(1 + random().nextInt(3));
      }
    }
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      Map<String, long[]> actual = collect(reader);
      for (Map.Entry<String, long[]> e : expected.entrySet()) {
        assertArrayEquals(e.getKey(), e.getValue(), actual.get(e.getKey()));
      }
    }
    IOUtils.close(writer, dir);
  }

  /** Collect field "val" for all live docs, keyed by the "id" stored term. */
  private static Map<String, long[]> collect(DirectoryReader reader) throws Exception {
    Map<String, long[]> result = new TreeMap<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader lr = ctx.reader();
      SortedNumericDocValues dv = lr.getSortedNumericDocValues("val");
      Terms idTerms = lr.terms("id");
      if (idTerms == null || dv == null) {
        continue;
      }
      Map<Integer, String> idByDoc = new HashMap<>();
      TermsEnum te = idTerms.iterator();
      org.apache.lucene.util.BytesRef term;
      while ((term = te.next()) != null) {
        PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
        int d;
        while ((d = pe.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          idByDoc.put(d, term.utf8ToString());
        }
      }
      Bits liveDocs = lr.getLiveDocs();
      int doc;
      while ((doc = dv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (liveDocs != null && liveDocs.get(doc) == false) {
          continue;
        }
        int count = dv.docValueCount();
        long[] arr = new long[count];
        for (int i = 0; i < count; i++) {
          arr[i] = dv.nextValue();
        }
        result.put(idByDoc.get(doc), arr);
      }
    }
    return result;
  }
}
