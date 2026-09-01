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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Tests the incremental doc-values update path ({@link IndexWriterConfig#setMaxDocValuesOverlays}),
 * where a set-only update is stored as a sparse delta overlaid on the base column at read time and
 * the overlays are folded once they exceed the configured maximum.
 */
public class TestIncrementalDocValuesUpdates extends LuceneTestCase {

  private IndexWriterConfig incrementalConfig() {
    return new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxDocValuesOverlays(TestUtil.nextInt(random(), 1, 6));
  }

  private static void assertNumeric(IndexReader reader, String id, long expected)
      throws IOException {
    assertNumericField(reader, id, "val", expected);
  }

  private static void assertNumericField(IndexReader reader, String id, String field, long expected)
      throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      TermsEnum te = ctx.reader().terms("id").iterator();
      if (te.seekExact(new BytesRef(id))) {
        PostingsEnum pe = te.postings(null);
        int doc = pe.nextDoc();
        NumericDocValues dv = ctx.reader().getNumericDocValues(field);
        assertTrue(id + "." + field, dv.advanceExact(doc));
        assertEquals(id + "." + field, expected, dv.longValue());
        return;
      }
    }
    fail("id not found: " + id);
  }

  /** Random set-only updates across many docs and fields, interleaved with reopens and merges. */
  public void testRandomizedSetOnlyUpdates() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      int numDocs = atLeast(50);
      Map<String, Long> expected = new HashMap<>();
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        expected.put("d" + i, (long) i);
        w.addDocument(d);
      }
      int updates = atLeast(200);
      for (int i = 0; i < updates; i++) {
        String id = "d" + random().nextInt(numDocs);
        long v = random().nextLong();
        w.updateNumericDocValue(new Term("id", id), "val", v);
        expected.put(id, v);
        if (rarely()) {
          w.commit();
        }
        if (rarely()) {
          w.forceMerge(1 + random().nextInt(3));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        for (Map.Entry<String, Long> e : expected.entrySet()) {
          assertNumeric(reader, e.getKey(), e.getValue());
        }
      }
    }
  }

  /**
   * A merge flattens the overlay back into a single dense column that still reads the latest
   * values.
   */
  public void testMergeFlattensOverlay() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      for (int i = 0; i < 20; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        w.addDocument(d);
      }
      for (int round = 0; round < 10; round++) {
        for (int i = 0; i < 20; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", 1000L + round * 100 + i);
        }
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertEquals(1, reader.leaves().size());
        for (int i = 0; i < 20; i++) {
          assertNumeric(reader, "d" + i, 1000L + 9 * 100 + i);
        }
      }
    }
  }

  /**
   * Removing a value (null update) falls back to the dense rewrite and is observed as "no value".
   */
  public void testResetRemovesValue() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      Document d = new Document();
      d.add(new StringField("id", "d0", StringField.Store.NO));
      d.add(new NumericDocValuesField("val", 5));
      w.addDocument(d);
      w.updateNumericDocValue(new Term("id", "d0"), "val", 7);
      w.updateDocValues(new Term("id", "d0"), new NumericDocValuesField("val", null));
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReaderContext ctx = reader.leaves().get(0);
        NumericDocValues dv = ctx.reader().getNumericDocValues("val");
        assertFalse("value should have been removed", dv.advanceExact(0));
      }
    }
  }

  /**
   * An index whose updates were written with the feature disabled keeps working when it is enabled.
   */
  public void testContinuesIndexWrittenWithFeatureDisabled() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir, new IndexWriterConfig(new MockAnalyzer(random())).setMaxDocValuesOverlays(0))) {
        for (int i = 0; i < 10; i++) {
          Document d = new Document();
          d.add(new StringField("id", "d" + i, StringField.Store.NO));
          d.add(new NumericDocValuesField("val", i));
          w.addDocument(d);
        }
        // dense update generations, written by the classic path
        for (int i = 0; i < 10; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", 100L + i);
        }
        w.commit();
      }
      // reopen with the feature enabled and stack sparse deltas on top of the dense generations
      try (IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
        for (int i = 0; i < 10; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", 200L + i);
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (int i = 0; i < 10; i++) {
            assertNumeric(reader, "d" + i, 200L + i);
          }
        }
      }
    }
  }

  /**
   * Repeatedly updating the whole corpus makes the delta generations cover the entire column; the
   * writer then folds back to a single dense column (reclaiming the base) instead of overlaying an
   * ever-denser delta. Checked here only for value correctness across the transition.
   */
  public void testFoldsToDenseOnFullCorpusUpdates() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random())).setMaxDocValuesOverlays(2))) {
      int numDocs = 40;
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        w.addDocument(d);
      }
      long expected = 0;
      for (int round = 0; round < 12; round++) {
        expected = 1000L + round;
        for (int i = 0; i < numDocs; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", expected + i);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        for (int i = 0; i < numDocs; i++) {
          assertNumeric(reader, "d" + i, expected + i);
        }
      }
    }
  }

  /**
   * Several updatable fields on the same docs, updated independently. Each field's overlay is
   * tracked separately.
   */
  public void testMultipleUpdatableFields() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      int numDocs = atLeast(40);
      int numFields = 3;
      long[][] expected = new long[numDocs][numFields];
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        for (int f = 0; f < numFields; f++) {
          long v = i * 10L + f;
          d.add(new NumericDocValuesField("f" + f, v));
          expected[i][f] = v;
        }
        w.addDocument(d);
      }
      int updates = atLeast(300);
      for (int u = 0; u < updates; u++) {
        int i = random().nextInt(numDocs);
        int f = random().nextInt(numFields);
        long v = random().nextLong();
        w.updateNumericDocValue(new Term("id", "d" + i), "f" + f, v);
        expected[i][f] = v;
        if (rarely()) {
          w.commit();
        }
        if (rarely()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        for (int i = 0; i < numDocs; i++) {
          for (int f = 0; f < numFields; f++) {
            assertNumericField(reader, "d" + i, "f" + f, expected[i][f]);
          }
        }
      }
    }
  }

  /**
   * Updates interleaved with deletes: deleted docs drop out, surviving docs keep their latest
   * value.
   */
  public void testUpdatesInterleavedWithDeletes() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      int numDocs = atLeast(60);
      Map<String, Long> expected = new HashMap<>();
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        expected.put("d" + i, (long) i);
        w.addDocument(d);
      }
      int ops = atLeast(300);
      for (int o = 0; o < ops; o++) {
        String id = "d" + random().nextInt(numDocs);
        if (expected.containsKey(id) && random().nextInt(6) == 0) {
          w.deleteDocuments(new Term("id", id));
          expected.remove(id);
        } else if (expected.containsKey(id)) {
          long v = random().nextLong();
          w.updateNumericDocValue(new Term("id", id), "val", v);
          expected.put(id, v);
        }
        if (rarely()) {
          w.commit();
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        for (Map.Entry<String, Long> e : expected.entrySet()) {
          assertNumeric(reader, e.getKey(), e.getValue());
        }
      }
    }
  }

  /**
   * Doc-values updates on an index that is sorted on a different field: overlay docs are in the
   * sorted order.
   */
  public void testSortedIndexWithUpdates() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf =
          incrementalConfig().setIndexSort(new Sort(new SortField("sortkey", SortField.Type.LONG)));
      try (IndexWriter w = new IndexWriter(dir, conf)) {
        int numDocs = atLeast(40);
        Map<String, Long> expected = new HashMap<>();
        for (int i = 0; i < numDocs; i++) {
          Document d = new Document();
          d.add(new StringField("id", "d" + i, StringField.Store.NO));
          d.add(new NumericDocValuesField("sortkey", random().nextInt(1000)));
          d.add(new NumericDocValuesField("val", i));
          expected.put("d" + i, (long) i);
          w.addDocument(d);
        }
        for (int u = 0; u < atLeast(200); u++) {
          String id = "d" + random().nextInt(numDocs);
          long v = random().nextLong();
          w.updateNumericDocValue(new Term("id", id), "val", v);
          expected.put(id, v);
          if (rarely()) {
            w.commit();
          }
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (Map.Entry<String, Long> e : expected.entrySet()) {
            assertNumeric(reader, e.getKey(), e.getValue());
          }
        }
      }
    }
  }

  /**
   * addIndexes(CodecReader...) flattens the source overlay into the destination, preserving the
   * latest values.
   */
  public void testAddIndexesFromUpdatedIndex() throws Exception {
    try (Directory src = newDirectory();
        Directory dst = newDirectory()) {
      Map<String, Long> expected = new HashMap<>();
      try (IndexWriter w = new IndexWriter(src, incrementalConfig())) {
        int numDocs = atLeast(30);
        for (int i = 0; i < numDocs; i++) {
          Document d = new Document();
          d.add(new StringField("id", "d" + i, StringField.Store.NO));
          d.add(new NumericDocValuesField("val", i));
          expected.put("d" + i, (long) i);
          w.addDocument(d);
        }
        for (int u = 0; u < atLeast(150); u++) {
          String id = "d" + random().nextInt(numDocs);
          long v = random().nextLong();
          w.updateNumericDocValue(new Term("id", id), "val", v);
          expected.put(id, v);
        }
        w.commit();
      }
      try (IndexWriter w = new IndexWriter(dst, incrementalConfig());
          DirectoryReader src2 = DirectoryReader.open(src)) {
        CodecReader[] readers = new CodecReader[src2.leaves().size()];
        for (int i = 0; i < readers.length; i++) {
          readers[i] = (CodecReader) src2.leaves().get(i).reader();
        }
        w.addIndexes(readers);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (Map.Entry<String, Long> e : expected.entrySet()) {
            assertNumeric(reader, e.getKey(), e.getValue());
          }
        }
      }
    }
  }

  public void testBinarySetOnlyUpdates() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      int numDocs = atLeast(30);
      Map<String, String> expected = new HashMap<>();
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new BinaryDocValuesField("val", new BytesRef("v" + i)));
        expected.put("d" + i, "v" + i);
        w.addDocument(d);
      }
      int updates = atLeast(120);
      for (int i = 0; i < updates; i++) {
        String id = "d" + random().nextInt(numDocs);
        String v = "u" + random().nextInt(1_000_000);
        w.updateBinaryDocValue(new Term("id", id), "val", new BytesRef(v));
        expected.put(id, v);
        if (rarely()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        for (Map.Entry<String, String> e : expected.entrySet()) {
          boolean found = false;
          for (LeafReaderContext ctx : reader.leaves()) {
            TermsEnum te = ctx.reader().terms("id").iterator();
            if (te.seekExact(new BytesRef(e.getKey()))) {
              int doc = te.postings(null).nextDoc();
              BinaryDocValues dv = ctx.reader().getBinaryDocValues("val");
              assertTrue(e.getKey(), dv.advanceExact(doc));
              assertEquals(e.getKey(), new BytesRef(e.getValue()), dv.binaryValue());
              found = true;
              break;
            }
          }
          assertTrue("id not found: " + e.getKey(), found);
        }
      }
    }
  }

  /**
   * Doc-values updates on a skip-indexed field are rejected up front by {@link IndexWriter}. That
   * pre-existing restriction is what lets the overlay assume a field's skipper always has a single
   * producer (skippers are never overlaid), so the incremental path needs no special handling for
   * them.
   */
  public void testCannotUpdateSkipIndexedField() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      Document d = new Document();
      d.add(new StringField("id", "0", StringField.Store.NO));
      d.add(NumericDocValuesField.indexedField("val", 1L));
      w.addDocument(d);
      w.commit();
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () -> w.updateNumericDocValue(new Term("id", "0"), "val", 2L));
      assertTrue(e.getMessage(), e.getMessage().contains("doc values skip index"));
    }
  }

  /**
   * A reader that predates the feature rejects the index (before any codec) rather than misreading
   * a delta as the whole column, and the overlay round-trips through the segments file.
   */
  public void testOverlaySegmentRejectedByOlderReaders() throws Exception {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, incrementalConfig())) {
      Document d = new Document();
      d.add(new StringField("id", "0", StringField.Store.NO));
      d.add(new NumericDocValuesField("val", 1L));
      w.addDocument(d);
      w.commit();
      w.updateNumericDocValue(new Term("id", "0"), "val", 2L); // writes a sparse overlay generation
      w.commit();
    }
    // The commit records overlay generations, so its segments file is written at VERSION_10_6; a
    // reader that only understands up to VERSION_86 rejects it with IndexFormatTooNewException.
    assertOldReaderRejects(dir);
    // And the current reader sees the overlay round-tripped through the segments file.
    assertTrue(
        SegmentInfos.readLatestCommit(dir).asList().stream()
            .anyMatch(SegmentCommitInfo::hasDocValuesOverlays));
    dir.close();
  }

  /**
   * After the deltas first fold back to a dense column, further updates must keep writing sparse
   * deltas over that dense base rather than degrading to a full-column rewrite on every fold.
   */
  public void testSparseFoldOverDenseBase() throws Exception {
    int numDocs = 10;
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxDocValuesOverlays(1)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        w.addDocument(d);
      }
      w.commit();
      // Update every doc twice: the second round's coverage reaches maxDoc and folds to a dense
      // column, so the field's base becomes a dense generation rather than the core column.
      for (int round = 1; round <= 2; round++) {
        for (int i = 0; i < numDocs; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", 100L * round + i);
        }
        w.commit();
      }
      // Now keep updating a single doc across two more folds over the dense base.
      for (int round = 0; round < 2; round++) {
        w.updateNumericDocValue(new Term("id", "d0"), "val", 999L + round);
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
        assertNumeric(reader, "d0", 1000L);
        for (int i = 1; i < numDocs; i++) {
          assertNumeric(reader, "d" + i, 200L + i);
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  /**
   * Soft deletes are numeric doc-values updates on the soft-deletes field, so they ride the overlay
   * path too. Marking docs across many commits folds that field's overlay (and folds it to a dense
   * column once coverage crosses the threshold); liveness and the surviving values must stay
   * correct throughout.
   */
  public void testSoftDeletesOverOverlay() throws Exception {
    String softField = "__soft_deletes";
    int numDocs = 20;
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                    .setSoftDeletesField(softField)
                    .setMaxDocValuesOverlays(2)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int i = 0; i < numDocs; i++) {
        Document d = new Document();
        d.add(new StringField("id", "d" + i, StringField.Store.NO));
        d.add(new NumericDocValuesField("val", i));
        w.addDocument(d);
      }
      w.commit();
      // Soft-delete every even doc, one commit each, so the soft-deletes field accrues deltas that
      // fold and eventually fold to a dense column (coverage reaches half the segment).
      for (int i = 0; i < numDocs; i += 2) {
        w.updateDocValues(new Term("id", "d" + i), new NumericDocValuesField(softField, 1L));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertEquals(numDocs / 2, reader.numDocs());
        for (int i = 1; i < numDocs; i += 2) {
          assertNumeric(reader, "d" + i, i);
        }
      }
      TestUtil.checkIndex(dir);
    }
  }

  /**
   * addIndexes(Directory...) copies segments as-is via copySegmentAsIs, so the copied segment must
   * keep its doc-values overlay rather than flatten it the way the CodecReader path does.
   */
  public void testAddIndexesDirectoryCarriesOverlay() throws Exception {
    try (Directory src = newDirectory();
        Directory dst = newDirectory()) {
      int numDocs = 10;
      try (IndexWriter w =
          new IndexWriter(src, incrementalConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < numDocs; i++) {
          Document d = new Document();
          d.add(new StringField("id", "d" + i, StringField.Store.NO));
          d.add(new NumericDocValuesField("val", i));
          w.addDocument(d);
        }
        w.commit();
        for (int i = 0; i < numDocs; i++) {
          w.updateNumericDocValue(new Term("id", "d" + i), "val", 100L + i);
        }
        w.commit();
        assertTrue("source segment should carry an overlay", hasOverlay(src));
      }
      try (IndexWriter w =
          new IndexWriter(
              dst,
              new IndexWriterConfig(new MockAnalyzer(random()))
                  .setMergePolicy(NoMergePolicy.INSTANCE))) {
        w.addIndexes(src);
        w.commit();
        assertTrue("copied segment should still carry the overlay", hasOverlay(dst));
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (int i = 0; i < numDocs; i++) {
            assertNumeric(reader, "d" + i, 100L + i);
          }
        }
      }
      TestUtil.checkIndex(dst);
    }
  }

  private static boolean hasOverlay(Directory dir) throws IOException {
    for (SegmentCommitInfo si : SegmentInfos.readLatestCommit(dir)) {
      if (si.getDocValuesOverlays().isEmpty() == false) {
        return true;
      }
    }
    return false;
  }

  private static void assertOldReaderRejects(Directory dir) throws IOException {
    String segmentsFile = SegmentInfos.getLastCommitSegmentsFileName(dir);
    try (ChecksumIndexInput in = dir.openChecksumInput(segmentsFile)) {
      assertEquals(CodecUtil.CODEC_MAGIC, CodecUtil.readBEInt(in));
      expectThrows(
          IndexFormatTooNewException.class,
          () ->
              CodecUtil.checkHeaderNoMagic(
                  in, "segments", SegmentInfos.VERSION_74, SegmentInfos.VERSION_86));
    }
  }
}
