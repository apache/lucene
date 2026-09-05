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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/** Tests for merges that produce several output segments. */
public class TestMultiOutputMerge extends LuceneTestCase {

  private static final int OUTPUTS = 3;
  private static final int SEGMENTS = 4;
  private static final int PER_SEGMENT = 120;
  private static final String SOFT = "soft_deleted";

  private Analyzer analyzer;
  private CountDownLatch mergeStarted;
  private CountDownLatch proceed;
  private volatile boolean enabled;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random());
    mergeStarted = new CountDownLatch(1);
    proceed = new CountDownLatch(1);
    enabled = false;
  }

  private IndexWriterConfig config() {
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    // A contiguous doc range is a contiguous key range only when sorted.
    iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.STRING)));
    iwc.setMergePolicy(new PartitioningMergePolicy());
    return iwc;
  }

  private static Document doc(String id, long val) {
    Document d = new Document();
    d.add(new StringField("id", id, Field.Store.NO));
    d.add(new SortedDocValuesField("sort", new BytesRef(id)));
    d.add(new StoredField("id", id));
    d.add(new NumericDocValuesField("val", val));
    return d;
  }

  /** A document whose vector is derived from its id, so a mismatch after a merge is detectable. */
  private static Document vectorDoc(String id, float[] vector) {
    Document d = doc(id, 0);
    d.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
    return d;
  }

  private static float[] vectorFor(int seg, int d) {
    return new float[] {seg, d, (float) (seg * PER_SEGMENT + d)};
  }

  /**
   * Every value here is derived from {@code ord}, so any value that ends up on the wrong document
   * after a partitioned merge is a mismatch rather than merely a plausible-looking number.
   */
  private static Document everyTypeDoc(String id, int ord) {
    Document d = new Document();
    d.add(new StringField("id", id, Field.Store.NO));
    d.add(new StoredField("id", id));
    d.add(new SortedDocValuesField("sort", new BytesRef(id)));

    // postings with positions, offsets and norms, plus term vectors
    FieldType text = new FieldType(TextField.TYPE_NOT_STORED);
    text.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    text.setStoreTermVectors(true);
    text.setStoreTermVectorPositions(true);
    text.setStoreTermVectorOffsets(true);
    text.freeze();
    d.add(new Field("text", textFor(ord), text));

    d.add(new NumericDocValuesField("num", ord));
    d.add(new BinaryDocValuesField("bin", new BytesRef(binFor(ord))));
    d.add(new SortedSetDocValuesField("sset", new BytesRef(binFor(ord))));
    d.add(new SortedSetDocValuesField("sset", new BytesRef("shared")));
    d.add(new SortedNumericDocValuesField("snum", ord));
    d.add(new SortedNumericDocValuesField("snum", ord + 1L));
    // a doc-values field carrying a skip index, which the range-restricting producer passes through
    d.add(NumericDocValuesField.indexedField("skip", ord));

    d.add(new IntPoint("point", ord));
    d.add(new LongPoint("point2d", ord, -ord));

    d.add(new KnnFloatVectorField("vec", floatVecFor(ord), VectorSimilarityFunction.EUCLIDEAN));
    d.add(new KnnByteVectorField("bvec", byteVecFor(ord), VectorSimilarityFunction.EUCLIDEAN));
    return d;
  }

  /** Float vectors, document by document, which assertReaderEquals does not cover. */
  private static void assertVectorsEqual(LeafReader expected, LeafReader actual)
      throws IOException {
    FloatVectorValues left = expected.getFloatVectorValues("vec");
    FloatVectorValues right = actual.getFloatVectorValues("vec");
    assertEquals("one side has vectors and the other does not", left == null, right == null);
    if (left == null) {
      return;
    }
    assertEquals("vector count", left.size(), right.size());
    assertEquals("vector dimension", left.dimension(), right.dimension());
    KnnVectorValues.DocIndexIterator l = left.iterator();
    KnnVectorValues.DocIndexIterator r = right.iterator();
    for (int doc = l.nextDoc();
        doc != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS;
        doc = l.nextDoc()) {
      assertEquals("vectors on different documents", doc, r.nextDoc());
      assertArrayEquals(
          "vector on doc " + doc, left.vectorValue(l.index()), right.vectorValue(r.index()), 0f);
    }
    assertEquals(
        "the other side has more vectors",
        KnnVectorValues.DocIndexIterator.NO_MORE_DOCS,
        r.nextDoc());
  }

  /** The ordinal an id was generated from, so a reference document can be rebuilt from it. */
  private static int ordOf(String id) {
    String[] parts = id.split("-");
    return Integer.parseInt(parts[1]) * PER_SEGMENT + Integer.parseInt(parts[2]);
  }

  private static String textFor(int ord) {
    return "shared term ord" + ord;
  }

  private static String binFor(int ord) {
    return String.format(java.util.Locale.ROOT, "bin-%06d", ord);
  }

  private static float[] floatVecFor(int ord) {
    return new float[] {ord, -ord, ord * 0.5f};
  }

  private static byte[] byteVecFor(int ord) {
    return new byte[] {(byte) ord, (byte) -ord, (byte) (ord / 2)};
  }

  private static String id(int seg, int d) {
    return String.format(java.util.Locale.ROOT, "id-%02d-%04d", seg, d);
  }

  /** Every live document's stored id. */
  private static List<String> liveIds(DirectoryReader r) throws IOException {
    List<String> out = new ArrayList<>();
    for (LeafReaderContext ctx : r.leaves()) {
      StoredFields sf = ctx.reader().storedFields();
      Bits live = ctx.reader().getLiveDocs();
      for (int d = 0; d < ctx.reader().maxDoc(); d++) {
        if (live == null || live.get(d)) {
          out.add(sf.document(d).get("id"));
        }
      }
    }
    return out;
  }

  private static void assertEachOutputSorted(DirectoryReader r) throws IOException {
    for (LeafReaderContext ctx : r.leaves()) {
      StoredFields sf = ctx.reader().storedFields();
      String prev = null;
      for (int d = 0; d < ctx.reader().maxDoc(); d++) {
        String v = sf.document(d).get("id");
        if (prev != null) {
          assertTrue("output not index-sorted: " + prev + " then " + v, v.compareTo(prev) >= 0);
        }
        prev = v;
      }
    }
  }

  /** A merge producing several outputs keeps every document exactly once. */
  public void testProducesMultipleOutputs() throws Exception {
    try (Directory dir = newDirectory()) {
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
            expected.add(id(seg, d));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);
        List<String> live = liveIds(r);
        assertEquals(expected.size(), live.size());
        assertEquals(expected, new HashSet<>(live));
        assertEachOutputSorted(r);
      }
    }
  }

  /** Deletes arriving after the merge snapshot must land on the output owning the doc. */
  public void testConcurrentDeletes() throws Exception {
    try (Directory dir = newDirectory()) {
      Set<String> expected = new HashSet<>();
      List<String> deleted = new ArrayList<>();
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
            expected.add(id(seg, d));
          }
          w.flush();
        }
        w.commit();
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 5; d < PER_SEGMENT; d += 17) {
            deleted.add(id(seg, d));
          }
        }
        expected.removeAll(deleted);

        Thread deleter =
            new Thread(
                () -> {
                  try {
                    mergeStarted.await();
                    for (String id : deleted) {
                      w.deleteDocuments(new Term("id", id));
                    }
                    // Force the buffered deletes to resolve against the merging
                    // segments so they must be carried over.
                    DirectoryReader.open(w).close();
                  } catch (Throwable t) {
                    throw new AssertionError(t);
                  } finally {
                    proceed.countDown();
                  }
                });
        deleter.start();
        enabled = true;
        w.maybeMerge();
        deleter.join();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        List<String> live = liveIds(r);
        assertEquals("no document may be duplicated", live.size(), new HashSet<>(live).size());
        assertEquals(expected, new HashSet<>(live));
        assertEachOutputSorted(r);
      }
    }
  }

  /** Doc-values updates arriving mid-merge must be remapped to the owning output. */
  public void testConcurrentDocValuesUpdates() throws Exception {
    try (Directory dir = newDirectory()) {
      Map<String, Long> expected = new HashMap<>();
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 1));
            expected.put(id(seg, d), 1L);
          }
          w.flush();
        }
        w.commit();

        List<String> updates = new ArrayList<>();
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 7; d < PER_SEGMENT; d += 19) {
            updates.add(id(seg, d));
          }
        }
        updates.forEach(id -> expected.put(id, 42L));

        Thread updater =
            new Thread(
                () -> {
                  try {
                    mergeStarted.await();
                    for (String id : updates) {
                      w.updateNumericDocValue(new Term("id", id), "val", 42L);
                    }
                    DirectoryReader.open(w).close();
                  } catch (Throwable t) {
                    throw new AssertionError(t);
                  } finally {
                    proceed.countDown();
                  }
                });
        updater.start();
        enabled = true;
        w.maybeMerge();
        updater.join();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : r.leaves()) {
          StoredFields sf = ctx.reader().storedFields();
          NumericDocValues dv = ctx.reader().getNumericDocValues("val");
          Bits live = ctx.reader().getLiveDocs();
          for (int d = 0; d < ctx.reader().maxDoc(); d++) {
            if (live != null && live.get(d) == false) {
              continue;
            }
            String id = sf.document(d).get("id");
            assertNotNull(dv);
            assertTrue("no value for " + id, dv.advanceExact(d));
            assertEquals("wrong value for " + id, (long) expected.get(id), dv.longValue());
          }
        }
      }
    }
  }

  /** Soft deletes must be accounted per output, not for the merge as a whole. */
  public void testSoftDeletes() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = config();
      iwc.setSoftDeletesField(SOFT);
      iwc.setMergePolicy(
          new SoftDeletesRetentionMergePolicy(
              SOFT, MatchAllDocsQuery::new, new PartitioningMergePolicy()));
      Set<String> originals = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
            originals.add(id(seg, d));
          }
          w.flush();
        }
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 3; d < PER_SEGMENT; d += 23) {
            Document tomb = doc(id(seg, d), 0);
            tomb.add(new NumericDocValuesField(SOFT, 1));
            w.softUpdateDocument(
                new Term("id", id(seg, d)), tomb, new NumericDocValuesField(SOFT, 1));
          }
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue(r.leaves().size() > 1);
        // Retention keeps tombstones, so ids may repeat; nothing may be lost.
        assertTrue(new HashSet<>(liveIds(r)).containsAll(originals));
      }
    }
  }

  /**
   * A wrapper that keeps soft-deleted documents keeps hard-deleted ones with them. Those must not
   * reach an output: the merge is the last chance to drop them, and nothing downstream knows they
   * were deleted.
   */
  public void testHardDeletesDoNotSurviveRetention() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = config();
      iwc.setSoftDeletesField(SOFT);
      iwc.setMergePolicy(
          new SoftDeletesRetentionMergePolicy(
              SOFT, MatchAllDocsQuery::new, new PartitioningMergePolicy()));
      Set<String> hardDeleted = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
          }
          w.flush();
        }
        // The document has to be soft-deleted AND hard-deleted: retention keeps it for the
        // first reason, and the merge has to drop it for the second.
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 3; d < PER_SEGMENT; d += 23) {
            Document tomb = doc(id(seg, d), 0);
            tomb.add(new NumericDocValuesField(SOFT, 1));
            w.softUpdateDocument(
                new Term("id", id(seg, d)), tomb, new NumericDocValuesField(SOFT, 1));
          }
          w.commit();
          for (int d = 3; d < PER_SEGMENT; d += 46) {
            w.deleteDocuments(new Term("id", id(seg, d)));
            hardDeleted.add(id(seg, d));
          }
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        Set<String> live = new HashSet<>(liveIds(r));
        for (String id : hardDeleted) {
          assertFalse("hard-deleted " + id + " came back", live.contains(id));
        }
      }
    }
  }

  /** A malformed partition spec must be rejected, not silently corrupt the index. */
  public void testRejectsBadPartitions() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = config();
      iwc.setMergePolicy(new BadPartitioningMergePolicy());
      // Serial, so the validation failure surfaces on this thread rather than
      // on a merge thread where the assertion could not observe it.
      iwc.setMergeScheduler(new SerialMergeScheduler());
      Throwable caught = null;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 2; seg++) {
          for (int d = 0; d < 20; d++) {
            w.addDocument(doc(id(seg, d), 0));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        try {
          w.maybeMerge();
        } catch (Throwable t) {
          caught = t;
        }
        w.rollback();
      } catch (Throwable t) {
        if (caught == null) {
          caught = t;
        }
      }
      assertNotNull("a malformed partition spec must be rejected", caught);
      boolean sawIae = false;
      for (Throwable t = caught; t != null; t = t.getCause()) {
        if (t instanceof IllegalArgumentException) {
          sawIae = true;
          break;
        }
      }
      assertTrue("expected IllegalArgumentException in the cause chain, got " + caught, sawIae);
    }
  }

  /**
   * An IOException while writing one of the outputs must leave the index exactly as it was: a
   * partitioned merge is all-or-nothing, like any other merge.
   */
  public void testIOExceptionWritingAnOutput() throws Exception {
    try (Directory dir = newDirectory()) {
      Set<String> committed = new HashSet<>();
      IndexWriterConfig iwc = config();
      iwc.setMergeScheduler(new SerialMergeScheduler());
      IndexWriter w = new IndexWriter(dir, iwc);
      for (int seg = 0; seg < SEGMENTS; seg++) {
        for (int d = 0; d < PER_SEGMENT; d++) {
          w.addDocument(doc(id(seg, d), 0));
          committed.add(id(seg, d));
        }
        w.flush();
      }
      w.commit();

      AtomicBoolean fired = new AtomicBoolean();
      if (dir instanceof MockDirectoryWrapper mock) {
        // Fail once, partway through writing the partitioned outputs.
        mock.failOn(
            new MockDirectoryWrapper.Failure() {
              @Override
              public void eval(MockDirectoryWrapper d) throws IOException {
                if (fired.get()) {
                  return;
                }
                for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                  if ("multiOutputMergeMiddle".equals(e.getMethodName())) {
                    fired.set(true);
                    throw new IOException("injected while writing a partitioned output");
                  }
                }
              }
            });
      }

      enabled = true;
      proceed.countDown();
      try {
        w.maybeMerge();
      } catch (Throwable _) {
        // the injected failure may surface here
      }
      if (dir instanceof MockDirectoryWrapper) {
        assertTrue("the failure must actually have been injected", fired.get());
      }
      // The injected Failure disables itself after firing once.
      try {
        w.rollback();
      } catch (Throwable _) {
        // writer may already be tragically closed by the injected failure
      }

      // Whatever happened, the committed index must still be intact and valid.
      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        List<String> live = liveIds(r);
        assertEquals("no document may be duplicated", live.size(), new HashSet<>(live).size());
        assertEquals(committed, new HashSet<>(live));
      }
    }
  }

  /** rollback() while a partitioned merge is in flight must revert to the last commit. */
  public void testRollbackDuringPartitionedMerge() throws Exception {
    try (Directory dir = newDirectory()) {
      Set<String> committed = new HashSet<>();
      IndexWriter w = new IndexWriter(dir, config());
      for (int seg = 0; seg < SEGMENTS; seg++) {
        for (int d = 0; d < PER_SEGMENT; d++) {
          w.addDocument(doc(id(seg, d), 0));
          committed.add(id(seg, d));
        }
        w.flush();
      }
      w.commit();

      Thread roller =
          new Thread(
              () -> {
                try {
                  mergeStarted.await();
                  // Release the merge first: rollback() waits for in-flight
                  // merges, so holding it parked here would deadlock.
                  proceed.countDown();
                  w.rollback();
                } catch (Throwable _) {
                  // rollback races the merge; either ordering is acceptable
                }
              });
      roller.start();
      enabled = true;
      try {
        w.maybeMerge();
      } catch (Throwable _) {
        // may surface the abort
      }
      roller.join();
      try {
        w.close();
      } catch (Throwable _) {
      }

      TestUtil.checkIndex(dir);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        List<String> live = liveIds(r);
        assertEquals("no document may be duplicated", live.size(), new HashSet<>(live).size());
        assertEquals("rollback must restore the committed state", committed, new HashSet<>(live));
      }
    }
  }

  /** A wrapper that drops the partitioning must fail loudly, not silently make one segment. */
  public void testWrappingMustPreservePartitioning() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = config();
      iwc.setMergeScheduler(new SerialMergeScheduler());
      iwc.setMergePolicy(
          new OneMergeWrappingMergePolicy(
              new PartitioningMergePolicy(),
              toWrap ->
                  // Deliberately forgets to forward isPartitioned().
                  new MergePolicy.OneMerge(toWrap.segments) {
                    @Override
                    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                      return toWrap.wrapForMerge(reader);
                    }
                  }));
      Throwable caught = null;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < 2; seg++) {
          for (int d = 0; d < 20; d++) {
            w.addDocument(doc(id(seg, d), 0));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        try {
          w.maybeMerge();
        } catch (Throwable t) {
          caught = t;
        }
        w.rollback();
      } catch (Throwable t) {
        if (caught == null) {
          caught = t;
        }
      }
      assertNotNull("dropping the partitioning must be reported", caught);
      boolean sawIse = false;
      for (Throwable t = caught; t != null; t = t.getCause()) {
        if (t instanceof IllegalStateException
            && t.getMessage() != null
            && t.getMessage().contains("dropped the partitioning")) {
          sawIse = true;
          break;
        }
      }
      assertTrue("expected the wrapping check to fire, got " + caught, sawIse);
    }
  }

  /** FilterOneMerge forwards partitioning, so wrapping through it still produces k outputs. */
  public void testFilterOneMergePreservesPartitioning() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = config();
      iwc.setMergePolicy(
          new OneMergeWrappingMergePolicy(
              new PartitioningMergePolicy(), MergePolicy.FilterOneMerge::new));
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
            expected.add(id(seg, d));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue(
            "wrapping through FilterOneMerge must keep several outputs", r.leaves().size() > 1);
        assertEquals(expected, new HashSet<>(liveIds(r)));
      }
    }
  }

  // ------------------------------------------------------------------

  /**
   * Vector fields survive a partitioned merge without the KNN format knowing about partitioning.
   *
   * <p>Nothing narrows vector values by document range, so each output reads every input's vectors
   * and keeps the ones its range owns. Its graph is then built from scratch: a graph is reused only
   * when a reader's deleted fraction is small, and an output of a k-way split presents everything
   * outside its own range as deleted, which for any k &gt; 1 is at least half the segment.
   */
  public void testVectorsSurvivePartitionedMerge() throws Exception {
    try (Directory dir = newDirectory()) {
      Map<String, float[]> expected = new HashMap<>();
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            float[] vector = vectorFor(seg, d);
            w.addDocument(vectorDoc(id(seg, d), vector));
            expected.put(id(seg, d), vector);
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);

        // Every vector is present exactly once, still attached to its own document.
        Map<String, float[]> found = new HashMap<>();
        for (LeafReaderContext ctx : r.leaves()) {
          StoredFields sf = ctx.reader().storedFields();
          FloatVectorValues values = ctx.reader().getFloatVectorValues("vec");
          assertNotNull("output lost the vector field entirely", values);
          KnnVectorValues.DocIndexIterator it = values.iterator();
          for (int doc = it.nextDoc();
              doc != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS;
              doc = it.nextDoc()) {
            String id = sf.document(doc).get("id");
            assertNull("vector for " + id + " appeared in two outputs", found.put(id, null));
            assertArrayEquals(
                "vector changed for " + id, expected.get(id), values.vectorValue(it.index()), 0f);
          }
        }
        assertEquals(expected.keySet(), found.keySet());

        // And the rebuilt graphs are searchable. Deliberately not asserting which document comes
        // back: HNSW is approximate, so the exact nearest neighbour is a recall property of the
        // graph rather than anything partitioning decides. What matters here is that every output's
        // graph can be traversed and reaches real documents.
        IndexSearcher searcher = new IndexSearcher(r);
        float[] target = vectorFor(SEGMENTS - 1, PER_SEGMENT - 1);
        int k = 10;
        TopDocs hits = searcher.search(new KnnFloatVectorQuery("vec", target, k), k);
        assertEquals("every output's graph should contribute", k, hits.scoreDocs.length);
        StoredFields hitFields = searcher.storedFields();
        for (ScoreDoc hit : hits.scoreDocs) {
          String id = hitFields.document(hit.doc).get("id");
          assertTrue("hit on an unknown document " + id, expected.containsKey(id));
          assertTrue("hit with a non-finite score", Float.isFinite(hit.score));
        }
      }
    }
  }

  /**
   * Every field type survives a partitioned merge, still attached to the document it came from.
   *
   * <p>Only doc values and the postings know about the split at all -- doc values seek to the
   * output's range, postings are read per output -- so for every other format this asserts that
   * being handed a reader that presents the rest of the segment as deleted is enough. Values are
   * derived from the document's ordinal, so a value landing on the wrong document fails here rather
   * than looking plausible.
   */
  public void testEveryFieldTypeSurvivesPartitionedMerge() throws Exception {
    try (Directory dir = newDirectory()) {
      Map<String, Integer> expected = new HashMap<>();
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            int ord = seg * PER_SEGMENT + d;
            w.addDocument(everyTypeDoc(id(seg, d), ord));
            expected.put(id(seg, d), ord);
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }

      TestUtil.checkIndex(dir);

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);
        Set<String> seen = new HashSet<>();

        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader leaf = ctx.reader();
          StoredFields stored = leaf.storedFields();
          TermVectors termVectors = leaf.termVectors();

          NumericDocValues num = leaf.getNumericDocValues("num");
          BinaryDocValues bin = leaf.getBinaryDocValues("bin");
          SortedSetDocValues sset = leaf.getSortedSetDocValues("sset");
          SortedNumericDocValues snum = leaf.getSortedNumericDocValues("snum");
          NumericDocValues skip = leaf.getNumericDocValues("skip");
          NumericDocValues norms = leaf.getNormValues("text");
          FloatVectorValues vecs = leaf.getFloatVectorValues("vec");
          ByteVectorValues bvecs = leaf.getByteVectorValues("bvec");

          assertNotNull("lost doc values", num);
          assertNotNull("lost the skip-indexed field", skip);
          assertNotNull("lost norms", norms);
          assertNotNull("lost float vectors", vecs);
          assertNotNull("lost byte vectors", bvecs);
          assertNotNull("lost the doc-values skipper", leaf.getDocValuesSkipper("skip"));

          KnnVectorValues.DocIndexIterator vecIt = vecs.iterator();
          KnnVectorValues.DocIndexIterator bvecIt = bvecs.iterator();

          for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            String id = stored.document(doc).get("id");
            assertNotNull("document with no stored id", id);
            Integer ord = expected.get(id);
            assertNotNull("unknown document " + id, ord);
            assertTrue("document " + id + " appeared in two outputs", seen.add(id));

            assertTrue(num.advanceExact(doc));
            assertEquals("num", ord.intValue(), num.longValue());

            assertTrue(bin.advanceExact(doc));
            assertEquals("bin", new BytesRef(binFor(ord)), bin.binaryValue());

            assertTrue(sset.advanceExact(doc));
            assertEquals("sset count", 2, sset.docValueCount());
            Set<String> setValues = new HashSet<>();
            for (int i = 0; i < 2; i++) {
              setValues.add(sset.lookupOrd(sset.nextOrd()).utf8ToString());
            }
            assertEquals("sset", Set.of(binFor(ord), "shared"), setValues);

            assertTrue(snum.advanceExact(doc));
            assertEquals("snum count", 2, snum.docValueCount());
            assertEquals("snum[0]", ord.intValue(), snum.nextValue());
            assertEquals("snum[1]", ord + 1L, snum.nextValue());

            assertTrue(skip.advanceExact(doc));
            assertEquals("skip", ord.intValue(), skip.longValue());

            assertTrue(norms.advanceExact(doc));
            assertTrue("norm should be non-zero", norms.longValue() > 0);

            Terms tv = termVectors.get(doc, "text");
            assertNotNull("lost term vectors for " + id, tv);
            TermsEnum tvTerms = tv.iterator();
            assertTrue(
                "term vector missing its unique term",
                tvTerms.seekExact(new BytesRef("ord" + ord)));

            assertEquals("float vector doc", doc, vecIt.advance(doc));
            assertArrayEquals(
                "float vector", floatVecFor(ord), vecs.vectorValue(vecIt.index()), 0f);
            assertEquals("byte vector doc", doc, bvecIt.advance(doc));
            assertArrayEquals("byte vector", byteVecFor(ord), bvecs.vectorValue(bvecIt.index()));
          }
        }
        assertEquals("some documents were lost", expected.keySet(), seen);

        // The indexed structures still answer queries, across whichever output owns each document.
        IndexSearcher searcher = new IndexSearcher(r);
        for (int ord : new int[] {0, 1, expected.size() / 2, expected.size() - 1}) {
          assertEquals(
              "postings for ord" + ord,
              1,
              searcher.count(new TermQuery(new Term("text", "ord" + ord))));
          assertEquals(
              "1d point for ord" + ord, 1, searcher.count(IntPoint.newExactQuery("point", ord)));
          assertEquals(
              "2d point for ord" + ord,
              1,
              searcher.count(
                  LongPoint.newRangeQuery(
                      "point2d", new long[] {ord, -ord}, new long[] {ord, -ord})));
        }
        assertEquals(
            "shared term should match every document",
            expected.size(),
            searcher.count(new TermQuery(new Term("text", "shared"))));
      }
    }
  }

  /**
   * Each output is structurally identical to a segment built by indexing its documents directly.
   *
   * <p>The strongest statement available: rather than checking the values this test happens to
   * think of, it duels every output against a reference segment holding the same documents in the
   * same order, comparing terms, postings, norms, stored fields, term vectors, doc values, points
   * and vectors. A partitioned merge is supposed to be indistinguishable from having indexed each
   * output's share on its own, and this is that sentence as an assertion.
   */
  public void testOutputsMatchDirectlyIndexedSegments() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, config())) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(everyTypeDoc(id(seg, d), seg * PER_SEGMENT + d));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);
        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader output = ctx.reader();

          // Rebuild exactly this output's documents, in the order it holds them.
          StoredFields stored = output.storedFields();
          List<String> ids = new ArrayList<>();
          for (int doc = 0; doc < output.maxDoc(); doc++) {
            ids.add(stored.document(doc).get("id"));
          }

          try (Directory reference = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
            iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.STRING)));
            try (IndexWriter rw = new IndexWriter(reference, iwc)) {
              for (String id : ids) {
                rw.addDocument(everyTypeDoc(id, ordOf(id)));
              }
              rw.forceMerge(1);
            }
            try (DirectoryReader refReader = DirectoryReader.open(reference)) {
              assertEquals("reference should be one segment", 1, refReader.leaves().size());
              assertReaderEquals(
                  "output of a partitioned merge vs directly indexed", refReader, output);
              // assertReaderEquals covers everything but the vectors, so compare those here.
              assertVectorsEqual(refReader.leaves().get(0).reader(), output);
            }
          }
        }
      }
    }
  }

  /** A boundary landing inside a document block is refused rather than silently splitting it. */
  public void testRefusesToSplitDocumentBlocks() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = misalignedBlockConfig();
      // Serial, so the validation failure surfaces on this thread.
      iwc.setMergeScheduler(new SerialMergeScheduler());
      Throwable caught = null;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        // 3 does not divide the cut points the policy chooses, so a boundary lands inside a block.
        indexBlocks(w, 3);
        enabled = true;
        proceed.countDown();
        try {
          w.maybeMerge();
        } catch (Throwable t) {
          caught = t;
        }
        w.rollback();
      } catch (Throwable t) {
        if (caught == null) {
          caught = t;
        }
      }
      assertNotNull("splitting a document block must be refused", caught);
      boolean sawIae = false;
      for (Throwable t = caught; t != null; t = t.getCause()) {
        if (t instanceof IllegalArgumentException
            && t.getMessage().contains("cuts inside a document block")) {
          sawIae = true;
          break;
        }
      }
      assertTrue("expected a block-splitting rejection, got " + caught, sawIae);
    }
  }

  /**
   * Blocks survive a partitioned merge when the boundaries respect them.
   *
   * <p>Nested documents are not excluded by the primitive; what it requires is that a cut falls
   * between blocks. Here the block size divides the cut points, which is the same condition a
   * policy would arrange for by aligning its boundaries to parent documents.
   */
  public void testBlocksSurviveBoundariesThatRespectThem() throws Exception {
    final int blockSize = 4; // divides the policy's cut points (0, 40, 80, 120)
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, blockConfig())) {
        indexBlocks(w, blockSize);
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }

      TestUtil.checkIndex(dir);

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);
        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader leaf = ctx.reader();
          StoredFields stored = leaf.storedFields();
          Map<String, Integer> perOutput = new HashMap<>();
          for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            perOutput.merge(stored.document(doc).get("parent"), 1, Integer::sum);
          }
          for (Map.Entry<String, Integer> e : perOutput.entrySet()) {
            assertEquals(
                "block " + e.getKey() + " was split across outputs",
                blockSize,
                e.getValue().intValue());
          }
          // The parent of each block is still its last document, which is what block-join needs.
          NumericDocValues parents = leaf.getNumericDocValues("_parent");
          assertNotNull("lost the parent field", parents);
          for (int doc = blockSize - 1; doc < leaf.maxDoc(); doc += blockSize) {
            assertTrue("document " + doc + " should be a parent", parents.advanceExact(doc));
          }
        }
      }
    }
  }

  private void indexBlocks(IndexWriter w, int blockSize) throws IOException {
    for (int seg = 0; seg < SEGMENTS; seg++) {
      for (int b = 0; b < PER_SEGMENT / blockSize; b++) {
        List<Document> block = new ArrayList<>();
        String parentId = id(seg, b);
        for (int c = 0; c < blockSize - 1; c++) {
          Document child = new Document();
          child.add(new StringField("kind", "child", Field.Store.NO));
          child.add(new StoredField("parent", parentId));
          block.add(child);
        }
        Document parent = new Document();
        parent.add(new StringField("kind", "parent", Field.Store.NO));
        parent.add(new StoredField("parent", parentId));
        parent.add(new SortedDocValuesField("sort", new BytesRef(parentId)));
        block.add(parent);
        w.addDocuments(block);
      }
      w.flush();
    }
    w.commit();
  }

  /** Boundaries snapped to block ends, as a policy partitioning a block index must do. */
  private IndexWriterConfig blockConfig() {
    IndexWriterConfig iwc = misalignedBlockConfig();
    iwc.setMergePolicy(new BlockAlignedPartitioningMergePolicy());
    return iwc;
  }

  /** Boundaries on even document counts, which take no notice of blocks. */
  private IndexWriterConfig misalignedBlockConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.STRING)));
    iwc.setParentField("_parent");
    iwc.setMergePolicy(new PartitioningMergePolicy());
    return iwc;
  }

  /**
   * Cuts on even document counts, then snaps each boundary forward to the end of the block it lands
   * in -- which is what a policy partitioning a block index has to do, since a boundary is a
   * document offset and nothing about an even split respects blocks.
   *
   * <p>This is also why the test cannot simply pick a block size that divides the cut points: the
   * writer flushes on its own schedule, so segments do not all hold the same number of documents.
   */
  private class BlockAlignedPartitioningMergePolicy extends PartitioningMergePolicy {
    @Override
    OneMerge merge(List<SegmentCommitInfo> segs, int[][] parts) {
      return new Partitioned(segs, parts) {
        @Override
        public int[][] getDocRangePartitions(List<CodecReader> readers) {
          int[][] aligned = super.getDocRangePartitions(readers);
          for (int i = 0; i < readers.size(); i++) {
            CodecReader reader = readers.get(i);
            // A parent field's numeric doc values hold a value exactly on the last document of each
            // block, so iterating it lands on block ends.
            try {
              NumericDocValues parents = reader.getNumericDocValues("_parent");
              int[] b = aligned[i];
              for (int o = 1; o < b.length - 1; o++) {
                if (b[o] == 0 || b[o] >= reader.maxDoc()) {
                  continue;
                }
                int blockEnd = parents.advance(b[o] - 1);
                b[o] = blockEnd == NumericDocValues.NO_MORE_DOCS ? reader.maxDoc() : blockEnd + 1;
                if (b[o] < b[o - 1]) {
                  b[o] = b[o - 1];
                }
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
          return aligned;
        }
      };
    }
  }

  /**
   * An index sort is not required. It is what makes an output's documents a range of keys, but the
   * split itself is well defined without one -- the documents are distributed by position.
   */
  public void testPartitionsAnUnsortedIndex() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
      iwc.setMergePolicy(new PartitioningMergePolicy()); // deliberately no index sort
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            w.addDocument(doc(id(seg, d), 0));
            expected.add(id(seg, d));
          }
          w.flush();
        }
        w.commit();
        enabled = true;
        proceed.countDown();
        w.maybeMerge();
      }

      TestUtil.checkIndex(dir);

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected several outputs, got " + r.leaves().size(), r.leaves().size() > 1);
        List<String> live = liveIds(r);
        assertEquals(expected.size(), live.size());
        assertEquals(expected, new HashSet<>(live));
      }
    }
  }

  /**
   * The live documents of an output are a view over the reader's own, so they must answer exactly
   * as the bit set they replaced would have. Checked against that bit set, for {@code get} and for
   * {@code applyMask}, which the bulk merge paths use.
   */
  public void testRangeLiveDocsMatchesAMaterialisedBitSet() throws Exception {
    Random random = random();
    for (int iter = 0; iter < 200; iter++) {
      final int maxDoc = TestUtil.nextInt(random, 1, 400);
      final int a = random.nextInt(maxDoc + 1);
      final int b = random.nextInt(maxDoc + 1);
      final int start = Math.min(a, b);
      final int end = Math.max(a, b);

      // No deletions, everything deleted, or a random scatter of them.
      final Bits deletes;
      switch (random.nextInt(3)) {
        case 0 -> deletes = null;
        case 1 -> deletes = new Bits.MatchNoBits(maxDoc);
        default -> {
          FixedBitSet scattered = new FixedBitSet(maxDoc);
          for (int doc = 0; doc < maxDoc; doc++) {
            if (random.nextBoolean()) {
              scattered.set(doc);
            }
          }
          deletes = scattered;
        }
      }

      // What the reader used to build: a bit per document, masked by the deletions.
      FixedBitSet expected = new FixedBitSet(maxDoc);
      if (start < end) {
        expected.set(start, end);
      }
      if (deletes != null) {
        deletes.applyMask(expected, 0);
      }

      Bits view = new DocRangeCodecReader.RangeLiveDocs(deletes, start, end, maxDoc);
      assertEquals(maxDoc, view.length());
      for (int doc = 0; doc < maxDoc; doc++) {
        assertEquals(
            "doc " + doc + " of [" + start + "," + end + ")", expected.get(doc), view.get(doc));
      }

      // applyMask over a window that stays inside the reader, since the contract reads
      // length() only up to the end of the window.
      final int offset = random.nextInt(maxDoc);
      final int window = TestUtil.nextInt(random, 1, maxDoc - offset);
      FixedBitSet fromView = new FixedBitSet(window);
      FixedBitSet fromBitSet = new FixedBitSet(window);
      for (int i = 0; i < window; i++) {
        if (random.nextBoolean()) {
          fromView.set(i);
          fromBitSet.set(i);
        }
      }
      view.applyMask(fromView, offset);
      expected.applyMask(fromBitSet, offset);
      assertEquals(
          "applyMask at offset " + offset + " window " + window + " of [" + start + "," + end + ")",
          fromBitSet,
          fromView);
    }
  }

  /**
   * A range reader's cursor must not walk on after being asked for a document beyond its range.
   * {@code advanceExact} answers such a request without moving the values it wraps, so a following
   * {@code nextDoc} would step an iterator still positioned behind the range and hand back a
   * document a second time.
   */
  public void testAdvanceExactPastTheRangeEndsTheIteration() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int d = 0; d < PER_SEGMENT; d++) {
          w.addDocument(doc(id(0, d), d));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader leaf = (CodecReader) reader.leaves().get(0).reader();
        final int start = PER_SEGMENT / 4;
        final int end = PER_SEGMENT / 2;
        DocRangeCodecReader ranged = new DocRangeCodecReader(leaf, start, end);
        FieldInfo field = ranged.getFieldInfos().fieldInfo("val");

        NumericDocValues values = ranged.getDocValuesReader().getNumeric(field);
        assertFalse("nothing is exposed past the range", values.advanceExact(end + 1));
        assertEquals(DocValuesIterator.NO_MORE_DOCS, values.nextDoc());

        // The same reader still iterates its range correctly from a fresh cursor.
        NumericDocValues fresh = ranged.getDocValuesReader().getNumeric(field);
        int seen = 0;
        for (int doc = fresh.nextDoc();
            doc != DocValuesIterator.NO_MORE_DOCS;
            doc = fresh.nextDoc()) {
          assertTrue("walked outside the range at " + doc, doc >= start && doc < end);
          seen++;
        }
        assertEquals(end - start, seen);
      }
    }
  }

  /** An output owning no document here reads no terms dictionary and no points. */
  public void testEmptyRangeReadsNothing() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int d = 0; d < PER_SEGMENT; d++) {
          w.addDocument(doc(id(0, d), d));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader leaf = (CodecReader) reader.leaves().get(0).reader();
        DocRangeCodecReader empty = new DocRangeCodecReader(leaf, 7, 7);
        assertEquals(0, empty.numDocs());
        assertNull(empty.getPostingsReader());
        assertNull(empty.getPointsReader());
        assertFalse(empty.getLiveDocs().get(7));

        DocRangeCodecReader occupied = new DocRangeCodecReader(leaf, 7, 8);
        assertNotNull(occupied.getPostingsReader());
        assertTrue(occupied.getLiveDocs().get(7));
        assertFalse(occupied.getLiveDocs().get(8));
      }
    }
  }

  /**
   * What the whole point of an index sort is: boundaries placed where the key changes, rather than
   * on document counts, give outputs that each own a disjoint range of that key.
   *
   * <p>The boundaries are offsets into the inputs as they stand before the merge, but each input is
   * already sorted, so a contiguous range of an input is a range of keys within it. Taking the same
   * key in every input therefore gives one output the whole of that key range, and the merge sorts
   * it as usual.
   */
  public void testBoundariesOnAKeyGiveDisjointKeyRanges() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
      iwc.setIndexSort(new Sort(new SortField("tenant", SortField.Type.STRING)));
      iwc.setMergePolicy(new KeyPartitioningMergePolicy());
      Random random = random();
      int expected = 0;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < SEGMENTS; seg++) {
          for (int d = 0; d < PER_SEGMENT; d++) {
            Document doc = doc(id(seg, d), 0);
            // Tenants land in every segment, so no input holds a key range of its own.
            doc.add(new SortedDocValuesField("tenant", new BytesRef(tenant(random.nextInt(10)))));
            w.addDocument(doc);
            expected++;
          }
          w.commit();
        }
        w.forceMerge(OUTPUTS, true);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(OUTPUTS, reader.leaves().size());
        assertEquals(expected, reader.numDocs());
        String previousMax = null;
        for (LeafReaderContext ctx : reader.leaves()) {
          SortedDocValues tenants = ctx.reader().getSortedDocValues("tenant");
          String min = null;
          String max = null;
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            assertTrue(tenants.advanceExact(doc));
            String value = tenants.lookupOrd(tenants.ordValue()).utf8ToString();
            if (min == null) {
              min = value;
            }
            assertTrue(
                "segment is not sorted at doc " + doc, max == null || value.compareTo(max) >= 0);
            max = value;
          }
          assertNotNull("an output holds no documents", min);
          if (previousMax != null) {
            assertTrue(
                "key ranges overlap: " + previousMax + " then " + min,
                min.compareTo(previousMax) > 0);
          }
          previousMax = max;
        }
      }
    }
  }

  private static String tenant(int i) {
    return String.format(java.util.Locale.ROOT, "tenant-%02d", i);
  }

  /**
   * Cuts each input where the tenant reaches a boundary value, which is what a policy partitioning
   * by key does. The offsets differ per input, since each holds its own mix of tenants.
   */
  private class KeyPartitioningMergePolicy extends MergePolicy {
    private final String[] cuts = {tenant(3), tenant(7)};

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos infos, int max, Map<SegmentCommitInfo, Boolean> toMerge, MergeContext ctx) {
      List<SegmentCommitInfo> segs = new ArrayList<>();
      for (SegmentCommitInfo si : infos) {
        if (ctx.getMergingSegments().contains(si)) {
          return null;
        }
        if (toMerge.containsKey(si)) {
          segs.add(si);
        }
      }
      // Splitting leaves as many segments as it found outputs, so answering again would replan
      // the merge for ever.
      if (segs.size() <= max) {
        return null;
      }
      MergeSpecification spec = new MergeSpecification();
      spec.add(
          new MergePolicy.OneMerge(segs) {
            @Override
            public boolean isPartitioned() {
              return true;
            }

            @Override
            public int[][] getDocRangePartitions(List<CodecReader> readers) throws IOException {
              int[][] boundaries = new int[readers.size()][cuts.length + 2];
              for (int i = 0; i < readers.size(); i++) {
                CodecReader reader = readers.get(i);
                SortedDocValues tenants = reader.getSortedDocValues("tenant");
                int next = 1;
                for (int doc = 0; doc < reader.maxDoc() && next <= cuts.length; doc++) {
                  assertTrue(tenants.advanceExact(doc));
                  String value = tenants.lookupOrd(tenants.ordValue()).utf8ToString();
                  while (next <= cuts.length && value.compareTo(cuts[next - 1]) >= 0) {
                    boundaries[i][next++] = doc;
                  }
                }
                while (next <= cuts.length) {
                  boundaries[i][next++] = reader.maxDoc();
                }
                boundaries[i][cuts.length + 1] = reader.maxDoc();
              }
              return boundaries;
            }
          });
      return spec;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger t, SegmentInfos infos, MergeContext ctx) {
      return null;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos i, MergeContext c) {
      return null;
    }
  }

  private class PartitioningMergePolicy extends MergePolicy {
    /** Overridden by the block-aware subclass, which needs to adjust the boundaries. */
    OneMerge merge(List<SegmentCommitInfo> segs, int[][] parts) {
      return new Partitioned(segs, parts);
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger t, SegmentInfos infos, MergeContext ctx) {
      if (enabled == false || infos.size() < 2) {
        return null;
      }
      List<SegmentCommitInfo> segs = new ArrayList<>();
      for (SegmentCommitInfo si : infos) {
        if (ctx.getMergingSegments().contains(si)) {
          return null;
        }
        segs.add(si);
      }
      int[][] parts = new int[segs.size()][];
      for (int i = 0; i < segs.size(); i++) {
        int maxDoc = segs.get(i).info.maxDoc();
        int[] b = new int[OUTPUTS + 1];
        for (int o = 0; o <= OUTPUTS; o++) {
          b[o] = (int) ((long) o * maxDoc / OUTPUTS);
        }
        parts[i] = b;
      }
      MergeSpecification spec = new MergeSpecification();
      spec.add(merge(segs, parts));
      // One partitioned merge per test, and then done. This policy has no fixed point -- it
      // answers every request while there is more than one segment, and it turns OUTPUTS
      // segments back into OUTPUTS segments -- while IndexWriter asks the policy again after
      // every merge it finishes. Under the concurrent scheduler the writer closes out from
      // under that; under the serial one, which the framework picks at random, the scheduler
      // drains the queue in a loop and the merges never stop.
      enabled = false;
      return spec;
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos i, int m, Map<SegmentCommitInfo, Boolean> s, MergeContext c) {
      return null;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos i, MergeContext c) {
      return null;
    }
  }

  /** Emits boundaries that do not cover every document. */
  private class BadPartitioningMergePolicy extends PartitioningMergePolicy {
    @Override
    public MergeSpecification findMerges(MergeTrigger t, SegmentInfos infos, MergeContext ctx) {
      MergeSpecification spec = super.findMerges(t, infos, ctx);
      if (spec == null) {
        return null;
      }
      MergeSpecification bad = new MergeSpecification();
      for (OneMerge m : spec.merges) {
        int[][] parts = ((Partitioned) m).parts;
        parts[0][parts[0].length - 1] -= 1; // last boundary no longer maxDoc
        bad.add(new Partitioned(m.segments, parts));
      }
      return bad;
    }
  }

  private class Partitioned extends MergePolicy.OneMerge {
    final int[][] parts;

    Partitioned(List<SegmentCommitInfo> segments, int[][] parts) {
      super(segments);
      this.parts = parts;
    }

    @Override
    public boolean isPartitioned() {
      return true;
    }

    @Override
    public int[][] getDocRangePartitions(List<CodecReader> readers) {
      return parts;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
      // Runs after initMergeReaders has snapshotted liveDocs, so anything the
      // test deletes from here on is genuinely concurrent.
      mergeStarted.countDown();
      try {
        proceed.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      return reader;
    }
  }
}
