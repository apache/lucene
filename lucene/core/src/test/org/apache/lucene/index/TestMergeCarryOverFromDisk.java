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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Deterministically exercises the doc-values-update merge carry-over path: an update that resolves
 * onto a segment <em>while it is being merged</em> must appear on the merged segment. That
 * carry-over is reconstructed from the source segments at merge commit; the test blocks a merge
 * mid-flight (only its own output, via {@link IOContext#mergeInfo}, so the update's flush is free),
 * applies + resolves updates, then releases the merge.
 */
public class TestMergeCarryOverFromDisk extends LuceneTestCase {

  /** Wraps a directory, pausing the first merge output write until the test releases it. */
  private static class MergePausingDirectory extends FilterDirectory {
    final CountDownLatch mergeStarted = new CountDownLatch(1);
    final CountDownLatch resumeMerge = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    MergePausingDirectory(Directory in) {
      super(in);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      if (context != null && context.mergeInfo() != null && mergeStarted.getCount() > 0) {
        mergeStarted.countDown();
        try {
          resumeMerge.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      return super.createOutput(name, context);
    }
  }

  private static Document doc(int id, long ndv, String bdv) {
    return doc(id, ndv, bdv, ndv);
  }

  /** As {@link #doc(int, long, String)} but with an explicit, possibly multi-valued, snv set. */
  private static Document doc(int id, long ndv, String bdv, long... snv) {
    Document d = new Document();
    d.add(new StringField("id", Integer.toString(id), StringField.Store.NO));
    d.add(new NumericDocValuesField("ndv", ndv));
    d.add(new BinaryDocValuesField("bdv", new BytesRef(bdv)));
    d.add(new SortedNumericDocValuesField("svsnv", ndv));
    for (long v : snv) {
      d.add(new SortedNumericDocValuesField("snv", v));
    }
    return d;
  }

  public void testUpdatesResolvedDuringMergeAreCarriedOver() throws Exception {
    MergePausingDirectory dir = new MergePausingDirectory(newDirectory());
    IndexWriterConfig conf =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergeScheduler(new ConcurrentMergeScheduler());
    IndexWriter writer = new IndexWriter(dir, conf);

    // Two segments; the default TieredMergePolicy won't auto-merge two small segments. docs 0-3 are
    // single-valued and should create a NumericDocValues on disk; docs 4-7 are genuinely
    // multi-valued ({i, i+1000}) and should create a SortedNumericDocValues on disk. A doc never
    // updated for snv must keep its whole value set through the merge, while a doc updated for snv
    // (3 and 7 below) collapses to the single update value.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(doc(i, i, "v" + i, i));
    }
    writer.commit();
    for (int i = 4; i < 8; i++) {
      writer.addDocument(doc(i, i, "v" + i, i, i + 1000));
    }
    writer.commit();

    // Force the merge on a background thread so we can inject updates while it is paused
    // mid-flight.
    Thread merger =
        new Thread(
            () -> {
              try {
                writer.forceMerge(1);
              } catch (Throwable t) {
                dir.failure.compareAndSet(null, t);
              }
            },
            "forceMerge");
    merger.start();

    // Wait until the merge has opened its readers (its on-disk baseline is fixed) and begun writing
    // output.
    dir.mergeStarted.await();

    // Update numeric, binary, and sorted-numeric values on docs in the now-merging segments, then
    // resolve them to the segments (and disk) by reopening a reader from the writer.
    writer.updateNumericDocValue(new Term("id", "1"), "ndv", 101L);
    writer.updateNumericDocValue(new Term("id", "5"), "ndv", 105L);
    writer.updateBinaryDocValue(new Term("id", "2"), "bdv", new BytesRef("updated-2"));
    writer.updateBinaryDocValue(new Term("id", "6"), "bdv", new BytesRef("updated-6"));
    writer.updateSortedNumericDocValue(new Term("id", "3"), "snv", 103L);
    writer.updateSortedNumericDocValue(new Term("id", "7"), "snv", 107L);
    writer.updateSortedNumericDocValue(new Term("id", "3"), "svsnv", 103L);
    writer.updateSortedNumericDocValue(new Term("id", "7"), "svsnv", 107L);
    // Force resolution onto the merging segments (writes DV gens to disk).
    try (DirectoryReader r = DirectoryReader.open(writer)) {
      assertNotNull(r);
    }

    // Let the merge finish; the disk carry-over in commitMergedDeletesAndUpdates runs here.
    dir.resumeMerge.countDown();
    merger.join();
    assertNull("forceMerge failed: " + dir.failure.get(), dir.failure.get());

    // Verify every doc in the merged segment: the numeric, binary, and sorted-numeric updates that
    // resolved during the merge landed on exactly their docs, and every untouched doc (including
    // the
    // multi-valued one) kept the value it had before the merge.
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertEquals(1, reader.leaves().size()); // single merged segment
      LeafReader leaf = reader.leaves().get(0).reader();
      NumericDocValues ndv = leaf.getNumericDocValues("ndv");
      BinaryDocValues bdv = leaf.getBinaryDocValues("bdv");
      SortedNumericDocValues svsnv = leaf.getSortedNumericDocValues("svsnv");
      assertTrue(
          "single-valued SortedNumericDocValues should be a singleton",
          DocValues.isSingleton(svsnv));
      SortedNumericDocValues snv = leaf.getSortedNumericDocValues("snv");
      assertFalse(
          "multi-valued SortedNumericDocValues should not be a singleton",
          DocValues.isSingleton(snv));
      // docId -> id, so per-doc sorted-numeric value sets can be checked by id (postings iterate
      // per term, not in doc order).
      Map<Integer, String> idByDoc = new HashMap<>();
      TermsEnum te = leaf.terms("id").iterator();
      BytesRef t;
      while ((t = te.next()) != null) {
        PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
        for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
          idByDoc.put(d, t.utf8ToString());
        }
      }
      Map<String, Long> ndvById = new HashMap<>();
      Map<String, String> bdvById = new HashMap<>();
      Map<String, long[]> snvById = new HashMap<>();
      for (int d = 0; d < leaf.maxDoc(); d++) {
        String id = idByDoc.get(d);
        assertTrue(ndv.advanceExact(d));
        ndvById.put(id, ndv.longValue());
        assertTrue(bdv.advanceExact(d));
        bdvById.put(id, bdv.binaryValue().utf8ToString());
        assertTrue(snv.advanceExact(d)); // doc order, so advanceExact is monotonic
        long[] vals = new long[snv.docValueCount()];
        for (int i = 0; i < vals.length; i++) {
          vals[i] = snv.nextValue();
        }
        snvById.put(id, vals);
      }
      // Every doc must hold exactly its expected value after the merge: the four updated docs carry
      // the new value, and every untouched doc keeps what it had before the merge.
      for (int id = 0; id < 8; id++) {
        String key = Integer.toString(id);
        long expectedNdv =
            switch (id) {
              case 1 -> 101L;
              case 5 -> 105L;
              default -> id;
            };
        assertEquals("ndv for doc " + id, expectedNdv, (long) ndvById.get(key));
        String expectedBdv =
            switch (id) {
              case 2 -> "updated-2";
              case 6 -> "updated-6";
              default -> "v" + id;
            };
        assertEquals("bdv for doc " + id, expectedBdv, bdvById.get(key));
        long[] expectedSnv =
            switch (id) {
              case 3 -> new long[] {103};
              case 7 -> new long[] {107};
              default -> id < 4 ? new long[] {id} : new long[] {id, id + 1000};
            };
        assertArrayEquals("snv for doc " + id, expectedSnv, snvById.get(key));
      }
    }

    writer.close();
    dir.close();
  }

  /**
   * Applies more than {@code maxDocValuesOverlays} updates to one field of a merging segment while
   * the merge is paused, forcing the sparse overlays to fold (and possibly fold to a dense column)
   * before the carry-over reads them back. The merged segment must reflect the final value.
   */
  public void testFoldedOverlaysCarriedOverDuringMerge() throws Exception {
    MergePausingDirectory dir = new MergePausingDirectory(newDirectory());
    IndexWriterConfig conf =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergeScheduler(new ConcurrentMergeScheduler())
            // small overlay budget so a handful of updates already triggers a fold
            .setMaxDocValuesOverlays(4);
    IndexWriter writer = new IndexWriter(dir, conf);
    // Segment 1 (docs 0-3): single-valued snv, stored as a numeric column. Segment 2 (docs 4-7):
    // multi-valued snv ({i, i+1000}), stored as a sorted-numeric column.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(doc(i, i, "v" + i, i));
    }
    writer.commit();
    for (int i = 4; i < 8; i++) {
      writer.addDocument(doc(i, i, "v" + i, i, i + 1000));
    }
    writer.commit();

    Thread merger =
        new Thread(
            () -> {
              try {
                writer.forceMerge(1);
              } catch (Throwable t) {
                dir.failure.compareAndSet(null, t);
              }
            },
            "forceMerge");
    merger.start();
    dir.mergeStarted.await();

    // Many updates to doc 3 (in the first, merging segment), each resolved separately so it becomes
    // a new overlay generation; well past maxDocValuesOverlays, so folds happen mid-merge. Both a
    // numeric and a sorted-numeric field are folded so the carry-over reads back a folded column of
    // each kind.
    long finalValue = -1;
    long finalSnv = -1;
    for (int v = 0; v < 20; v++) {
      finalValue = 1000L + v;
      finalSnv = 2000L + v;
      writer.updateNumericDocValue(new Term("id", "3"), "ndv", finalValue);
      writer.updateSortedNumericDocValue(new Term("id", "3"), "snv", finalSnv);
      try (DirectoryReader r = DirectoryReader.open(writer)) {
        assertNotNull(r);
      }
    }

    dir.resumeMerge.countDown();
    merger.join();
    assertNull("forceMerge failed: " + dir.failure.get(), dir.failure.get());

    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertEquals(1, reader.leaves().size());
      LeafReader leaf = reader.leaves().get(0).reader();
      NumericDocValues ndv = leaf.getNumericDocValues("ndv");
      SortedNumericDocValues snv = leaf.getSortedNumericDocValues("snv");
      Set<Long> values = new HashSet<>();
      Set<Long> snvValues = new HashSet<>();
      for (int d = 0; d < leaf.maxDoc(); d++) {
        assertTrue(ndv.advanceExact(d));
        values.add(ndv.longValue());
        assertTrue(snv.advanceExact(d));
        for (int i = 0, count = snv.docValueCount(); i < count; i++) {
          snvValues.add(snv.nextValue());
        }
      }
      assertTrue("folded final value " + finalValue + " missing", values.contains(finalValue));
      assertTrue("folded final snv " + finalSnv + " missing", snvValues.contains(finalSnv));
    }

    writer.close();
    dir.close();
  }

  /**
   * A soft-delete that resolves onto a segment while it is being merged must be carried over, so
   * the old document is not live on the merged segment. The soft-deletes field is a doc-values
   * update that need not exist on disk yet, exercising the carry-over of pending updates to a field
   * absent from the source reader.
   */
  public void testSoftDeleteResolvedDuringMergeIsCarriedOver() throws Exception {
    MergePausingDirectory dir = new MergePausingDirectory(newDirectory());
    IndexWriterConfig conf =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergeScheduler(new ConcurrentMergeScheduler())
            .setSoftDeletesField("__soft");
    IndexWriter writer = new IndexWriter(dir, conf);
    // Segment 1 (docs 0-3): single-valued snv, stored as a numeric column. Segment 2 (docs 4-7):
    // multi-valued snv ({i, i+1000}), stored as a sorted-numeric column.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(doc(i, i, "v" + i, i));
    }
    writer.commit();
    for (int i = 4; i < 8; i++) {
      writer.addDocument(doc(i, i, "v" + i, i, i + 1000));
    }
    writer.commit();

    Thread merger =
        new Thread(
            () -> {
              try {
                writer.forceMerge(1);
              } catch (Throwable t) {
                dir.failure.compareAndSet(null, t);
              }
            },
            "forceMerge");
    merger.start();
    dir.mergeStarted.await();

    // Replace doc id=1 (in the merging segment): adds a new doc and soft-deletes the old one via a
    // doc-values update to the soft-deletes field. Resolve it onto the merging segment.
    Document replacement = doc(1, 101, "updated-1");
    writer.softUpdateDocument(
        new Term("id", "1"), replacement, new NumericDocValuesField("__soft", 1));
    // Also update a sorted-numeric value on a surviving multi-valued doc (id 5, in segment 2), to
    // confirm the update collapses it to a single value and is carried over alongside the
    // soft-delete.
    writer.updateSortedNumericDocValue(new Term("id", "5"), "snv", 205L);
    try (DirectoryReader r = DirectoryReader.open(writer)) {
      assertNotNull(r);
    }

    dir.resumeMerge.countDown();
    merger.join();
    assertNull("forceMerge failed: " + dir.failure.get(), dir.failure.get());

    // Exactly one doc with id=1 must be live (the replacement); the old one must be soft-deleted.
    // The replacement doc carries its own snv, and the sorted-numeric update on doc 5 is carried
    // over too.
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      int live = 0;
      boolean sawCarriedSnv = false;
      for (LeafReaderContext ctx : reader.leaves()) {
        LeafReader leaf = ctx.reader();
        NumericDocValues ndv = leaf.getNumericDocValues("ndv");
        SortedNumericDocValues snv = leaf.getSortedNumericDocValues("snv");
        Bits liveDocs = leaf.getLiveDocs();
        for (int d = 0; d < leaf.maxDoc(); d++) {
          if (liveDocs != null && liveDocs.get(d) == false) {
            continue;
          }
          assertTrue(ndv.advanceExact(d));
          assertTrue(snv.advanceExact(d)); // doc order, so advanceExact is monotonic
          long[] snvVals = new long[snv.docValueCount()];
          for (int i = 0; i < snvVals.length; i++) {
            snvVals[i] = snv.nextValue();
          }
          if (ndv.longValue() == 101) {
            live++;
            // the replacement doc is in the single-valued segment
            assertArrayEquals("replacement doc snv", new long[] {101}, snvVals);
          }
          // doc 5 (multi-valued base) collapsed to a single value by the carried update
          if (Arrays.equals(snvVals, new long[] {205})) {
            sawCarriedSnv = true;
          }
        }
      }
      assertEquals("exactly one live replacement doc expected", 1, live);
      assertTrue("carried sorted-numeric update 205 missing", sawCarriedSnv);
    }

    writer.close();
    dir.close();
  }
}
