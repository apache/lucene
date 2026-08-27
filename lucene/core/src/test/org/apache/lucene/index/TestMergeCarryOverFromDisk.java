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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Deterministically exercises the doc-values-update merge carry-over path: an update that resolves
 * onto a segment <em>while it is being merged</em> must appear on the merged segment. This is the
 * path backed in heap today by {@code ReadersAndUpdates.mergingDVUpdates}; the test blocks a merge
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
    Document d = new Document();
    d.add(new StringField("id", Integer.toString(id), StringField.Store.NO));
    d.add(new NumericDocValuesField("ndv", ndv));
    d.add(new BinaryDocValuesField("bdv", new BytesRef(bdv)));
    return d;
  }

  public void testUpdatesResolvedDuringMergeAreCarriedOver() throws Exception {
    MergePausingDirectory dir = new MergePausingDirectory(newDirectory());
    IndexWriterConfig conf =
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergeScheduler(new ConcurrentMergeScheduler());
    IndexWriter writer = new IndexWriter(dir, conf);

    // Two segments; the default TieredMergePolicy won't auto-merge two small segments.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(doc(i, i, "v" + i));
    }
    writer.commit();
    for (int i = 4; i < 8; i++) {
      writer.addDocument(doc(i, i, "v" + i));
    }
    writer.commit();

    // Force the merge on a background thread so we can inject updates while it is paused mid-flight.
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

    // Wait until the merge has opened its readers (setIsMerging done) and begun writing output.
    dir.mergeStarted.await();

    // Update numeric + binary values on docs in the now-merging segments, and reset one, then
    // resolve them to the segments (and disk) by reopening a reader from the writer.
    writer.updateNumericDocValue(new Term("id", "1"), "ndv", 101L);
    writer.updateNumericDocValue(new Term("id", "5"), "ndv", 105L);
    writer.updateBinaryDocValue(new Term("id", "2"), "bdv", new BytesRef("updated-2"));
    writer.updateBinaryDocValue(new Term("id", "6"), "bdv", new BytesRef("updated-6"));
    // Force resolution onto the merging segments (writes DV gens to disk + populates mergingDVUpdates).
    try (DirectoryReader r = DirectoryReader.open(writer)) {
      assertNotNull(r);
    }

    // Let the merge finish; the carry-over assert in commitMergedDeletesAndUpdates runs here.
    dir.resumeMerge.countDown();
    merger.join();
    assertNull("forceMerge failed: " + dir.failure.get(), dir.failure.get());

    // Verify the merged segment reflects the numeric + binary updates that resolved during the merge.
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertEquals(1, reader.leaves().size()); // single merged segment
      LeafReader leaf = reader.leaves().get(0).reader();
      NumericDocValues ndv = leaf.getNumericDocValues("ndv");
      BinaryDocValues bdv = leaf.getBinaryDocValues("bdv");
      java.util.Set<Long> ndvValues = new java.util.HashSet<>();
      java.util.Set<String> bdvValues = new java.util.HashSet<>();
      for (int d = 0; d < leaf.maxDoc(); d++) {
        assertTrue(ndv.advanceExact(d));
        ndvValues.add(ndv.longValue());
        assertTrue(bdv.advanceExact(d));
        bdvValues.add(bdv.binaryValue().utf8ToString());
      }
      assertTrue("carried numeric update 101 missing", ndvValues.contains(101L));
      assertTrue("carried numeric update 105 missing", ndvValues.contains(105L));
      assertTrue("carried binary update updated-2 missing", bdvValues.contains("updated-2"));
      assertTrue("carried binary update updated-6 missing", bdvValues.contains("updated-6"));
    }

    writer.close();
    dir.close();
  }
}
