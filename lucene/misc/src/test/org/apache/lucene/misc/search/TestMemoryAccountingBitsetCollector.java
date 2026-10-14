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

package org.apache.lucene.misc.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.misc.CollectorMemoryTracker;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestMemoryAccountingBitsetCollector extends LuceneTestCase {

  Directory dir;
  IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      doc.add(new SortedDocValuesField("field2", new BytesRef(Boolean.toString(i % 2 == 0))));
      iw.addDocument(doc);
    }
    reader = iw.getReader();
    iw.close();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    reader.close();
    dir.close();
  }

  public void testMemoryAccountingBitsetCollectorMemoryLimit() throws Exception {
    long collectorMemoryLimit = 150;
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", collectorMemoryLimit);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);
    IndexSearcher searcher = newSearcher(reader);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(MatchAllDocsQuery.INSTANCE, bitsetCollectorManager));
  }

  public void testCollectedResult() throws Exception {
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", Long.MAX_VALUE);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);

    IndexSearcher searcher = newSearcher(reader);
    MemoryAccountingBitsetCollectorManager.Result result =
        searcher.search(MatchAllDocsQuery.INSTANCE, bitsetCollectorManager);

    assertEquals(1000, result.bitSet().cardinality());
    for (int i = 0; i < 1000; i++) {
      assertTrue(result.bitSet().get(i));
    }
    // For collector with collecting only 1 doc, 80 bytes are required.
    assertTrue(result.totalBytesUsed() >= 80);
  }

  public void testResultBitSetSizedToHighestMatchedDoc() throws Exception {
    // Highly selective query: matches a single document early in the index. The result bitset
    // must be sized tightly to (highestMatchedDoc + 1) rather than padded to the last visited
    // leaf's docBase + maxDoc. Uses newSearcher() to get randomized executor/slicing coverage;
    // the deterministic intra-segment case is exercised by the sister test below.
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", Long.MAX_VALUE);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);

    IndexSearcher searcher = newSearcher(reader);
    MemoryAccountingBitsetCollectorManager.Result result =
        searcher.search(new TermQuery(new Term("field", "5")), bitsetCollectorManager);

    assertEquals(1, result.bitSet().cardinality());
    int matchedDoc = result.bitSet().nextSetBit(0);
    assertEquals(matchedDoc + 1, result.bitSet().length());
    assertTrue(result.bitSet().length() < reader.maxDoc());
  }

  public void testResultBitSetSizedToHighestMatchedDocUnderIntraSegmentConcurrency()
      throws Exception {
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", Long.MAX_VALUE);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);

    IndexSearcher searcher =
        new IndexSearcher(reader, Runnable::run) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            // Split each leaf into two partitions, each in its own slice, to force
            // intra-segment concurrency.
            List<LeafSlice> slices = new ArrayList<>();
            for (LeafReaderContext ctx : leaves) {
              int maxDoc = ctx.reader().maxDoc();
              if (maxDoc <= 1) {
                slices.add(
                    new LeafSlice(
                        Collections.singletonList(
                            LeafReaderContextPartition.createForEntireSegment(ctx))));
              } else {
                int mid = maxDoc / 2;
                slices.add(
                    new LeafSlice(
                        Collections.singletonList(
                            LeafReaderContextPartition.createFromAndTo(ctx, 0, mid))));
                slices.add(
                    new LeafSlice(
                        Collections.singletonList(
                            LeafReaderContextPartition.createFromAndTo(ctx, mid, maxDoc))));
              }
            }
            return slices.toArray(LeafSlice[]::new);
          }
        };

    // Use a mid-index match so any given leaf's slice split lands the match in a non-trivial
    // partition, and the highest-set bit differs from the other tests.
    MemoryAccountingBitsetCollectorManager.Result result =
        searcher.search(new TermQuery(new Term("field", "500")), bitsetCollectorManager);

    assertEquals(1, result.bitSet().cardinality());
    int matchedDoc = result.bitSet().nextSetBit(0);
    assertEquals(matchedDoc + 1, result.bitSet().length());
    assertTrue(result.bitSet().length() < reader.maxDoc());
  }

  public void testResultBitSetEmptyOnNoMatches() throws Exception {
    CollectorMemoryTracker tracker =
        new CollectorMemoryTracker("testMemoryTracker", Long.MAX_VALUE);
    MemoryAccountingBitsetCollectorManager bitsetCollectorManager =
        new MemoryAccountingBitsetCollectorManager(tracker);

    IndexSearcher searcher = newSearcher(reader);
    MemoryAccountingBitsetCollectorManager.Result result =
        searcher.search(new TermQuery(new Term("field", "does-not-exist")), bitsetCollectorManager);

    assertEquals(0, result.bitSet().cardinality());
    // The bitset must have at least one bit so that the natural iteration idiom stays safe on
    // empty results: without this guard, nextSetBit(0) would read past an empty long[] and throw
    // ArrayIndexOutOfBoundsException (see #16452 review).
    assertEquals(1, result.bitSet().length());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, result.bitSet().nextSetBit(0));
    assertFalse(result.bitSet().get(0));
  }
}
