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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestPartialHitCountCollector extends LuceneTestCase {

  private Directory dir;
  private IndexReader reader;
  private int numDocs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    // some tests need thresholds above the sync interval of collectors that are still below the
    // number of live docs
    int numIndexedDocs = atLeast(2000);
    for (int i = 0; i < numIndexedDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      doc.add(new StringField("all", "true", Field.Store.NO));
      doc.add(new StringField("mod", Integer.toString(i % 3), Field.Store.NO));
      iw.addDocument(doc);
    }
    for (int i = 0; i < numIndexedDocs; i++) {
      if (random().nextInt(10) == 0) {
        iw.deleteDocuments(new Term("id", Integer.toString(i)));
      }
    }
    reader = iw.getReader();
    iw.close();
    numDocs = reader.numDocs();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testNegativeThreshold() {
    IndexSearcher searcher = newSearcher(reader);
    expectThrows(
        IllegalArgumentException.class,
        () -> new PartialHitCountCollectorManager(searcher.getSlices(), -1));
  }

  public void testMaxValueThreshold() throws IOException {
    IndexSearcher searcher = newSearcher(reader, true, true, randomConcurrency());
    PartialHitCountCollectorManager manager =
        new PartialHitCountCollectorManager(searcher.getSlices(), Integer.MAX_VALUE);
    for (Query query : queries()) {
      assertEquals(
          new TotalHits(searcher.count(query), TotalHits.Relation.EQUAL_TO),
          searcher.search(query, manager));
    }
  }

  public void testZeroThresholdNoHits() throws IOException {
    IndexSearcher searcher = newSearcher(reader, true, true, randomConcurrency());
    Query noHits = new PerHitQuery(new TermQuery(new Term("mod", "3")));
    assertEquals(
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        searcher.search(noHits, new PartialHitCountCollectorManager(searcher.getSlices(), 0)));
  }

  public void testStopsAtThreshold() throws IOException {
    assertStopsAtThreshold(newPlainSearcher());
    assertStopsAtThreshold(newSequentialSearcher(false));
    assertStopsAtThreshold(newSequentialSearcher(true));
  }

  public void testCountFromIndexStatistics() throws IOException {
    assertCountFromIndexStatistics(newPlainSearcher());
    assertCountFromIndexStatistics(newSequentialSearcher(true));
  }

  public void testWithTopScoreDocCollectorManager() throws IOException {
    IndexSearcher searcher = newSearcher(reader, true, true, randomConcurrency());
    Query query = new PerHitQuery(new TermQuery(new Term("all", "true")));
    int threshold = random().nextInt(numDocs - 1);
    MultiCollectorManager manager =
        new MultiCollectorManager(
            new TopScoreDocCollectorManager(10, Integer.MAX_VALUE),
            new PartialHitCountCollectorManager(searcher.getSlices(), threshold));
    Object[] results = searcher.search(query, manager);

    TopDocs topDocs = (TopDocs) results[0];
    assertEquals(new TotalHits(numDocs, TotalHits.Relation.EQUAL_TO), topDocs.totalHits);
    assertEquals(10, topDocs.scoreDocs.length);

    assertPartialCount(numDocs, threshold, (TotalHits) results[1]);
  }

  public void testRandom() throws IOException {
    for (int iter = 0; iter < 20; iter++) {
      IndexSearcher searcher = newSearcher(reader, true, true, randomConcurrency());
      Query query = RandomizedTest.randomFrom(queries());
      int threshold =
          switch (random().nextInt(10)) {
            case 0 -> Integer.MAX_VALUE;
            // the highest threshold for which collectors sync hit counts
            case 1 -> Integer.MAX_VALUE - 1;
            default -> random().nextInt(numDocs * 2);
          };
      int expected = searcher.count(query);
      TotalHits totalHits =
          searcher.search(
              query, new PartialHitCountCollectorManager(searcher.getSlices(), threshold));
      assertPartialCount(expected, threshold, totalHits);
    }
  }

  public void testCollectorsShareHitCount() throws IOException {
    IndexSearcher searcher = newPlainSearcher();
    LeafReaderContext context = reader.leaves().getFirst();
    int numHits = context.reader().maxDoc();
    int threshold = TestUtil.nextInt(random(), 1, numHits);
    PartialHitCountCollectorManager manager =
        new PartialHitCountCollectorManager(searcher.getSlices(), threshold);

    // without a weight, collectors can't count leaves from index statistics and collect hits
    PartialHitCountCollector first = manager.newCollector();
    LeafCollector firstLeafCollector = first.getLeafCollector(context);
    firstLeafCollector.collectRange(0, threshold);
    firstLeafCollector.finish();
    assertEquals(threshold, first.getTotalHits());
    assertFalse(first.isEarlyTerminated());

    PartialHitCountCollector second = manager.newCollector();
    LeafCollector secondLeafCollector = second.getLeafCollector(context);
    expectThrows(
        CollectionTerminatedException.class, () -> secondLeafCollector.collectRange(0, numHits));
    secondLeafCollector.finish();
    assertEquals(numHits, second.getTotalHits());
    assertTrue(second.isEarlyTerminated());

    PartialHitCountCollector third = manager.newCollector();
    expectThrows(CollectionTerminatedException.class, () -> third.getLeafCollector(context));
    assertEquals(0, third.getTotalHits());
    assertTrue(third.isEarlyTerminated());

    assertEquals(
        new TotalHits(threshold + numHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        manager.reduce(List.of(first, second, third)));
  }

  public void testManagersDoNotShareHitCount() throws IOException {
    IndexSearcher searcher = newPlainSearcher();
    LeafReaderContext context = reader.leaves().getFirst();
    int numHits = context.reader().maxDoc();
    int threshold = random().nextInt(numHits);
    PartialHitCountCollectorManager firstManager =
        new PartialHitCountCollectorManager(searcher.getSlices(), threshold);
    PartialHitCountCollectorManager secondManager =
        new PartialHitCountCollectorManager(searcher.getSlices(), threshold);

    PartialHitCountCollector first = firstManager.newCollector();
    LeafCollector firstLeafCollector = first.getLeafCollector(context);
    expectThrows(
        CollectionTerminatedException.class, () -> firstLeafCollector.collectRange(0, numHits));
    firstLeafCollector.finish();

    // the hits counted for the first manager don't count towards the threshold of the second one
    PartialHitCountCollector second = secondManager.newCollector();
    second.getLeafCollector(context).finish();
    assertFalse(second.isEarlyTerminated());

    assertEquals(
        new TotalHits(0, TotalHits.Relation.EQUAL_TO), secondManager.reduce(List.of(second)));
    assertEquals(
        new TotalHits(numHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        firstManager.reduce(List.of(first)));
  }

  public void testEmptyIndex() throws IOException {
    try (IndexReader emptyReader = new MultiReader()) {
      IndexSearcher searcher = newSearcher(emptyReader, true, true, randomConcurrency());
      for (int threshold : new int[] {0, random().nextInt(10), Integer.MAX_VALUE}) {
        assertEquals(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            searcher.search(
                MatchAllDocsQuery.INSTANCE,
                new PartialHitCountCollectorManager(searcher.getSlices(), threshold)));
      }
    }
  }

  public void testSyncWithoutExceedingThreshold() throws IOException {
    try (Directory singleSegmentDir = newDirectory();
        IndexReader singleSegmentReader = newSingleSegmentReader(singleSegmentDir)) {
      IndexSearcher searcher = new IndexSearcher(singleSegmentReader);
      searcher.setQueryCache(null);
      int numIndexedDocs = singleSegmentReader.maxDoc();
      // syncs within a segment don't stop counting when the threshold isn't exceeded
      assertEquals(
          new TotalHits(numIndexedDocs, TotalHits.Relation.EQUAL_TO),
          searcher.search(
              new PerHitQuery(new TermQuery(new Term("all", "true"))),
              new PartialHitCountCollectorManager(searcher.getSlices(), numIndexedDocs)));
    }
  }

  public void testSyncIntervalCap() throws IOException {
    try (Directory singleSegmentDir = newDirectory();
        IndexReader singleSegmentReader = newSingleSegmentReader(singleSegmentDir)) {
      IndexSearcher searcher = new IndexSearcher(singleSegmentReader);
      // the first collector shares its hits after the sync interval, so the second one stops
      // after the sync interval too, rather than after exceeding the threshold on its own
      int threshold = PartialHitCountCollector.SYNC_INTERVAL * 3 / 2;
      PartialHitCountCollectorManager manager =
          new PartialHitCountCollectorManager(searcher.getSlices(), threshold);
      LeafReaderContext context = singleSegmentReader.leaves().getFirst();
      PartialHitCountCollector first = manager.newCollector();
      LeafCollector firstLeafCollector = first.getLeafCollector(context);
      PartialHitCountCollector second = manager.newCollector();
      LeafCollector secondLeafCollector = second.getLeafCollector(context);

      for (int doc = 0; doc < PartialHitCountCollector.SYNC_INTERVAL; doc++) {
        firstLeafCollector.collect(doc);
      }
      assertFalse(first.isEarlyTerminated());
      expectThrows(
          CollectionTerminatedException.class,
          () -> secondLeafCollector.collectRange(0, PartialHitCountCollector.SYNC_INTERVAL));
      assertTrue(second.isEarlyTerminated());
      assertEquals(
          new TotalHits(
              2 * PartialHitCountCollector.SYNC_INTERVAL,
              TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
          manager.reduce(List.of(first, second)));
    }
  }

  public void testSyncAfterShareOfThreshold() throws IOException {
    try (Directory singleSegmentDir = newDirectory();
        IndexReader singleSegmentReader = newSingleSegmentReader(singleSegmentDir)) {
      // with several slices, the first collector shares its hits after its share of the
      // threshold, so the second one stops once both together exceed the threshold. The
      // partitions only give the manager several slices, collectors are driven by hand.
      int numSlices = TestUtil.nextInt(random(), 2, 4);
      IndexSearcher searcher = newPartitionedSearcher(singleSegmentReader, numSlices);
      assertEquals(numSlices, searcher.getSlices().length);
      // keep the share below the sync interval so that it comes from the number of slices
      int threshold =
          TestUtil.nextInt(
              random(),
              numSlices,
              Math.min(
                  numSlices * PartialHitCountCollector.SYNC_INTERVAL - 2,
                  singleSegmentReader.maxDoc() - 1));
      PartialHitCountCollectorManager manager =
          new PartialHitCountCollectorManager(searcher.getSlices(), threshold);
      int share = (threshold + 1) / numSlices;
      LeafReaderContext context = singleSegmentReader.leaves().getFirst();
      PartialHitCountCollector first = manager.newCollector();
      LeafCollector firstLeafCollector = first.getLeafCollector(context);
      for (int doc = 0; doc < share; doc++) {
        firstLeafCollector.collect(doc);
      }
      assertFalse(first.isEarlyTerminated());
      PartialHitCountCollector second = manager.newCollector();
      LeafCollector secondLeafCollector = second.getLeafCollector(context);
      expectThrows(
          CollectionTerminatedException.class,
          () -> secondLeafCollector.collectRange(0, threshold + 1 - share));
      assertTrue(second.isEarlyTerminated());
      assertEquals(
          new TotalHits(threshold + 1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
          manager.reduce(List.of(first, second)));
    }
  }

  public void testSegmentPartitions() throws IOException {
    IndexSearcher searcher = newSequentialSearcher(true);
    // the count of a segment is computed once for its first partition, not once per partition.
    // Reusing the manager checks that the first partitions decide again how to count segments.
    PartialHitCountCollectorManager manager =
        new PartialHitCountCollectorManager(searcher.getSlices(), numDocs);
    TotalHits allHits = new TotalHits(numDocs, TotalHits.Relation.EQUAL_TO);
    assertEquals(allHits, searcher.search(MatchAllDocsQuery.INSTANCE, manager));
    assertEquals(
        allHits, searcher.search(new PerHitQuery(new TermQuery(new Term("all", "true"))), manager));
  }

  /**
   * Checks that searches stop right after exceeding thresholds of 0 and around the sync interval,
   * which requires slices to be searched one after the other, and that the manager can be reused.
   */
  private void assertStopsAtThreshold(IndexSearcher searcher) throws IOException {
    Query allQuery = new PerHitQuery(new TermQuery(new Term("all", "true")));
    Query noHits = new PerHitQuery(new TermQuery(new Term("mod", "3")));
    int syncInterval = PartialHitCountCollector.SYNC_INTERVAL;
    int[] thresholds = {
      0, random().nextInt(syncInterval), TestUtil.nextInt(random(), syncInterval, numDocs - 2)
    };
    for (int threshold : thresholds) {
      PartialHitCountCollectorManager manager =
          new PartialHitCountCollectorManager(searcher.getSlices(), threshold);
      TotalHits expected =
          new TotalHits(threshold + 1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
      assertEquals(expected, searcher.search(allQuery, manager));
      assertEquals(expected, searcher.search(allQuery, manager));
      // fails if the hits counted by the previous searches still count towards the threshold
      assertEquals(new TotalHits(0, TotalHits.Relation.EQUAL_TO), searcher.search(noHits, manager));
    }
  }

  /**
   * Checks that with a threshold of 0, segments are counted from index statistics until one has a
   * hit, then the remaining partitions are skipped, including the other partitions of that segment.
   * Slices must be searched one after the other.
   */
  private static void assertCountFromIndexStatistics(IndexSearcher searcher) throws IOException {
    long expectedCount = 0;
    boolean skippedPartitions = false;
    outer:
    for (IndexSearcher.LeafSlice slice : searcher.getSlices()) {
      for (IndexSearcher.LeafReaderContextPartition partition : slice.partitions) {
        if (expectedCount > 0) {
          skippedPartitions = true;
          break outer;
        }
        if (partition.minDocId == 0) {
          expectedCount += partition.ctx.reader().numDocs();
        }
      }
    }
    assertEquals(
        new TotalHits(
            expectedCount,
            skippedPartitions
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO),
        searcher.search(
            MatchAllDocsQuery.INSTANCE,
            new PartialHitCountCollectorManager(searcher.getSlices(), 0)));
  }

  /**
   * Returns a reader over a single segment of more than twice the sync interval of documents, all
   * matching {@code all:true}.
   */
  private static IndexReader newSingleSegmentReader(Directory dir) throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numIndexedDocs = 2 * PartialHitCountCollector.SYNC_INTERVAL + random().nextInt(100);
    for (int i = 0; i < numIndexedDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("all", "true", Field.Store.NO));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    IndexReader singleSegmentReader = iw.getReader();
    iw.close();
    assertEquals(1, singleSegmentReader.leaves().size());
    return singleSegmentReader;
  }

  /**
   * Returns a searcher that searches one slice per segment, or two slices per segment if {@code
   * splitSegments} is {@code true}, one after the other.
   */
  private IndexSearcher newSequentialSearcher(boolean splitSegments) {
    IndexSearcher searcher =
        new IndexSearcher(reader, Runnable::run) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            List<LeafSlice> slices = new ArrayList<>();
            for (LeafReaderContext leaf : leaves) {
              int maxDoc = leaf.reader().maxDoc();
              if (splitSegments && maxDoc >= 2) {
                int mid = maxDoc / 2;
                slices.add(
                    new LeafSlice(
                        List.of(LeafReaderContextPartition.createFromAndTo(leaf, 0, mid))));
                slices.add(
                    new LeafSlice(
                        List.of(LeafReaderContextPartition.createFromAndTo(leaf, mid, maxDoc))));
              } else {
                slices.add(
                    new LeafSlice(
                        List.of(LeafReaderContextPartition.createForEntireSegment(leaf))));
              }
            }
            return slices.toArray(LeafSlice[]::new);
          }
        };
    searcher.setQueryCache(null);
    return searcher;
  }

  /**
   * Returns a searcher that splits the single segment of the reader into partitions of equal size.
   */
  private static IndexSearcher newPartitionedSearcher(IndexReader reader, int numPartitions) {
    // searchers without an executor search a single slice
    IndexSearcher searcher =
        new IndexSearcher(reader, Runnable::run) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            LeafReaderContext leaf = leaves.getFirst();
            int maxDoc = leaf.reader().maxDoc();
            LeafSlice[] slices = new LeafSlice[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
              int minDocId = maxDoc * i / numPartitions;
              int maxDocId = maxDoc * (i + 1) / numPartitions;
              slices[i] =
                  new LeafSlice(
                      List.of(
                          LeafReaderContextPartition.createFromAndTo(leaf, minDocId, maxDocId)));
            }
            return slices;
          }
        };
    searcher.setQueryCache(null);
    return searcher;
  }

  private static void assertPartialCount(long expected, int threshold, TotalHits totalHits) {
    if (expected <= threshold || totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
      assertEquals(new TotalHits(expected, TotalHits.Relation.EQUAL_TO), totalHits);
    } else {
      assertTrue(totalHits.value() > threshold);
      assertTrue(totalHits.value() <= expected);
    }
  }

  private IndexSearcher newPlainSearcher() {
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    return searcher;
  }

  private static Concurrency randomConcurrency() {
    return RandomizedTest.randomFrom(Concurrency.values());
  }

  private static List<Query> queries() {
    Query mod0 = new TermQuery(new Term("mod", "0"));
    Query mod0Or1 =
        new BooleanQuery.Builder()
            .add(mod0, Occur.SHOULD)
            .add(new TermQuery(new Term("mod", "1")), Occur.SHOULD)
            .build();
    return List.of(
        MatchAllDocsQuery.INSTANCE,
        mod0,
        mod0Or1,
        new TermQuery(new Term("mod", "3")),
        new PerHitQuery(MatchAllDocsQuery.INSTANCE),
        new PerHitQuery(mod0),
        new PerHitQuery(mod0Or1));
  }

  /**
   * Wraps a query to hide its {@link Weight#count} and to collect its hits one by one, rather than
   * in batches via {@link DocIdStream}.
   */
  private static final class PerHitQuery extends Query {
    private final Query in;

    PerHitQuery(Query in) {
      this.in = in;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      Query rewritten = in.rewrite(indexSearcher);
      if (rewritten != in) {
        return new PerHitQuery(rewritten);
      }
      return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new FilterWeight(this, in.createWeight(searcher, scoreMode, boost)) {
        @Override
        public int count(LeafReaderContext context) {
          return -1;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier inSupplier = super.scorerSupplier(context);
          if (inSupplier == null) {
            return null;
          }
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return inSupplier.get(leadCost);
            }

            @Override
            public long cost() throws IOException {
              return inSupplier.cost();
            }
          };
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
      in.visit(visitor.getSubVisitor(Occur.MUST, this));
    }

    @Override
    public String toString(String field) {
      return "PerHitQuery(" + in.toString(field) + ")";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && in.equals(((PerHitQuery) other).in);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + in.hashCode();
    }
  }
}
