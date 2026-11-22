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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.FixedBitSetCollector;
import org.apache.lucene.tests.search.ScorerIndexSearcher;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Differential oracle: the optimised bulk-scoring path must produce the same document set as the
 * doc-by-doc {@link Scorer} path ({@link ScorerIndexSearcher}) for random queries over random
 * indexes. Compares match sets only, not scores. Written after the {@code
 * DocValuesRangeIterator.docIDRunEnd()} bug (GH#16450), which this harness catches.
 */
public class TestBulkScorerConsistency extends LuceneTestCase {

  /**
   * Random index, random queries, fast == slow. The fast side is a bare {@link IndexSearcher} so
   * the production bulk scorers run unwrapped by the test framework.
   */
  public void testBulkEqualsScorer() throws IOException {
    try (Directory dir = newDirectory()) {
      List<FieldSpec> fields = new RandomIndexBuilder(random()).build(dir);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher fastSearcher = new IndexSearcher(reader);
        ScorerIndexSearcher slowSearcher = new ScorerIndexSearcher(reader);
        fastSearcher.setQueryCache(null);
        slowSearcher.setQueryCache(null);

        RandomLuceneQueryGenerator gen = new RandomLuceneQueryGenerator(random(), fields);
        int numQueries = TestUtil.nextInt(random(), 20, 100);
        for (int q = 0; q < numQueries; q++) {
          assertBulkEqualsScorer(fastSearcher, slowSearcher, gen.next());
        }
      }
    }
  }

  /** Self-test: a bulk scorer that collects without checking matches must be flagged. */
  public void testHarnessDetectsBrokenBulkScorer() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 100; i++) {
          Document doc = new Document();
          doc.add(new StringField("f", i % 2 == 0 ? "even" : "odd", Field.Store.NO));
          w.addDocument(doc);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        ScorerIndexSearcher correct = new ScorerIndexSearcher(reader);
        LyingIndexSearcher lying = new LyingIndexSearcher(reader);
        correct.setQueryCache(null);
        lying.setQueryCache(null);

        int maxDoc = reader.maxDoc();
        Query query = new TermQuery(new Term("f", "even"));

        FixedBitSet correctResult =
            correct.search(query, FixedBitSetCollector.createManager(maxDoc));
        FixedBitSet lyingResult = lying.search(query, FixedBitSetCollector.createManager(maxDoc));

        assertEquals(50, correctResult.cardinality());
        assertFalse(lyingResult.equals(correctResult));
        assertEquals(maxDoc, lyingResult.cardinality());
      }
    }
  }

  /**
   * Regression test for GH#16450: {@code docIDRunEnd()} over-reported for a non-contiguous ordinal
   * set, so on a {@code WINDOW_SIZE + 1}-doc index the trailing single-doc window looked fully
   * matching and {@code DenseConjunctionBulkScorer} collected it via {@code collectRange} without
   * confirming {@code matches()}.
   */
  public void testDVOrdinalSetFalsePositive() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE + 1;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < maxDoc; i++) {
          Document doc = new Document();
          // Most docs: "aaa" (ord 0). Every 100th: "ccc" (ord 2) to establish the 3-term vocab.
          // Last doc: "bbb" (ord 1) — inside bounding range [0,2] but NOT in set {0,2}.
          String val = (i == maxDoc - 1) ? "bbb" : (i % 100 == 0 ? "ccc" : "aaa");
          doc.add(SortedDocValuesField.indexedField("dv", new BytesRef(val)));
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher fast = new IndexSearcher(reader);
        ScorerIndexSearcher slow = new ScorerIndexSearcher(reader);
        fast.setQueryCache(null);
        slow.setQueryCache(null);

        // Ordinal set {aaa=0, ccc=2} with gap at bbb=1: non-contiguous, so the block iterator
        // cannot prove whole-block matches and docIDRunEnd() must stay conservative.
        Query setQuery =
            SortedDocValuesField.newSlowSetQuery(
                "dv", List.of(new BytesRef("aaa"), new BytesRef("ccc")));
        Query rangeQuery =
            SortedDocValuesField.newSlowRangeQuery(
                "dv", new BytesRef("aaa"), new BytesRef("ccc"), true, true);
        Query q =
            new BooleanQuery.Builder()
                .add(setQuery, BooleanClause.Occur.FILTER)
                .add(rangeQuery, BooleanClause.Occur.FILTER)
                .build();

        FixedBitSet fastBits = fast.search(q, FixedBitSetCollector.createManager(maxDoc));
        FixedBitSet slowBits = slow.search(q, FixedBitSetCollector.createManager(maxDoc));
        assertEquals("fast path cardinality", maxDoc - 1, fastBits.cardinality());
        assertEquals("slow path cardinality", maxDoc - 1, slowBits.cardinality());
      }
    }
  }

  static void assertBulkEqualsScorer(
      IndexSearcher fastSearcher, ScorerIndexSearcher slowSearcher, Query query)
      throws IOException {
    int maxDoc = fastSearcher.getIndexReader().maxDoc();
    FixedBitSet fast = fastSearcher.search(query, FixedBitSetCollector.createManager(maxDoc));
    FixedBitSet slow = slowSearcher.search(query, FixedBitSetCollector.createManager(maxDoc));
    if (slow.equals(fast) == false) {
      StringBuilder diff = new StringBuilder();
      for (int doc = 0, shown = 0; doc < maxDoc && shown < 10; doc++) {
        if (slow.get(doc) != fast.get(doc)) {
          diff.append(doc).append(slow.get(doc) ? " missed by bulk; " : " bulk false positive; ");
          shown++;
        }
      }
      fail("bulk scorer and doc-by-doc scorer disagree on: " + query + " — docs: " + diff);
    }
  }

  // Replaces the real bulk scorer with one that collects every doc in [min, max)
  // unconditionally (i.e. claims all docs match). Used only by testHarnessDetectsBrokenBulkScorer
  // to confirm the oracle catches a scorer that over-reports.
  static class LyingIndexSearcher extends IndexSearcher {
    LyingIndexSearcher(org.apache.lucene.index.IndexReader r) {
      super(r);
    }

    @Override
    protected void searchLeaf(
        LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
        throws IOException {
      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(ctx);
      } catch (CollectionTerminatedException _) {
        return;
      }
      ScorerSupplier ss = weight.scorerSupplier(ctx);
      if (ss == null) {
        leafCollector.finish();
        return;
      }
      BulkScorer real = ss.bulkScorer();
      if (real == null) {
        leafCollector.finish();
        return;
      }
      try {
        int maxDoc = Math.min(maxDocId, ctx.reader().maxDoc());
        new LyingBulkScorer(real)
            .score(leafCollector, ctx.reader().getLiveDocs(), minDocId, maxDoc);
      } catch (CollectionTerminatedException _) {
        // normal
      }
      leafCollector.finish();
    }
  }

  static class LyingBulkScorer extends BulkScorer {
    private final BulkScorer in;

    LyingBulkScorer(BulkScorer in) {
      this.in = in;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      collector.setScorer(
          new Scorable() {
            @Override
            public float score() {
              return 0f;
            }
          });
      for (int doc = min; doc < max; doc++) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          collector.collect(doc);
        }
      }
      return max;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }
}
