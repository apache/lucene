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

package org.apache.lucene.monitor;

import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestForceNoBulkScoringQuery extends LuceneTestCase {

  public void testEquality() {

    TermQuery tq1 = new TermQuery(new Term("f", "t"));
    TermQuery tq2 = new TermQuery(new Term("f", "t2"));
    TermQuery tq3 = new TermQuery(new Term("f", "t2"));

    assertEquals(new ForceNoBulkScoringQuery(tq1), new ForceNoBulkScoringQuery(tq1));
    assertNotEquals(new ForceNoBulkScoringQuery(tq1), new ForceNoBulkScoringQuery(tq2));
    assertEquals(new ForceNoBulkScoringQuery(tq2), new ForceNoBulkScoringQuery(tq3));

    assertEquals(
        new ForceNoBulkScoringQuery(tq2).hashCode(), new ForceNoBulkScoringQuery(tq3).hashCode());
  }

  public void testRewrite() throws IOException {

    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

      Document doc = new Document();
      doc.add(new TextField("field", "term1 term2 term3 term4", Field.Store.NO));
      iw.addDocument(doc);
      iw.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        PrefixQuery pq = new PrefixQuery(new Term("field", "term"));
        ForceNoBulkScoringQuery q = new ForceNoBulkScoringQuery(pq);

        assertEquals(q.getWrappedQuery(), pq);

        Query rewritten = q.rewrite(newSearcher(reader));
        assertTrue(rewritten instanceof ForceNoBulkScoringQuery);

        Query inner = ((ForceNoBulkScoringQuery) rewritten).getWrappedQuery();
        assertNotEquals(inner, pq);
      }
    }
  }

  public void testBulkScoringIsDisabled() throws IOException {

    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {

      Document doc = new Document();
      doc.add(new TextField("field", "term1 term2 term3 term4", Field.Store.NO));
      iw.addDocument(doc);
      iw.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        // use a plain searcher: newSearcher may wrap with AssertingIndexSearcher, whose
        // ScorerSupplier sometimes builds the bulk scorer from scorer() instead of delegating to
        // bulkScorer(), which breaks the sanity check below for some seeds
        IndexSearcher searcher = new IndexSearcher(reader);
        // disable query caching, so that we exercise the search path directly rather than
        // any bulk-scoring optimizations the cache may apply internally
        searcher.setQueryCache(null);

        Query throwing = new ThrowsOnBulkScoreQuery(new TermQuery(new Term("field", "term1")));

        // sanity check that the wrapped query really does throw when bulk-scored directly
        expectThrows(AssertionError.class, () -> searcher.search(throwing, 10));

        Query wrapped = new ForceNoBulkScoringQuery(throwing);
        // should not throw, because bulk scoring has been disabled
        assertEquals(1, searcher.search(wrapped, 10).totalHits.value());
      }
    }
  }

  /**
   * A query wrapper whose ScorerSupplier throws an AssertionError if bulkScorer() is called, used
   * to check that {@link ForceNoBulkScoringQuery} never calls bulkScorer() on the ScorerSupplier of
   * the query it wraps.
   */
  private static class ThrowsOnBulkScoreQuery extends Query {

    private final Query in;

    private ThrowsOnBulkScoreQuery(Query in) {
      this.in = in;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      Query rewritten = in.rewrite(indexSearcher);
      if (rewritten != in) {
        return new ThrowsOnBulkScoreQuery(rewritten);
      }
      return super.rewrite(indexSearcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      Weight innerWeight = in.createWeight(searcher, scoreMode, boost);
      return new FilterWeight(innerWeight) {
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier innerSupplier = super.scorerSupplier(context);
          if (innerSupplier == null) {
            return null;
          }
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return innerSupplier.get(leadCost);
            }

            @Override
            public long cost() {
              return innerSupplier.cost();
            }

            @Override
            public BulkScorer bulkScorer() {
              throw new AssertionError("bulkScorer() should not have been called");
            }
          };
        }
      };
    }

    @Override
    public String toString(String field) {
      return "ThrowsOnBulkScore(" + in.toString(field) + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      in.visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
      return sameClassAs(o) && in.equals(((ThrowsOnBulkScoreQuery) o).in);
    }

    @Override
    public int hashCode() {
      return classHash() * 31 + in.hashCode();
    }
  }
}
