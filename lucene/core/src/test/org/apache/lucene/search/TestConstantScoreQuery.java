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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * This class only tests some basic functionality in CSQ, the main parts are mostly tested by
 * MultiTermQuery tests, explanations seems to be tested in TestExplanations!
 */
public class TestConstantScoreQuery extends LuceneTestCase {

  public void testCSQ() throws Exception {
    final Query q1 = new ConstantScoreQuery(new TermQuery(new Term("a", "b")));
    final Query q2 = new ConstantScoreQuery(new TermQuery(new Term("a", "c")));
    final Query q3 =
        new ConstantScoreQuery(TermRangeQuery.newStringRange("a", "b", "c", true, true));
    QueryUtils.check(q1);
    QueryUtils.check(q2);
    QueryUtils.checkEqual(q1, q1);
    QueryUtils.checkEqual(q2, q2);
    QueryUtils.checkEqual(q3, q3);
    QueryUtils.checkUnequal(q1, q2);
    QueryUtils.checkUnequal(q2, q3);
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, new TermQuery(new Term("a", "b")));
  }

  // a query for which other queries don't have special rewrite rules
  private static class QueryWrapper extends Query {

    private final Query in;

    QueryWrapper(Query in) {
      this.in = in;
    }

    @Override
    public String toString(String field) {
      return "MockQuery";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return in.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      in.visit(visitor);
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && in.equals(((QueryWrapper) other).in);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + in.hashCode();
    }
  }

  public void testConstantScoreQueryAndFilter() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "a", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("field", "b", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    Query filterB = new QueryWrapper(new TermQuery(new Term("field", "b")));
    Query query = new ConstantScoreQuery(filterB);

    IndexSearcher s = newSearcher(r);
    Query filtered =
        new BooleanQuery.Builder().add(query, Occur.MUST).add(filterB, Occur.FILTER).build();
    assertEquals(1, s.count(filtered)); // Query for field:b, Filter field:b

    Query filterA = new QueryWrapper(new TermQuery(new Term("field", "a")));
    query = new ConstantScoreQuery(filterA);

    filtered = new BooleanQuery.Builder().add(query, Occur.MUST).add(filterB, Occur.FILTER).build();
    assertEquals(0, s.count(filtered)); // Query field:b, Filter field:a

    r.close();
    d.close();
  }

  public void testPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    Query q = searcher.rewrite(new ConstantScoreQuery(pq));

    final Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testRewriteBubblesUpMatchNoDocsQuery() throws IOException {
    IndexSearcher searcher = newSearcher(new MultiReader());

    Query query = new ConstantScoreQuery(MatchNoDocsQuery.INSTANCE);
    assertEquals(MatchNoDocsQuery.INSTANCE, searcher.rewrite(query));
  }
}
