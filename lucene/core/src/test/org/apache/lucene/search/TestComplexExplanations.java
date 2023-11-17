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

import java.util.Arrays;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.tests.search.BaseExplanationTestCase;

/**
 * TestExplanations subclass that builds up super crazy complex queries on the assumption that if
 * the explanations work out right for them, they should work for anything.
 */
public class TestComplexExplanations extends BaseExplanationTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // TODO: switch to BM25?
    searcher.setSimilarity(new ClassicSimilarity());
  }

  @Override
  public void tearDown() throws Exception {
    searcher.setSimilarity(IndexSearcher.getDefaultSimilarity());
    super.tearDown();
  }

  // :TODO: we really need more crazy complex cases.

  // //////////////////////////////////////////////////////////////////

  // The rest of these aren't that complex, but they are <i>somewhat</i>
  // complex, and they expose weakness in dealing with queries that match
  // with scores of 0 wrapped in other queries

  public void testT3() throws Exception {
    TermQuery query = new TermQuery(new Term(FIELD, "w1"));
    bqtest(new BoostQuery(query, 0), new int[] {0, 1, 2, 3});
  }

  public void testMA3() throws Exception {
    Query q = new MatchAllDocsQuery();
    bqtest(new BoostQuery(q, 0), new int[] {0, 1, 2, 3});
  }

  public void testFQ5() throws Exception {
    TermQuery query = new TermQuery(new Term(FIELD, "xx"));
    Query filtered =
        new BooleanQuery.Builder()
            .add(new BoostQuery(query, 0), Occur.MUST)
            .add(matchTheseItems(new int[] {1, 3}), Occur.FILTER)
            .build();
    bqtest(filtered, new int[] {3});
  }

  public void testCSQ4() throws Exception {
    Query q = new ConstantScoreQuery(matchTheseItems(new int[] {3}));
    bqtest(new BoostQuery(q, 0), new int[] {3});
  }

  public void testDMQ10() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    query.add(new BoostQuery(boostedQuery, 100), Occur.SHOULD);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));

    DisjunctionMaxQuery q =
        new DisjunctionMaxQuery(
            Arrays.asList(query.build(), new BoostQuery(xxBoostedQuery, 0)), 0.5f);
    bqtest(new BoostQuery(q, 0), new int[] {0, 2, 3});
  }

  public void testMPQ7() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1"}));
    qb.add(ta(new String[] {"w2"}));
    qb.setSlop(1);
    bqtest(new BoostQuery(qb.build(), 0), new int[] {0, 1, 2});
  }

  public void testBQ12() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w2"));
    query.add(new BoostQuery(boostedQuery, 0), Occur.SHOULD);

    qtest(query.build(), new int[] {0, 1, 2, 3});
  }

  public void testBQ13() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    query.add(new BoostQuery(boostedQuery, 0), Occur.MUST_NOT);

    qtest(query.build(), new int[] {1, 2, 3});
  }

  public void testBQ18() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    query.add(new BoostQuery(boostedQuery, 0), Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);

    qtest(query.build(), new int[] {0, 1, 2, 3});
  }

  public void testBQ21() throws Exception {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST);
    builder.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);

    Query query = builder.build();

    bqtest(new BoostQuery(query, 0), new int[] {0, 1, 2, 3});
  }

  public void testBQ22() throws Exception {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    builder.add(new BoostQuery(boostedQuery, 0), Occur.MUST);
    builder.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);
    BooleanQuery query = builder.build();

    bqtest(new BoostQuery(query, 0), new int[] {0, 1, 2, 3});
  }
}
