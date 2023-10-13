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

package org.apache.lucene.sandbox.search;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.RandomApproximationQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestQueryProfilerIndexSearcher extends LuceneTestCase {

  private static Directory dir;
  private static IndexReader reader;

  @BeforeClass
  public static void setup() throws IOException {
    dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = TestUtil.nextInt(random(), 1, 20);
    for (int i = 0; i < numDocs; ++i) {
      final int numHoles = random().nextInt(5);
      for (int j = 0; j < numHoles; ++j) {
        w.addDocument(new Document());
      }
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
      w.addDocument(doc);
    }
    reader = w.getReader();
    w.close();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    IOUtils.close(reader, dir);
    dir = null;
    reader = null;
  }

  public void testBasic() throws IOException {
    QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query query = new TermQuery(new Term("foo", "bar"));
    searcher.search(query, 1);

    List<QueryProfilerResult> results = searcher.getProfileResult();
    assertEquals(1, results.size());
    Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.COUNT.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.SCORE.toString()), greaterThan(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.MATCH.toString()), equalTo(0L));

    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString() + "_count"),
        greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.SCORE.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.MATCH.toString() + "_count"), equalTo(0L));

    long rewriteTime = searcher.getRewriteTime();
    MatcherAssert.assertThat(rewriteTime, greaterThan(0L));
  }

  public void testTwoQueries() throws IOException {
    QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query firstQuery = new TermQuery(new Term("foo", "bar"));
    searcher.search(firstQuery, 1);

    Query secondQuery = new TermQuery(new Term("foo", "baz"));
    searcher.search(secondQuery, 1);

    List<QueryProfilerResult> results = searcher.getProfileResult();
    assertEquals(2, results.size());

    Map<String, Long> firstResult = results.get(0).getTimeBreakdown();
    MatcherAssert.assertThat(
        firstResult.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));

    Map<String, Long> secondResult = results.get(1).getTimeBreakdown();
    MatcherAssert.assertThat(
        secondResult.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));

    long rewriteTime = searcher.getRewriteTime();
    MatcherAssert.assertThat(rewriteTime, greaterThan(0L));
  }

  public void testNoCaching() throws IOException {
    IndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query query = new TermQuery(new Term("foo", "bar"));
    searcher.search(query, 1);

    LRUQueryCache cache = (LRUQueryCache) searcher.getQueryCache();
    MatcherAssert.assertThat(cache.getHitCount(), equalTo(0L));
    MatcherAssert.assertThat(cache.getCacheCount(), equalTo(0L));
    MatcherAssert.assertThat(cache.getTotalCount(), equalTo(cache.getMissCount()));
    MatcherAssert.assertThat(cache.getCacheSize(), equalTo(0L));
  }

  public void testNoScoring() throws IOException {
    QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query query = new TermQuery(new Term("foo", "bar"));
    searcher.search(query, 1, Sort.INDEXORDER); // scores are not needed

    List<QueryProfilerResult> results = searcher.getProfileResult();
    assertEquals(1, results.size());
    Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.COUNT.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString()), equalTo(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.SCORE.toString()), equalTo(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.MATCH.toString()), equalTo(0L));

    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString() + "_count"),
        greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.SCORE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.MATCH.toString() + "_count"), equalTo(0L));

    long rewriteTime = searcher.getRewriteTime();
    MatcherAssert.assertThat(rewriteTime, greaterThan(0L));
  }

  public void testUseIndexStats() throws IOException {
    QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query query = new TermQuery(new Term("foo", "bar"));
    searcher.count(query); // will use index stats

    List<QueryProfilerResult> results = searcher.getProfileResult();
    assertEquals(1, results.size());
    Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString()), equalTo(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.SCORE.toString()), equalTo(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.MATCH.toString()), equalTo(0L));

    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString() + "_count"),
        greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.SCORE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.MATCH.toString() + "_count"), equalTo(0L));

    long rewriteTime = searcher.getRewriteTime();
    MatcherAssert.assertThat(rewriteTime, greaterThan(0L));
  }

  public void testApproximations() throws IOException {
    QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(reader);
    Query query = new RandomApproximationQuery(new TermQuery(new Term("foo", "bar")), random());
    searcher.count(query);
    List<QueryProfilerResult> results = searcher.getProfileResult();
    assertEquals(1, results.size());
    Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString()), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString()), equalTo(0L));
    MatcherAssert.assertThat(breakdown.get(QueryProfilerTimingType.SCORE.toString()), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.MATCH.toString()), greaterThan(0L));

    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.CREATE_WEIGHT.toString() + "_count"),
        greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.COUNT.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.BUILD_SCORER.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count"), greaterThan(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.ADVANCE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.SCORE.toString() + "_count"), equalTo(0L));
    MatcherAssert.assertThat(
        breakdown.get(QueryProfilerTimingType.MATCH.toString() + "_count"), greaterThan(0L));

    long rewriteTime = searcher.getRewriteTime();
    MatcherAssert.assertThat(rewriteTime, greaterThan(0L));
  }
}
