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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.hamcrest.MatcherAssert;

public class TestProfilerCollector extends LuceneTestCase {

  public void testCollector() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = TestUtil.nextInt(random(), 1, 100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();

    IndexSearcher searcher = newSearcher(reader);

    ProfilerCollectorManager profilerCollectorManager =
        new ProfilerCollectorManager("total_hits") {
          @Override
          protected Collector createCollector() {
            return new TotalHitCountCollector();
          }
        };
    Query query = new TermQuery(new Term("foo", "bar"));
    ProfilerCollectorResult profileResult = searcher.search(query, profilerCollectorManager);

    MatcherAssert.assertThat(profileResult.getReason(), equalTo("total_hits"));
    MatcherAssert.assertThat(profileResult.getName(), equalTo("TotalHitCountCollector"));
    MatcherAssert.assertThat(profileResult.getTime(), greaterThan(0L));

    reader.close();
    dir.close();
  }

  public void testProfilerCollectorCustomName() {
    ProfilerCollector collector =
        new ProfilerCollector(
            new TestCollector(new TotalHitCountCollector()), "filter", List.of()) {
          @Override
          protected String deriveCollectorName(Collector c) {
            return c.toString();
          }
        };

    assertEquals("TestCollector(TotalHitCountCollector)", collector.getName());
  }

  public void testCompetitiveIteratorDelegation() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = TestUtil.nextInt(random(), 10, 100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new LongPoint("score", i));
      doc.add(new NumericDocValuesField("score", i));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    DirectoryReader reader = w.getReader();
    w.close();

    SortField sortField = new SortField("score", SortField.Type.LONG);
    TopFieldCollectorManager topFieldCollectorManager =
        new TopFieldCollectorManager(new Sort(sortField), 1, Integer.MAX_VALUE);

    ProfilerCollectorWrapper wrapper =
        new ProfilerCollectorWrapper(topFieldCollectorManager.newCollector());
    LeafCollector leafCollector = wrapper.getLeafCollector(reader.leaves().get(0));

    DocIdSetIterator competitiveIterator = leafCollector.competitiveIterator();
    assertNotNull(competitiveIterator);

    reader.close();
    dir.close();
  }

  private static final class TestCollector extends FilterCollector {
    TestCollector(Collector in) {
      super(in);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.getClass().getSimpleName() + ")";
    }
  }
}
