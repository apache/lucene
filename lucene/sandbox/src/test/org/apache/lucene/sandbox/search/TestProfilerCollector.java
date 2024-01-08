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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
    final int numDocs = TestUtil.nextInt(random(), 1, 20);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();

    ProfilerCollector collector =
        new ProfilerCollector(new TotalHitCountCollector(), "total_hits", List.of());
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new TermQuery(new Term("foo", "bar"));
    searcher.search(query, collector);

    MatcherAssert.assertThat(collector.getReason(), equalTo("total_hits"));
    MatcherAssert.assertThat(collector.getName(), equalTo("TotalHitCountCollector"));
    ProfilerCollectorResult profileResult = collector.getProfileResult();
    MatcherAssert.assertThat(profileResult.getReason(), equalTo("total_hits"));
    MatcherAssert.assertThat(profileResult.getName(), equalTo("TotalHitCountCollector"));
    MatcherAssert.assertThat(profileResult.getTime(), greaterThan(0L));
    MatcherAssert.assertThat(profileResult.getTime(), equalTo(collector.getTime()));

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
