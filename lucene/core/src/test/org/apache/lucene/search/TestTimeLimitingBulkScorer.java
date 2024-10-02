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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.QueryTimeoutImpl;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;

/** Tests the {@link TimeLimitingBulkScorer}. */
public class TestTimeLimitingBulkScorer extends LuceneTestCase {

  public void testTimeLimitingBulkScorer() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
    int n = 10000;
    for (int i = 0; i < n; i++) {
      Document d = new Document();
      d.add(newTextField("default", "ones ", Field.Store.YES));
      writer.addDocument(d);
    }
    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    IndexSearcher searcher;
    TopDocs top;
    ScoreDoc[] hits = null;

    Query query = new TermQuery(new Term("default", "ones"));
    directoryReader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(directoryReader);
    searcher.setTimeout(countingQueryTimeout(10));
    top = searcher.search(query, n);
    hits = top.scoreDocs;
    assertTrue(
        "Partial result and is aborted is true",
        hits.length > 0 && hits.length < n && searcher.timedOut());
    directoryReader.close();
    directory.close();
  }

  public void testExponentialRate() throws Exception {
    int MAX_DOCS = DocIdSetIterator.NO_MORE_DOCS - 1;
    var bulkScorer =
        new BulkScorer() {
          int expectedInterval = TimeLimitingBulkScorer.INTERVAL;
          int lastMax = 0;
          int lastInterval = 0;

          @Override
          public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
              throws IOException {
            var difference = max - min;
            // the rate shouldn't overflow - only increase or remain equal
            assertTrue("Rate should only go up", difference >= lastInterval);
            // make sure no documents end up being skipped
            assertTrue("Documents skipped", lastMax == min);
            // the difference should be exactly the step except the last run where the difference is
            // smaller since there are not enough docs
            assertTrue(
                "Incorrect rate encountered",
                max == MAX_DOCS ? expectedInterval >= difference : expectedInterval == difference);

            lastMax = max;
            lastInterval = difference;

            // use integer sum since the exponential growth formula yields different result due to
            // rounding
            expectedInterval = expectedInterval + expectedInterval / 2;
            // overflow - stop at the previous one
            if (expectedInterval < 0) {
              expectedInterval = lastInterval;
            }
            return max;
          }

          @Override
          public long cost() {
            return 1;
          }
        };

    var scorer = new TimeLimitingBulkScorer(bulkScorer, new QueryTimeoutImpl(-1));
    scorer.score(dummyCollector(), new Bits.MatchAllBits(Integer.MAX_VALUE), 0, MAX_DOCS);
  }

  private static QueryTimeout countingQueryTimeout(int timeallowed) {

    return new QueryTimeout() {
      int counter = 0;

      @Override
      public boolean shouldExit() {
        counter++;
        if (counter == timeallowed) {
          return true;
        }
        return false;
      }
    };
  }

  private static LeafCollector dummyCollector() {
    return new SimpleCollector() {
      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      public void collect(int doc) throws IOException {}
    };
  }
}
