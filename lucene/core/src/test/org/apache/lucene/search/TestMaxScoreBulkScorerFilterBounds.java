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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Regression test for a bug where MaxScoreBulkScorer could score past leaf maxDoc when a
 * restrictive filter and disjunction were used together.
 */
public class TestMaxScoreBulkScorerFilterBounds extends LuceneTestCase {

  public void testFilteredDisjunctionDoesNotScorePastMaxDoc() throws Exception {
  Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      // Create a small index where one clause matches more docs than the other, and a restrictive
      // filter
      for (int i = 0; i < 200; i++) {
        Document d = new Document();
        // Clause A matches ~1/3
        d.add(new StringField("a", (i % 3 == 0) ? "yes" : "no", Field.Store.NO));
        // Clause B matches ~1/9
        d.add(new StringField("b", (i % 9 == 0) ? "yes" : "no", Field.Store.NO));
        // Restrictive filter matches ~1%
        d.add(new StringField("f", (i % 100 == 0) ? "on" : "off", Field.Store.NO));
        w.addDocument(d);
      }
    }

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      Query disjunction =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term("a", "yes")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term("b", "yes")), BooleanClause.Occur.SHOULD)
              .build();

      Query filter = new TermQuery(new Term("f", "on"));

      Query filtered =
          new BooleanQuery.Builder()
              .add(disjunction, BooleanClause.Occur.SHOULD)
              .add(filter, BooleanClause.Occur.FILTER)
              .build();

      // This triggers TOP_SCORES path internally; just execute to ensure no exceptions
      TopDocs td = searcher.search(filtered, 10);
      assertNotNull(td);
    } finally {
      dir.close();
    }
  }
}

