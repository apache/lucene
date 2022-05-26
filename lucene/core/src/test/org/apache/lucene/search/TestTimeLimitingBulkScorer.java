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
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests the {@link TimeLimitingBulkScorer}. */
@LuceneTestCase.SuppressSysoutChecks(
    bugUrl = "http://test.is.timing.sensitive.so.it.prints.instead.of.failing")
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
    top = searcher.search(query, n, 5);

    if (top != null) {
      hits = top.scoreDocs;
      assertTrue("Partial result", hits.length > 0 && hits.length < n);
    } else {
      System.out.println("No results found");
    }
    directoryReader.close();
    directory.close();
  }
}
