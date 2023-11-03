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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTotalHitCountCollector extends LuceneTestCase {

  public void testBasics() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StringField("string", "a" + i, Field.Store.NO));
      doc.add(new StringField("string", "b" + i, Field.Store.NO));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader, true, true, random().nextBoolean());
    TotalHitCountCollectorManager collectorManager = new TotalHitCountCollectorManager();
    int totalHits = searcher.search(new MatchAllDocsQuery(), collectorManager);
    assertEquals(5, totalHits);

    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "a1")), Occur.SHOULD)
            .add(new TermQuery(new Term("string", "b3")), Occur.SHOULD)
            .build();
    totalHits = searcher.search(query, collectorManager);
    assertEquals(2, totalHits);

    reader.close();
    indexStore.close();
  }
}
