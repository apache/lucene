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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestWeight extends LuceneTestCase {

  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w =
        new RandomIndexWriter(
            random(), dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    for (int i = 0; i < 200; ++i) {
      Document doc = new Document();
      if (i == 42) {
        doc.add(new StringField("f1", "bar", Field.Store.NO));
        doc.add(new LongPoint("f2", 42L));
        doc.add(new NumericDocValuesField("f2", 42L));
      } else if (i == 100) {
        doc.add(new StringField("f1", "foo", Field.Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      } else {
        doc.add(new StringField("f1", "bar", Field.Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    w.close();

    final Query boolQuery =
        new BooleanQuery.Builder()
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 2),
                    NumericDocValuesField.newSlowRangeQuery("f2", 2L, 2L)),
                BooleanClause.Occur.MUST)
            .build();

    TopDocs topDocs = searcher.search(boolQuery, 300);
    assertEquals(199, topDocs.totalHits.value());
    reader.close();
    dir.close();
  }
}
