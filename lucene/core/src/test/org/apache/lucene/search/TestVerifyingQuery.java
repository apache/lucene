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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestVerifyingQuery extends LuceneTestCase {
  public void testTermQueries() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    final int numDocs = random().nextInt(5000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (random().nextBoolean()) {
        doc.add(new StringField("f1", "0", Field.Store.NO));
      } else {
        doc.add(new StringField("f1", "1", Field.Store.NO));
      }
      writer.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    Query q0 = new TermQuery(new Term("f1", "0"));
    Query q1 = new TermQuery(new Term("f1", "1"));

    searcher.search(new VerifyingQuery(q0, q0), numDocs);
    assertThrows(
        RuntimeException.class, () -> searcher.search(new VerifyingQuery(q0, q1), numDocs));

    reader.close();
    writer.close();
    dir.close();
  }

  public void testIndexOrDocValuesQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    final int numDocs = random().nextInt(5000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new LongPoint("f2", 42L));
      doc.add(new SortedNumericDocValuesField("f2", 42L));
      writer.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    final IndexOrDocValuesQuery query =
        new IndexOrDocValuesQuery(
            LongPoint.newExactQuery("f2", 42),
            SortedNumericDocValuesField.newSlowRangeQuery("f2", 42, 42L));

    searcher.search(new VerifyingQuery(query), numDocs);

    reader.close();
    writer.close();
    dir.close();
  }
}
