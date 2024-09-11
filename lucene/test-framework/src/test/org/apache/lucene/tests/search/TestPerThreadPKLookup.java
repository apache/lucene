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
package org.apache.lucene.tests.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.PerThreadPKLookup;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestPerThreadPKLookup extends LuceneTestCase {

  public void testPKLookupWithUpdate() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer =
        new IndexWriter(
            dir,
            new IndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE));

    Document doc;
    doc = new Document();
    doc.add(new KeywordField("PK", "1", Field.Store.NO));
    doc.add(new KeywordField("version", "1", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK", "1", Field.Store.NO));
    doc.add(new KeywordField("version", "2", Field.Store.NO));
    writer.updateDocument(new Term("PK", "1"), doc);

    doc = new Document();
    doc.add(new KeywordField("PK", "1", Field.Store.NO));
    doc.add(new KeywordField("version", "3", Field.Store.NO));
    // PK updates will be merged to one update.
    writer.updateDocument(new Term("PK", "1"), doc);
    writer.flush();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    PerThreadPKLookup pk = new PerThreadPKLookup(reader, "PK");

    int docID = pk.lookup(newBytesRef("1"));
    assertEquals(2, docID);

    reader.close();
    dir.close();
  }
}
