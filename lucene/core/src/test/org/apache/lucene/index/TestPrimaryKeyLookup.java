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
package org.apache.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestPrimaryKeyLookup extends LuceneTestCase {

  public void testReopen() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer =
        new IndexWriter(
            dir,
            new IndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(NoMergePolicy.INSTANCE));

    Document doc;
    doc = new Document();
    doc.add(new KeywordField("PK", "1", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK", "2", Field.Store.NO));
    writer.addDocument(doc);
    writer.flush();

    // Segment with no PK terms.
    doc = new Document();
    doc.add(new KeywordField("PK2", "3", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK2", "4", Field.Store.NO));
    writer.addDocument(doc);
    writer.flush();

    DirectoryReader reader1 = DirectoryReader.open(writer);
    PrimaryKeyLookup pk1 = new PrimaryKeyLookup(reader1, "PK");

    doc = new Document();
    doc.add(new KeywordField("PK", "5", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK", "6", Field.Store.NO));
    writer.addDocument(doc);
    writer.deleteDocuments(new Term("PK", "1"));
    writer.flush();

    doc = new Document();
    doc.add(new KeywordField("PK2", "7", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK2", "8", Field.Store.NO));
    writer.addDocument(doc);
    writer.flush();

    assertEquals(0, pk1.lookup(newBytesRef("1")));
    assertEquals(1, pk1.lookup(newBytesRef("2")));
    assertEquals(-1, pk1.lookup(newBytesRef("5")));
    assertEquals(-1, pk1.lookup(newBytesRef("8")));

    DirectoryReader reader2 = DirectoryReader.openIfChanged(reader1);
    PrimaryKeyLookup pk2 = pk1.reopen(reader2);

    assertEquals(-1, pk2.lookup(newBytesRef("1")));
    assertEquals(1, pk2.lookup(newBytesRef("2")));
    assertEquals(4, pk2.lookup(newBytesRef("5")));
    assertEquals(-1, pk2.lookup(newBytesRef("8")));

    doc = new Document();
    doc.add(new KeywordField("PK", "9", Field.Store.NO));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new KeywordField("PK", "10", Field.Store.NO));
    writer.addDocument(doc);
    writer.flush();

    assertEquals(-1, pk2.lookup(newBytesRef("9")));
    DirectoryReader reader3 = DirectoryReader.openIfChanged(reader2);
    PrimaryKeyLookup pk3 = pk2.reopen(reader3);
    assertEquals(8, pk3.lookup(newBytesRef("9")));

    DirectoryReader reader4 = DirectoryReader.openIfChanged(reader3);
    assertNull(pk3.reopen(reader4));

    writer.close();
    reader1.close();
    reader2.close();
    reader3.close();
    dir.close();
  }

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
    writer.updateDocument(new Term("PK", "1"), doc);
    writer.flush();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    PrimaryKeyLookup pk = new PrimaryKeyLookup(reader, "PK");
    assertEquals(2, pk.lookup(newBytesRef("1")));

    reader.close();
    dir.close();
  }

  /**
   * Constructing on one thread and calling {@link PrimaryKeyLookup#lookup} from another must fire
   * the thread-stickiness assertion.
   */
  public void testThreadStickinessAssertion() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new KeywordField("PK", "1", Field.Store.NO));
    writer.addDocument(doc);
    writer.commit();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    PrimaryKeyLookup pk = new PrimaryKeyLookup(reader, "PK");

    if (TEST_ASSERTS_ENABLED) {
      // pk was constructed on this thread; calling lookup from any other thread must trip
      // the thread-stickiness assertion in PrimaryKeyLookup.
      class CrossThreadLookup extends Thread {
        Throwable thrown;

        @Override
        public void run() {
          try {
            pk.lookup(newBytesRef("1"));
          } catch (Throwable t) {
            thrown = t;
          }
        }
      }

      CrossThreadLookup other = new CrossThreadLookup();
      other.setName("pk-cross-thread-lookup");
      other.start();
      other.join();

      assertTrue(
          "expected AssertionError but got: " + other.thrown,
          other.thrown instanceof AssertionError);
      assertTrue(
          "unexpected message: " + other.thrown.getMessage(),
          other.thrown.getMessage().contains("not thread-safe"));
    }
    reader.close();
    dir.close();
  }
}
