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
package org.apache.lucene.misc.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestBPReorderingMergePolicy extends LuceneTestCase {

  public void testReorderOnMerge() throws IOException {
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    IndexWriter w1 =
        new IndexWriter(dir1, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(2);
    BPReorderingMergePolicy mp = new BPReorderingMergePolicy(newLogMergePolicy(), reorderer);
    mp.setMinNaturalMergeNumDocs(2);
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig().setMergePolicy(mp));
    Document doc = new Document();
    StringField idField = new StringField("id", "", Store.YES);
    doc.add(idField);
    StringField bodyField = new StringField("body", "", Store.YES);
    doc.add(bodyField);

    for (int i = 0; i < 10000; ++i) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(Integer.toString(i % 2 == 0 ? 0 : i % 10));
      w1.addDocument(doc);
      w2.addDocument(doc);

      if (i % 10 == 0) {
        w1.deleteDocuments(new Term("id", Integer.toString(i / 3)));
        w2.deleteDocuments(new Term("id", Integer.toString(i / 3)));
      }
      if (i % 3 == 0) {
        DirectoryReader.open(w2).close();
      }
    }

    w1.forceMerge(1);
    w2.forceMerge(1);

    IndexReader reader1 = DirectoryReader.open(w1);
    IndexReader reader2 = DirectoryReader.open(w2);
    assertEquals(reader1.maxDoc(), reader2.maxDoc());

    StoredFields storedFields1 = reader1.storedFields();
    StoredFields storedFields2 = reader2.storedFields();

    // Check that data is consistent
    for (int i = 0; i < reader1.maxDoc(); ++i) {
      Document doc1 = storedFields1.document(i);
      String id = doc1.get("id");
      String body = doc1.get("body");

      PostingsEnum pe = reader2.leaves().get(0).reader().postings(new Term("id", id));
      assertNotNull(pe);
      int docID2 = pe.nextDoc();
      assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, docID2);
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());

      Document doc2 = storedFields2.document(docID2);
      assertEquals(id, doc2.get("id"));
      assertEquals(body, doc2.get("body"));
    }

    // Check that reader2 actually got reordered. This can only happen due to BPIndexReorderer since
    // it uses a log merge-policy under the hood, which only merges adjacent segments.
    boolean reordered = false;
    int previousId = -1;
    for (int i = 0; i < reader2.maxDoc(); ++i) {
      Document doc2 = storedFields2.document(i);
      String idString = doc2.get("id");
      int id = Integer.parseInt(idString);
      if (id < previousId) {
        reordered = true;
        break;
      }
      previousId = id;
    }
    assertTrue(reordered);

    SegmentReader sr = (SegmentReader) reader2.leaves().get(0).reader();
    final String reorderedString =
        sr.getSegmentInfo().info.getDiagnostics().get(BPReorderingMergePolicy.REORDERED);
    assertEquals(Boolean.TRUE.toString(), reorderedString);

    IOUtils.close(reader1, reader2, w1, w2, dir1, dir2);
  }

  public void testReorderOnAddIndexes() throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 =
        new IndexWriter(dir1, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));

    Document doc = new Document();
    StringField idField = new StringField("id", "", Store.YES);
    doc.add(idField);
    StringField bodyField = new StringField("body", "", Store.YES);
    doc.add(bodyField);

    for (int i = 0; i < 10000; ++i) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(Integer.toString(i % 2 == 0 ? 0 : i % 10));
      w1.addDocument(doc);

      if (i % 3 == 0) {
        DirectoryReader.open(w1).close();
      }
    }

    for (int i = 0; i < 10000; i += 10) {
      w1.deleteDocuments(new Term("id", Integer.toString(i / 3)));
    }

    Directory dir2 = newDirectory();
    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(2);
    BPReorderingMergePolicy mp = new BPReorderingMergePolicy(newLogMergePolicy(), reorderer);
    mp.setMinNaturalMergeNumDocs(2);
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig().setMergePolicy(mp));
    IndexReader reader1 = DirectoryReader.open(w1);
    CodecReader[] codecReaders =
        reader1.leaves().stream()
            .map(LeafReaderContext::reader)
            .map(
                t -> {
                  try {
                    return SlowCodecReaderWrapper.wrap(t);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .toArray(CodecReader[]::new);
    w2.addIndexes(codecReaders);
    w1.forceMerge(1);

    reader1.close();
    reader1 = DirectoryReader.open(w1);
    IndexReader reader2 = DirectoryReader.open(w2);
    assertEquals(1, reader2.leaves().size());
    assertEquals(reader1.maxDoc(), reader2.maxDoc());

    StoredFields storedFields1 = reader1.storedFields();
    StoredFields storedFields2 = reader2.storedFields();

    // Check that data is consistent
    for (int i = 0; i < reader1.maxDoc(); ++i) {
      Document doc1 = storedFields1.document(i);
      String id = doc1.get("id");
      String body = doc1.get("body");

      PostingsEnum pe = reader2.leaves().get(0).reader().postings(new Term("id", id));
      assertNotNull(pe);
      int docID2 = pe.nextDoc();
      assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, docID2);
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());

      Document doc2 = storedFields2.document(docID2);
      assertEquals(id, doc2.get("id"));
      assertEquals(body, doc2.get("body"));
    }

    // Check that reader2 actually got reordered. This can only happen due to BPIndexReorderer since
    // it uses a log merge-policy under the hood, which only merges adjacent segments.
    boolean reordered = false;
    int previousId = -1;
    for (int i = 0; i < reader2.maxDoc(); ++i) {
      Document doc2 = storedFields2.document(i);
      String idString = doc2.get("id");
      int id = Integer.parseInt(idString);
      if (id < previousId) {
        reordered = true;
        break;
      }
      previousId = id;
    }
    assertTrue(reordered);

    SegmentReader sr = (SegmentReader) reader2.leaves().get(0).reader();
    final String reorderedString =
        sr.getSegmentInfo().info.getDiagnostics().get(BPReorderingMergePolicy.REORDERED);
    assertEquals(Boolean.TRUE.toString(), reorderedString);

    IOUtils.close(reader1, reader2, w1, w2, dir1, dir2);
  }

  public void testReorderDoesntHaveEnoughRAM() throws IOException {
    // This just makes sure that reordering the index on merge does not corrupt its content
    Directory dir = newDirectory();
    BPIndexReorderer reorderer = new BPIndexReorderer();
    reorderer.setMinDocFreq(2);
    reorderer.setMinPartitionSize(2);
    reorderer.setRAMBudgetMB(Double.MIN_VALUE);
    BPReorderingMergePolicy mp = new BPReorderingMergePolicy(newLogMergePolicy(), reorderer);
    mp.setMinNaturalMergeNumDocs(2);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(mp));
    Document doc = new Document();
    StringField idField = new StringField("id", "", Store.YES);
    doc.add(idField);
    StringField bodyField = new StringField("body", "", Store.YES);
    doc.add(bodyField);

    for (int i = 0; i < 10; ++i) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(Integer.toString(i % 2 == 0 ? 0 : i % 10));
      w.addDocument(doc);
      DirectoryReader.open(w).close();
    }

    w.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(w);
    StoredFields storedFields = reader.storedFields();

    // This test fails if exceptions get thrown due to lack of RAM
    // We expect BP to not run, so the doc ID order should not be modified
    for (int i = 0; i < reader.maxDoc(); ++i) {
      Document storedDoc = storedFields.document(i);
      String id = storedDoc.get("id");
      assertEquals(Integer.toString(i), id);
    }

    IOUtils.close(reader, w, dir);
  }
}
