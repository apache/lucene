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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestIndexOptions extends LuceneTestCase {

  public void testChangeIndexOptionsViaAddDocument() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsViaAddDocument(from, to, false);
      }
    }
  }

  private void doTestChangeIndexOptionsViaAddDocument(
      IndexOptions from, IndexOptions to, boolean freeze) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    if (freeze) ft1.freeze();
    w.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    if (freeze) ft2.freeze();
    if (from == to) {
      w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))); // no exception
    } else {
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () -> w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))));
      assertEquals(
          "Inconsistency of field data structures across documents for field [foo] of doc [1]."
              + " index options: expected '"
              + from
              + "', but it has '"
              + to
              + "'.",
          e.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testChangeIndexOptionsViaAddIndexesCodecReader() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsAddIndexesCodecReader(from, to);
      }
    }
  }

  private void doTestChangeIndexOptionsAddIndexesCodecReader(IndexOptions from, IndexOptions to)
      throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    w1.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    w2.addDocument(Collections.singleton(new Field("foo", "bar", ft2)));

    try (CodecReader cr = (CodecReader) getOnlyLeafReader(DirectoryReader.open(w2))) {
      if (from == to) {
        w1.addIndexes(cr); // no exception
        w1.forceMerge(1);
        try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w1))) {
          IndexOptions expected = from == IndexOptions.NONE ? to : from;
          assertEquals(expected, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
        }
      } else {
        IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> w1.addIndexes(cr));
        assertEquals(
            "cannot change field \"foo\" from index options="
                + from
                + " to inconsistent index options="
                + to,
            e.getMessage());
      }
    }

    IOUtils.close(w1, w2, dir1, dir2);
  }

  public void testChangeIndexOptionsViaAddIndexesDirectory() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsAddIndexesDirectory(from, to);
      }
    }
  }

  private void doTestChangeIndexOptionsAddIndexesDirectory(IndexOptions from, IndexOptions to)
      throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(from);
    w1.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig());
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(to);
    w2.addDocument(Collections.singleton(new Field("foo", "bar", ft2)));
    w2.close();

    if (from == to) {
      w1.addIndexes(dir2); // no exception
      w1.forceMerge(1);
      try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w1))) {
        IndexOptions expected = from == IndexOptions.NONE ? to : from;
        assertEquals(expected, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
      }
    } else {
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> w1.addIndexes(dir2));
      assertEquals(
          "cannot change field \"foo\" from index options="
              + from
              + " to inconsistent index options="
              + to,
          e.getMessage());
    }

    IOUtils.close(w1, dir1, dir2);
  }

  public void testChangeIndexOptionsViaAddDocumentFrozenFieldType() throws IOException {
    for (IndexOptions from : IndexOptions.values()) {
      for (IndexOptions to : IndexOptions.values()) {
        doTestChangeIndexOptionsViaAddDocument(from, to, true);
      }
    }
  }

  public void testSameFrozenFieldTypeAcrossManyDocuments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.freeze();
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "bar" + i, ft)));
    }
    try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
      assertEquals(numDocs, r.maxDoc());
      assertEquals(
          IndexOptions.DOCS_AND_FREQS, r.getFieldInfos().fieldInfo("foo").getIndexOptions());
    }
    w.close();
    dir.close();
  }

  public void testMultiValuedFieldSameFrozenFieldType() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.freeze();
    Document doc = new Document();
    doc.add(new Field("foo", "bar1", ft));
    doc.add(new Field("foo", "bar2", ft));
    doc.add(new Field("foo", "bar3", ft));
    w.addDocument(doc);
    try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
      assertEquals(1, r.maxDoc());
      assertEquals(3, r.storedFields().document(0).getValues("foo").length);
    }
    w.close();
    dir.close();
  }

  public void testMultiValuedFieldMixedFrozenAndUnfrozenCompatibleTypes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType frozenFt = new FieldType(TextField.TYPE_STORED);
    frozenFt.freeze();
    // Unfrozen copy with same settings
    FieldType unfrozenFt = new FieldType(TextField.TYPE_STORED);
    Document doc = new Document();
    doc.add(new Field("foo", "bar1", frozenFt));
    doc.add(new Field("foo", "bar2", unfrozenFt));
    w.addDocument(doc);
    try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
      assertEquals(1, r.maxDoc());
      assertEquals(2, r.storedFields().document(0).getValues("foo").length);
    }
    w.close();
    dir.close();
  }

  public void testMultiValuedFieldMixedFrozenIncompatibleTypes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType frozenFt = new FieldType(TextField.TYPE_STORED);
    frozenFt.setIndexOptions(IndexOptions.DOCS);
    frozenFt.freeze();
    FieldType differentFt = new FieldType(TextField.TYPE_STORED);
    differentFt.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    differentFt.freeze();
    Document doc = new Document();
    doc.add(new Field("foo", "bar1", frozenFt));
    doc.add(new Field("foo", "bar2", differentFt));
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
    assertTrue(e.getMessage(), e.getMessage().contains("index options"));
    w.close();
    dir.close();
  }

  public void testFrozenFieldTypeTransitionAcrossDocuments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // First doc uses one frozen type
    FieldType ft1 = new FieldType(TextField.TYPE_STORED);
    ft1.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft1.freeze();
    w.addDocument(Collections.singleton(new Field("foo", "bar", ft1)));
    // Second doc uses a different frozen type with different options — must fail
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(IndexOptions.DOCS);
    ft2.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))));
    assertEquals(
        "Inconsistency of field data structures across documents for field [foo] of doc [1]."
            + " index options: expected '"
            + IndexOptions.DOCS_AND_FREQS
            + "', but it has '"
            + IndexOptions.DOCS
            + "'.",
        e.getMessage());
    w.close();
    dir.close();
  }

  public void testFrozenFieldTypeCacheRestabilizes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // Two distinct frozen FieldType instances with identical settings
    FieldType ftA = new FieldType(TextField.TYPE_STORED);
    ftA.freeze();
    FieldType ftB = new FieldType(TextField.TYPE_STORED);
    ftB.freeze();
    assert ftA != ftB; // different instances
    // First batch with ftA
    for (int i = 0; i < 10; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "val" + i, ftA)));
    }
    // Switch to ftB (same schema, different instance) — triggers cache invalidation + restabilize
    for (int i = 10; i < 20; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "val" + i, ftB)));
    }
    // Switch back to ftA
    for (int i = 20; i < 30; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "val" + i, ftA)));
    }
    try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
      assertEquals(30, r.maxDoc());
    }
    w.close();
    dir.close();
  }

  public void testFrozenFieldTypeAcrossSegments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.freeze();
    // Index several docs in the first segment using the frozen fast-path
    for (int i = 0; i < 10; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "val" + i, ft)));
    }
    w.commit();
    // Same frozen type in a new segment — should succeed
    for (int i = 10; i < 20; i++) {
      w.addDocument(Collections.singleton(new Field("foo", "val" + i, ft)));
    }
    w.commit();
    try (DirectoryReader r = DirectoryReader.open(w)) {
      assertEquals(20, r.numDocs());
    }
    // Different frozen type in a third segment — must fail against global field infos
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setIndexOptions(IndexOptions.DOCS);
    ft2.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> w.addDocument(Collections.singleton(new Field("foo", "bar", ft2))));
    assertTrue(e.getMessage(), e.getMessage().contains("index options"));
    w.close();
    dir.close();
  }

  public void testFrozenFieldTypeInDocumentBlock() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setParentField("__parent");
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.freeze();
    List<Document> block = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Document child = new Document();
      child.add(new Field("foo", "child" + i, ft));
      block.add(child);
    }
    Document parent = new Document();
    parent.add(new Field("foo", "parent", ft));
    block.add(parent);
    w.addDocuments(block);
    try (LeafReader r = getOnlyLeafReader(DirectoryReader.open(w))) {
      assertEquals(6, r.maxDoc());
    }
    w.close();
    dir.close();
  }
}
