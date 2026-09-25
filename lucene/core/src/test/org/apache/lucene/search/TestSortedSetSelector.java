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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;

/**
 * Tests for SortedSetSortField selectors other than MIN, these require optional codec support
 * (random access to ordinals)
 */
@SuppressCodecs({"SimpleText"})
public class TestSortedSetSelector extends LuceneTestCase {

  public void testMax() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);

    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'baz' comes before 'foo'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMaxReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);

    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'baz' comes before 'foo'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMaxMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);

    SortField sortField =
        new SortedSetSortField("value", false, SortedSetSelector.Type.MAX, SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // null comes first
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    // 'baz' comes before 'foo'
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMaxMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("foo")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);

    SortField sortField =
        new SortedSetSortField("value", false, SortedSetSelector.Type.MAX, SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // 'baz' comes before 'foo'
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMaxSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMin() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMinReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMinMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField =
        new SortedSetSortField(
            "value", false, SortedSetSelector.Type.MIDDLE_MIN, SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // null comes first
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    // 'b' comes before 'c'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMinMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField =
        new SortedSetSortField(
            "value", false, SortedSetSelector.Type.MIDDLE_MIN, SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMinSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MIN));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMax() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMaxReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", true, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMaxMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField =
        new SortedSetSortField(
            "value", false, SortedSetSelector.Type.MIDDLE_MAX, SortField.STRING_FIRST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // null comes first
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    // 'b' comes before 'c'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMaxMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("a")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("c")));
    doc.add(new SortedSetDocValuesField("value", newBytesRef("d")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("b")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    SortField sortField =
        new SortedSetSortField(
            "value", false, SortedSetSelector.Type.MIDDLE_MAX, SortField.STRING_LAST);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(3, td.totalHits.value());
    // 'b' comes before 'c'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMiddleMaxSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("baz")));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", newBytesRef("bar")));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    // slow wrapper does not support random access ordinals (there is no need for that!)
    IndexSearcher searcher = newSearcher(ir, false);
    Sort sort = new Sort(new SortedSetSortField("value", false, SortedSetSelector.Type.MIDDLE_MAX));

    TopDocs td = searcher.search(MatchAllDocsQuery.INSTANCE, 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'bar' comes before 'baz'
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  /**
   * The selector views cache the selected ordinal of the current doc. A bulk {@link
   * SortedDocValues#intoBitSet} delegated to the wrapped multi-valued iterator moves it without
   * going through the view's own nextDoc/advance, so the cached ordinal must be refreshed for the
   * doc the view ends up positioned on.
   */
  public void testIntoBitSetRefreshesCachedOrd() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(), dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    int numDocs = TestUtil.nextInt(random(), 50, 200);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      // the first doc is always multi-valued so that the field is never exposed as a singleton
      int numValues = i == 0 ? 2 : random().nextInt(4);
      for (int j = 0; j < numValues; j++) {
        doc.add(
            new SortedSetDocValuesField(
                "value", newBytesRef(TestUtil.randomSimpleString(random(), 1, 3))));
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    IndexReader ir = writer.getReader();
    writer.close();
    LeafReader leaf = getOnlyLeafReader(ir);
    assertNull(DocValues.unwrapSingleton(DocValues.getSortedSet(leaf, "value")));

    for (SortedSetSelector.Type type : SortedSetSelector.Type.values()) {
      SortedDocValues actual = SortedSetSelector.wrap(DocValues.getSortedSet(leaf, "value"), type);
      SortedDocValues expected =
          SortedSetSelector.wrap(DocValues.getSortedSet(leaf, "value"), type);
      FixedBitSet bits = new FixedBitSet(leaf.maxDoc());

      int doc = actual.nextDoc();
      while (doc != DocIdSetIterator.NO_MORE_DOCS) {
        int upTo = Math.min(leaf.maxDoc(), doc + 1 + random().nextInt(10));
        actual.intoBitSet(upTo, bits, 0);
        doc = actual.docID();
        assertTrue(doc >= upTo);
        if (doc != DocIdSetIterator.NO_MORE_DOCS) {
          assertTrue(expected.advanceExact(doc));
          assertEquals("type=" + type + " doc=" + doc, expected.ordValue(), actual.ordValue());
          if (random().nextBoolean()) {
            doc = actual.nextDoc();
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
              assertTrue(expected.advanceExact(doc));
              assertEquals("type=" + type + " doc=" + doc, expected.ordValue(), actual.ordValue());
            }
          }
        }
      }

      // every doc with a value was collected exactly once by the bulk calls, or visited by nextDoc
      SortedDocValues all = SortedSetSelector.wrap(DocValues.getSortedSet(leaf, "value"), type);
      int visited = 0;
      for (int d = all.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = all.nextDoc()) {
        visited++;
      }
      assertTrue(bits.cardinality() <= visited);
    }

    ir.close();
    dir.close();
  }
}
