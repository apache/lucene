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
package org.apache.lucene.document.column;

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseDictionaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDictionaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link org.apache.lucene.document.column.DictionaryColumn} end-to-end indexing. */
public class TestColumnBatchDictionaryColumn extends LuceneTestCase {

  private static final List<BytesRef> COLORS =
      List.of(new BytesRef("blue"), new BytesRef("green"), new BytesRef("red"));

  public void testSortedSparse() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // docIds 0,1,2 with ords 2,0,1 → "red","blue","green"
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "color",
                SortedDocValuesField.TYPE,
                COLORS,
                new int[] {0, 1, 2},
                new int[] {2, 0, 1})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");
    assertNotNull(sdv);

    assertEquals(0, sdv.nextDoc());
    assertEquals(new BytesRef("red"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(2, sdv.nextDoc());
    assertEquals(new BytesRef("green"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sdv.nextDoc());
    assertEquals(3, sdv.getValueCount()); // 3 distinct terms used

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Many sparse docs reusing a small dictionary: exercises the sparse SORTED bulk path's ord→hash
   * translation table. The dictionary contains an unused entry which must not be materialized in
   * the segment's term universe.
   */
  public void testSortedSparseLargeBatchReusesDictionary() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    int n = 1000;
    int[] docIds = new int[n];
    int[] ords = new int[n];
    for (int i = 0; i < n; i++) {
      docIds[i] = i;
      ords[i] = i % 2; // only ords 0 and 1 used; ord 2 ("red") is in the dictionary but unused
    }

    w.addBatch(
        simpleBatch(
            n,
            new ArrayDictionaryColumn("color", SortedDocValuesField.TYPE, COLORS, docIds, ords)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");
    assertNotNull(sdv);

    // Only two distinct terms ("blue", "green") were used.
    assertEquals(2, sdv.getValueCount());
    for (int i = 0; i < n; i++) {
      assertEquals(i, sdv.nextDoc());
      BytesRef expected = i % 2 == 0 ? new BytesRef("blue") : new BytesRef("green");
      assertEquals(expected, sdv.lookupOrd(sdv.ordValue()));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sdv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedDense() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // ords: blue, blue, red
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDenseDictionaryColumn(
                "color", SortedDocValuesField.TYPE, COLORS, new int[] {0, 0, 2})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");
    assertNotNull(sdv);

    assertEquals(0, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(2, sdv.nextDoc());
    assertEquals(new BytesRef("red"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sdv.nextDoc());

    // green was in the dictionary but never used; getValueCount must reflect only used terms
    assertEquals(2, sdv.getValueCount());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSet() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // doc 0: blue, green  doc 1: red  doc 2: blue, red
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "color",
                SortedSetDocValuesField.TYPE,
                COLORS,
                new int[] {0, 0, 1, 2, 2},
                new int[] {0, 1, 2, 0, 2})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedSetDocValues ssdv = leaf.getSortedSetDocValues("color");
    assertNotNull(ssdv);

    // doc 0: blue=0, green=1 (sorted ords)
    assertEquals(0, ssdv.nextDoc());
    assertEquals(2, ssdv.docValueCount());
    assertEquals(new BytesRef("blue"), ssdv.lookupOrd(ssdv.nextOrd()));
    assertEquals(new BytesRef("green"), ssdv.lookupOrd(ssdv.nextOrd()));

    // doc 1: red
    assertEquals(1, ssdv.nextDoc());
    assertEquals(1, ssdv.docValueCount());
    assertEquals(new BytesRef("red"), ssdv.lookupOrd(ssdv.nextOrd()));

    // doc 2: blue, red
    assertEquals(2, ssdv.nextDoc());
    assertEquals(2, ssdv.docValueCount());
    assertEquals(new BytesRef("blue"), ssdv.lookupOrd(ssdv.nextOrd()));
    assertEquals(new BytesRef("red"), ssdv.lookupOrd(ssdv.nextOrd()));

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, ssdv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSetDeduplicatesWithinDoc() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // doc 0: blue, blue, red — "blue" should appear only once
    w.addBatch(
        simpleBatch(
            1,
            new ArrayDictionaryColumn(
                "color",
                SortedSetDocValuesField.TYPE,
                COLORS,
                new int[] {0, 0, 0},
                new int[] {0, 0, 2})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedSetDocValues ssdv = leaf.getSortedSetDocValues("color");

    assertEquals(0, ssdv.nextDoc());
    assertEquals(2, ssdv.docValueCount()); // blue + red, not blue + blue + red
    assertEquals(new BytesRef("blue"), ssdv.lookupOrd(ssdv.nextOrd()));
    assertEquals(new BytesRef("red"), ssdv.lookupOrd(ssdv.nextOrd()));

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedSorted = new FieldType(SortedDocValuesField.TYPE);
    storedSorted.setStored(true);
    storedSorted.freeze();

    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "color", storedSorted, COLORS, new int[] {0, 1}, new int[] {2, 0})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields sf = leaf.storedFields();
    Document doc0 = sf.document(0);
    Document doc1 = sf.document(1);

    assertEquals("red", doc0.getBinaryValue("color").utf8ToString());
    assertEquals("blue", doc1.getBinaryValue("color").utf8ToString());

    r.close();
    w.close();
    dir.close();
  }

  public void testDictionaryWithDuplicateEntries() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Dictionary: ["blue", "blue", "red"] — two slots for blue
    List<BytesRef> dict = List.of(new BytesRef("blue"), new BytesRef("blue"), new BytesRef("red"));
    // ords 0 and 1 both mean "blue"; ord 2 means "red"
    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "color",
                SortedDocValuesField.TYPE,
                dict,
                new int[] {0, 1},
                new int[] {0, 1}))); // doc 0 → ord 0 (blue), doc 1 → ord 1 (also blue)

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");

    // Both docs have "blue"; getValueCount should be 1 (not 2)
    assertEquals(0, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.getValueCount());

    r.close();
    w.close();
    dir.close();
  }

  public void testMultipleBatchesDifferentDictionaries() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    List<BytesRef> dict1 = List.of(new BytesRef("blue"), new BytesRef("green"));
    List<BytesRef> dict2 =
        List.of(new BytesRef("green"), new BytesRef("purple"), new BytesRef("red"));

    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "color", SortedDocValuesField.TYPE, dict1, new int[] {0, 1}, new int[] {0, 1})));
    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "color",
                SortedDocValuesField.TYPE,
                dict2,
                new int[] {0, 1},
                new int[] {1, 2}))); // purple, red

    w.forceMerge(1); // merge to one segment; ordinals remapped globally
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");
    assertNotNull(sdv);

    // After merge, all 4 docs present with correct values
    for (int i = 0; i < 4; i++) {
      assertEquals(i, sdv.nextDoc());
    }
    // Verify total distinct values = 4
    assertEquals(4, sdv.getValueCount());

    r.close();
    w.close();
    dir.close();
  }

  public void testMixedWithPlainDocument() throws IOException {
    Directory dir = newDirectory();
    // Disable auto-flush so the addBatch and addDocument share one segment; otherwise a
    // randomized tiny maxBufferedDocs can split them and TMP's size-sorted forceMerge
    // may interleave them.
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // Batch adds doc 0 (blue) and doc 1 (red)
    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "color", SortedDocValuesField.TYPE, COLORS, new int[] {0, 1}, new int[] {0, 2})));

    // Plain document adds "green" for doc 2
    Document doc = new Document();
    doc.add(new SortedDocValuesField("color", new BytesRef("green")));
    w.addDocument(doc);

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues sdv = leaf.getSortedDocValues("color");

    assertEquals(3, sdv.getValueCount()); // blue, green, red

    // Verify each doc
    assertEquals(0, sdv.nextDoc());
    assertEquals(new BytesRef("blue"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(1, sdv.nextDoc());
    assertEquals(new BytesRef("red"), sdv.lookupOrd(sdv.ordValue()));
    assertEquals(2, sdv.nextDoc());
    assertEquals(new BytesRef("green"), sdv.lookupOrd(sdv.ordValue()));

    r.close();
    w.close();
    dir.close();
  }

  public void testOutOfRangeOrdSparseThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ArrayDictionaryColumn col =
        new ArrayDictionaryColumn(
            "color",
            SortedDocValuesField.TYPE,
            COLORS, // length 3
            new int[] {0},
            new int[] {5}); // ord 5 is out of range

    expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(1, col)));

    w.close();
    dir.close();
  }

  public void testOutOfRangeOrdDenseThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ArrayDenseDictionaryColumn col =
        new ArrayDenseDictionaryColumn(
            "color",
            SortedDocValuesField.TYPE,
            COLORS, // length 3
            new int[] {0, 99}); // ord 99 out of range

    expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(2, col)));

    w.close();
    dir.close();
  }

  public void testEmptyDictionaryThrows() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new ArrayDictionaryColumn(
                "f", SortedDocValuesField.TYPE, List.of(), new int[0], new int[0]));
  }

  public void testNullDictionaryEntryThrows() {
    List<BytesRef> dict = Arrays.asList(new BytesRef("a"), null);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new ArrayDictionaryColumn(
                "f", SortedDocValuesField.TYPE, dict, new int[0], new int[0]));
  }

  public void testIncompatibleDocValuesTypeThrows() throws IOException {
    // NUMERIC is not allowed for DictionaryColumn
    FieldType numericType = new FieldType();
    numericType.setDocValuesType(DocValuesType.NUMERIC);
    numericType.freeze();
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      ArrayDictionaryColumn col =
          new ArrayDictionaryColumn("f", numericType, COLORS, new int[] {0}, new int[] {0});
      expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(1, col)));
    }
    dir.close();
  }

  public void testRejectsBinaryDocValues() throws IOException {
    // BINARY DV writer doesn't dedup terms, so DictionaryColumn buys nothing there;
    // validation should redirect to BinaryColumn.
    FieldType binaryType = new FieldType();
    binaryType.setDocValuesType(DocValuesType.BINARY);
    binaryType.freeze();
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      ArrayDictionaryColumn col =
          new ArrayDictionaryColumn("f", binaryType, COLORS, new int[] {0}, new int[] {0});
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(1, col)));
      assertTrue(e.getMessage(), e.getMessage().contains("BinaryColumn"));
    }
    dir.close();
  }

  public void testRejectsPoints() throws IOException {
    FieldType pointType = new FieldType(SortedDocValuesField.TYPE);
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      ArrayDictionaryColumn col =
          new ArrayDictionaryColumn("f", pointType, COLORS, new int[] {0}, new int[] {0});
      expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(1, col)));
    }
    dir.close();
  }

  public void testRejectsNumericStoredType() throws IOException {
    // storedType() returning a numeric type should be rejected during validation
    FieldType storedSorted = new FieldType(SortedDocValuesField.TYPE);
    storedSorted.setStored(true);
    storedSorted.freeze();
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // Override storedType() to return INTEGER — rejected by validateDictionaryColumn
    ArrayDictionaryColumn col =
        new ArrayDictionaryColumn(
            "f", storedSorted, COLORS, new int[] {0}, new int[] {0}, StoredValue.Type.INTEGER);
    expectThrows(IllegalArgumentException.class, () -> w.addBatch(simpleBatch(1, col)));
    w.close();
    dir.close();
  }

  public void testInvertedDictionaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType stringType = new FieldType();
    stringType.setIndexOptions(IndexOptions.DOCS);
    stringType.setOmitNorms(true);
    stringType.setTokenized(false);
    stringType.freeze();

    // ords 0,2,0 → "blue","red","blue"
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "tag", stringType, COLORS, new int[] {0, 1, 2}, new int[] {0, 2, 0})));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(2, searcher.count(new TermQuery(new Term("tag", "blue"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "red"))));
    assertEquals(0, searcher.count(new TermQuery(new Term("tag", "green"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithSortedDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType invertedDvType = new FieldType();
    invertedDvType.setIndexOptions(IndexOptions.DOCS);
    invertedDvType.setOmitNorms(true);
    invertedDvType.setTokenized(false);
    invertedDvType.setDocValuesType(DocValuesType.SORTED);
    invertedDvType.freeze();

    // ords 0,2,0 → "blue","red","blue"
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "color", invertedDvType, COLORS, new int[] {0, 1, 2}, new int[] {0, 2, 0})));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(2, searcher.count(new TermQuery(new Term("color", "blue"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("color", "red"))));

    // Verify doc values
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues dv = leaf.getSortedDocValues("color");
    assertEquals(0, dv.nextDoc());
    assertEquals(new BytesRef("blue"), dv.lookupOrd(dv.ordValue()));
    assertEquals(1, dv.nextDoc());
    assertEquals(new BytesRef("red"), dv.lookupOrd(dv.ordValue()));
    assertEquals(2, dv.nextDoc());
    assertEquals(new BytesRef("blue"), dv.lookupOrd(dv.ordValue()));

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithStoredBinary() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType invertedStoredType = new FieldType();
    invertedStoredType.setIndexOptions(IndexOptions.DOCS);
    invertedStoredType.setOmitNorms(true);
    invertedStoredType.setTokenized(false);
    invertedStoredType.setStored(true);
    invertedStoredType.freeze();

    // ords 0,1,2 → "blue","green","red"
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "color", invertedStoredType, COLORS, new int[] {0, 1, 2}, new int[] {0, 1, 2})));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    assertEquals(1, searcher.count(new TermQuery(new Term("color", "blue"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("color", "green"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("color", "red"))));

    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    assertEquals(new BytesRef("blue"), storedFields.document(0).getField("color").binaryValue());
    assertEquals(new BytesRef("green"), storedFields.document(1).getField("color").binaryValue());
    assertEquals(new BytesRef("red"), storedFields.document(2).getField("color").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithStoredString() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType invertedStoredType = new FieldType();
    invertedStoredType.setIndexOptions(IndexOptions.DOCS);
    invertedStoredType.setOmitNorms(true);
    invertedStoredType.setTokenized(false);
    invertedStoredType.setStored(true);
    invertedStoredType.freeze();

    // ords 0,1,2 → "blue","green","red"; storedType overridden to STRING
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "color",
                invertedStoredType,
                COLORS,
                new int[] {0, 1, 2},
                new int[] {0, 1, 2},
                StoredValue.Type.STRING)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    assertEquals(1, searcher.count(new TermQuery(new Term("color", "blue"))));

    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    assertEquals("blue", storedFields.document(0).getField("color").stringValue());
    assertEquals("green", storedFields.document(1).getField("color").stringValue());
    assertEquals("red", storedFields.document(2).getField("color").stringValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testTokenizedDictionaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    List<BytesRef> phrases =
        List.of(
            new BytesRef("quick brown fox"),
            new BytesRef("lazy brown dog"),
            new BytesRef("quick fox jumps"));

    // ords 0,1,2 → each phrase goes to a separate doc
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDictionaryColumn(
                "text",
                TextField.TYPE_NOT_STORED,
                phrases,
                new int[] {0, 1, 2},
                new int[] {0, 1, 2})));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    assertEquals(2, searcher.count(new TermQuery(new Term("text", "quick"))));
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "brown"))));
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "fox"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "lazy"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "dog"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "jumps"))));
    assertEquals(0, searcher.count(new TermQuery(new Term("text", "missing"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testTokenizedWithStoredString() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    List<BytesRef> phrases = List.of(new BytesRef("hello world"), new BytesRef("goodbye world"));

    w.addBatch(
        simpleBatch(
            2,
            new ArrayDictionaryColumn(
                "text",
                TextField.TYPE_STORED,
                phrases,
                new int[] {0, 1},
                new int[] {0, 1},
                StoredValue.Type.STRING)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    assertEquals(2, searcher.count(new TermQuery(new Term("text", "world"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "hello"))));

    StoredFields storedFields = leaf.storedFields();
    assertEquals("hello world", storedFields.document(0).getField("text").stringValue());
    assertEquals("goodbye world", storedFields.document(1).getField("text").stringValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testRandomSortedCrossCheck() throws IOException {
    int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      int dictSize = TestUtil.nextInt(random(), 1, 32);
      List<BytesRef> dict = randomDictionary(dictSize);
      boolean dense = random().nextBoolean();
      int numBatches = TestUtil.nextInt(random(), 1, 4);

      try (Directory dictDir = newDirectory();
          Directory plainDir = newDirectory();
          IndexWriter dictW = new IndexWriter(dictDir, newIndexWriterConfig());
          IndexWriter plainW = new IndexWriter(plainDir, newIndexWriterConfig())) {

        // Per-insertion-id expected term, or null if the doc has no value.
        List<BytesRef> expected = new ArrayList<>();
        int nextId = 0;

        for (int b = 0; b < numBatches; b++) {
          int batchDocs = TestUtil.nextInt(random(), 1, 60);
          long[] ids = new long[batchDocs];

          if (dense) {
            int[] ords = new int[batchDocs];
            for (int i = 0; i < batchDocs; i++) {
              int ord = random().nextInt(dictSize);
              ords[i] = ord;
              ids[i] = nextId;
              BytesRef term = dict.get(ord);
              expected.add(term);
              Document plainDoc = new Document();
              plainDoc.add(new SortedDocValuesField("f", term));
              plainDoc.add(new NumericDocValuesField("id", nextId));
              plainW.addDocument(plainDoc);
              nextId++;
            }
            dictW.addBatch(
                simpleBatch(
                    batchDocs,
                    new ArrayDenseDictionaryColumn("f", SortedDocValuesField.TYPE, dict, ords),
                    new ArrayDenseLongColumn("id", NumericDocValuesField.TYPE, ids)));
          } else {
            List<Integer> ds = new ArrayList<>();
            List<Integer> os = new ArrayList<>();
            for (int i = 0; i < batchDocs; i++) {
              ids[i] = nextId;
              Document plainDoc = new Document();
              plainDoc.add(new NumericDocValuesField("id", nextId));
              if (random().nextInt(10) < 7) {
                int ord = random().nextInt(dictSize);
                ds.add(i);
                os.add(ord);
                plainDoc.add(new SortedDocValuesField("f", dict.get(ord)));
                expected.add(dict.get(ord));
              } else {
                expected.add(null);
              }
              plainW.addDocument(plainDoc);
              nextId++;
            }
            int[] docIds = ds.stream().mapToInt(Integer::intValue).toArray();
            int[] ords = os.stream().mapToInt(Integer::intValue).toArray();
            dictW.addBatch(
                simpleBatch(
                    batchDocs,
                    new ArrayDictionaryColumn("f", SortedDocValuesField.TYPE, dict, docIds, ords),
                    new ArrayDenseLongColumn("id", NumericDocValuesField.TYPE, ids)));
          }
        }

        if (random().nextBoolean()) {
          dictW.forceMerge(1);
          plainW.forceMerge(1);
        }

        try (DirectoryReader dictR = DirectoryReader.open(dictW);
            DirectoryReader plainR = DirectoryReader.open(plainW)) {
          assertSortedReadersEqual("f", "id", dictR, plainR, expected);
        }
      }
    }
  }

  public void testRandomSortedSetCrossCheck() throws IOException {
    int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      int dictSize = TestUtil.nextInt(random(), 1, 16);
      List<BytesRef> dict = randomDictionary(dictSize);
      int numBatches = TestUtil.nextInt(random(), 1, 4);

      // Per-insertion-id expected term SETS (deduped across the doc).
      List<Set<BytesRef>> expected = new ArrayList<>();
      int nextId = 0;

      try (Directory dictDir = newDirectory();
          Directory plainDir = newDirectory();
          IndexWriter dictW = new IndexWriter(dictDir, newIndexWriterConfig());
          IndexWriter plainW = new IndexWriter(plainDir, newIndexWriterConfig())) {

        for (int b = 0; b < numBatches; b++) {
          int batchDocs = TestUtil.nextInt(random(), 1, 40);
          List<Integer> ds = new ArrayList<>();
          List<Integer> os = new ArrayList<>();
          long[] ids = new long[batchDocs];
          for (int i = 0; i < batchDocs; i++) {
            int numValues = random().nextInt(5); // 0..4 values per doc
            Set<BytesRef> docTerms = new HashSet<>();
            Document plainDoc = new Document();
            plainDoc.add(new NumericDocValuesField("id", nextId));
            ids[i] = nextId;
            for (int v = 0; v < numValues; v++) {
              int ord = random().nextInt(dictSize);
              ds.add(i);
              os.add(ord);
              BytesRef term = dict.get(ord);
              if (docTerms.add(term)) {
                plainDoc.add(new SortedSetDocValuesField("f", BytesRef.deepCopyOf(term)));
              }
            }
            plainW.addDocument(plainDoc);
            expected.add(docTerms);
            nextId++;
          }

          int[] docIds = ds.stream().mapToInt(Integer::intValue).toArray();
          int[] ords = os.stream().mapToInt(Integer::intValue).toArray();
          dictW.addBatch(
              simpleBatch(
                  batchDocs,
                  new ArrayDictionaryColumn("f", SortedSetDocValuesField.TYPE, dict, docIds, ords),
                  new ArrayDenseLongColumn("id", NumericDocValuesField.TYPE, ids)));
        }

        if (random().nextBoolean()) {
          dictW.forceMerge(1);
          plainW.forceMerge(1);
        }

        try (DirectoryReader dictR = DirectoryReader.open(dictW);
            DirectoryReader plainR = DirectoryReader.open(plainW)) {
          assertSortedSetReadersEqual("f", "id", dictR, plainR, expected);
        }
      }
    }
  }

  private static List<BytesRef> randomDictionary(int size) {
    Set<String> uniq = new HashSet<>();
    while (uniq.size() < size) {
      uniq.add(TestUtil.randomSimpleString(random(), 1, 12));
    }
    List<BytesRef> dict = new ArrayList<>(size);
    for (String s : uniq) {
      dict.add(new BytesRef(s));
    }
    return dict;
  }

  private static void assertSortedReadersEqual(
      String field,
      String idField,
      DirectoryReader dictR,
      DirectoryReader plainR,
      List<BytesRef> expected)
      throws IOException {
    assertEquals(expected.size(), dictR.maxDoc());
    assertEquals(expected.size(), plainR.maxDoc());

    boolean anyValue = expected.stream().anyMatch(Objects::nonNull);
    if (!anyValue) {
      // No doc carries a value for "f"; nothing to compare.
      return;
    }
    checkSortedByInsertionId(field, idField, dictR, expected, "dict");
    checkSortedByInsertionId(field, idField, plainR, expected, "plain");
  }

  private static void checkSortedByInsertionId(
      String field, String idField, DirectoryReader r, List<BytesRef> expected, String label)
      throws IOException {
    int seen = 0;
    for (LeafReaderContext ctx : r.leaves()) {
      LeafReader leaf = ctx.reader();
      NumericDocValues ids = leaf.getNumericDocValues(idField);
      SortedDocValues dv = leaf.getSortedDocValues(field);
      assertNotNull(label + " reader missing id field", ids);
      int docID;
      while ((docID = ids.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        int insertionId = (int) ids.longValue();
        BytesRef want = expected.get(insertionId);
        BytesRef got = (dv != null && dv.advanceExact(docID)) ? dv.lookupOrd(dv.ordValue()) : null;
        assertEquals(label + " at insertionId " + insertionId, want, got);
        seen++;
      }
    }
    assertEquals(label + " reader saw wrong number of docs", expected.size(), seen);
  }

  private static void assertSortedSetReadersEqual(
      String field,
      String idField,
      DirectoryReader dictR,
      DirectoryReader plainR,
      List<Set<BytesRef>> expected)
      throws IOException {
    assertEquals(expected.size(), dictR.maxDoc());
    assertEquals(expected.size(), plainR.maxDoc());

    boolean anyValue = expected.stream().anyMatch(s -> !s.isEmpty());
    if (!anyValue) {
      return;
    }
    checkSortedSetByInsertionId(field, idField, dictR, expected, "dict");
    checkSortedSetByInsertionId(field, idField, plainR, expected, "plain");
  }

  private static void checkSortedSetByInsertionId(
      String field, String idField, DirectoryReader r, List<Set<BytesRef>> expected, String label)
      throws IOException {
    int seen = 0;
    for (LeafReaderContext ctx : r.leaves()) {
      LeafReader leaf = ctx.reader();
      NumericDocValues ids = leaf.getNumericDocValues(idField);
      SortedSetDocValues dv = leaf.getSortedSetDocValues(field);
      assertNotNull(label + " reader missing id field", ids);
      int docID;
      while ((docID = ids.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        int insertionId = (int) ids.longValue();
        Set<BytesRef> want = expected.get(insertionId);
        Set<BytesRef> got = (dv != null && dv.advanceExact(docID)) ? collect(dv) : Set.of();
        assertEquals(label + " at insertionId " + insertionId, want, got);
        seen++;
      }
    }
    assertEquals(label + " reader saw wrong number of docs", expected.size(), seen);
  }

  private static Set<BytesRef> collect(SortedSetDocValues dv) throws IOException {
    Set<BytesRef> out = new HashSet<>();
    for (int i = 0; i < dv.docValueCount(); i++) {
      out.add(BytesRef.deepCopyOf(dv.lookupOrd(dv.nextOrd())));
    }
    return out;
  }
}
