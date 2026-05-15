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

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayBinaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link BinaryColumn} batch indexing. */
public class TestColumnBatchBinaryColumn extends LuceneTestCase {

  public void testBinaryDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(3, new ArrayBinaryColumn("binary", BinaryDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    BinaryDocValues dv = leaf.getBinaryDocValues("binary");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.binaryValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("x")};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(3, new ArrayBinaryColumn("sorted", SortedDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues dv = leaf.getSortedDocValues("sorted");

    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef("x"), dv.lookupOrd(dv.ordValue()));
    assertEquals(1, dv.nextDoc());
    assertEquals(newBytesRef("y"), dv.lookupOrd(dv.ordValue()));
    assertEquals(2, dv.nextDoc());
    assertEquals(newBytesRef("x"), dv.lookupOrd(dv.ordValue()));

    // "x" and "y" should share ord space
    assertEquals(2, dv.getValueCount());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSetDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Doc 0 has two values, doc 1 has one value
    int[] docIds = {0, 0, 1};
    BytesRef[] values = {newBytesRef("a"), newBytesRef("b"), newBytesRef("a")};
    w.addBatch(
        simpleBatch(
            2, new ArrayBinaryColumn("sortedSet", SortedSetDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedSetDocValues dv = leaf.getSortedSetDocValues("sortedSet");

    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(newBytesRef("a"), dv.lookupOrd(dv.nextOrd()));
    assertEquals(newBytesRef("b"), dv.lookupOrd(dv.nextOrd()));

    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(newBytesRef("a"), dv.lookupOrd(dv.nextOrd()));

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + SORTED doc values
    FieldType storedSortedType = new FieldType();
    storedSortedType.setStored(true);
    storedSortedType.setDocValuesType(DocValuesType.SORTED);
    storedSortedType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("val", storedSortedType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("val").binaryValue());
    }

    // Verify doc values
    SortedDocValues dv = leaf.getSortedDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // StringField-like: DOCS, omitNorms, non-tokenized
    FieldType stringType = new FieldType();
    stringType.setIndexOptions(IndexOptions.DOCS);
    stringType.setOmitNorms(true);
    stringType.setTokenized(false);
    stringType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("alpha"), newBytesRef("beta"), newBytesRef("alpha")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("tag", stringType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(2, searcher.count(new TermQuery(new Term("tag", "alpha"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "beta"))));
    assertEquals(0, searcher.count(new TermQuery(new Term("tag", "gamma"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + SORTED doc values (like a StringField with doc values)
    FieldType invertedDvType = new FieldType();
    invertedDvType.setIndexOptions(IndexOptions.DOCS);
    invertedDvType.setOmitNorms(true);
    invertedDvType.setTokenized(false);
    invertedDvType.setDocValuesType(DocValuesType.SORTED);
    invertedDvType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("x")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", invertedDvType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(2, searcher.count(new TermQuery(new Term("field", "x"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "y"))));

    // Verify doc values
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithStored() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + stored (like StringField with Store.YES)
    FieldType invertedStoredType = new FieldType(StringField.TYPE_STORED);
    invertedStoredType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", invertedStoredType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "aaa"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "bbb"))));

    // Verify stored fields
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("field").binaryValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithStoredAndDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + stored + SORTED doc values
    FieldType allType = new FieldType();
    allType.setIndexOptions(IndexOptions.DOCS);
    allType.setOmitNorms(true);
    allType.setTokenized(false);
    allType.setStored(true);
    allType.setDocValuesType(DocValuesType.SORTED);
    allType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("z")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", allType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "x"))));

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      assertEquals(values[i], storedFields.document(i).getField("field").binaryValue());
    }

    // Verify doc values
    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedSparse() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType stringType = new FieldType();
    stringType.setIndexOptions(IndexOptions.DOCS);
    stringType.setOmitNorms(true);
    stringType.setTokenized(false);
    stringType.freeze();

    // Only doc 1 out of 3 has a term
    int[] docIds = {1};
    BytesRef[] values = {newBytesRef("found")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("tag", stringType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "found"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testTokenizedColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    // TextField-like: tokenized, DOCS_AND_FREQS_AND_POSITIONS
    int[] docIds = {0, 1, 2};
    BytesRef[] values = {
      newBytesRef("quick brown fox"), newBytesRef("lazy brown dog"), newBytesRef("quick fox jumps")
    };
    w.addBatch(
        simpleBatch(3, new ArrayBinaryColumn("text", TextField.TYPE_NOT_STORED, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Each word was tokenized — verify individual terms
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

  public void testTokenizedWithStored() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    int[] docIds = {0, 1};
    BytesRef[] values = {newBytesRef("hello world"), newBytesRef("goodbye world")};
    w.addBatch(
        simpleBatch(2, new ArrayBinaryColumn("text", TextField.TYPE_STORED, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify tokenized search
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "world"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "hello"))));

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    assertEquals(values[0], storedFields.document(0).getField("text").binaryValue());
    assertEquals(values[1], storedFields.document(1).getField("text").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredWithDocValuesAndPoints() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + SORTED_NUMERIC DV + 4-byte points
    FieldType allType = new FieldType();
    allType.setStored(true);
    allType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    allType.setDimensions(1, Integer.BYTES);
    allType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3, new ArrayLongColumn("field", allType, LongColumn.NumericKind.INT, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields — decoded as ints.
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(raw[i], storedFields.document(i).getField("field").numericValue().intValue());
    }

    // Verify doc values (raw int widened to long).
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("field");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], dv.nextValue());
    }

    // Verify points
    assertEquals(3, leaf.getPointValues("field").size());

    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedStoredWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + SORTED_NUMERIC doc values (multi-valued)
    FieldType storedSortedNumericType = new FieldType();
    storedSortedNumericType.setStored(true);
    storedSortedNumericType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    storedSortedNumericType.freeze();

    // Doc 0 has two values (10, 20), doc 1 has one value (30)
    int[] docIds = {0, 0, 1};
    long[] values = {10, 20, 30};
    w.addBatch(simpleBatch(2, new ArrayLongColumn("val", storedSortedNumericType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields — each value occurrence is stored separately
    StoredFields storedFields = leaf.storedFields();
    Document doc0 = storedFields.document(0);
    assertEquals(2, doc0.getFields("val").length);
    assertEquals(10L, doc0.getFields("val")[0].numericValue().longValue());
    assertEquals(20L, doc0.getFields("val")[1].numericValue().longValue());
    Document doc1 = storedFields.document(1);
    assertEquals(1, doc1.getFields("val").length);
    assertEquals(30L, doc1.getFields("val")[0].numericValue().longValue());

    // Verify doc values
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(10, dv.nextValue());
    assertEquals(20, dv.nextValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(30, dv.nextValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumnNumericDVBadFixedSizeThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Variable-size binary into NUMERIC DV should fail validation (fixedSize=-1).
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayBinaryColumn(
                        "val",
                        NumericDocValuesField.TYPE,
                        new int[] {0},
                        new BytesRef[] {newBytesRef("x")}))));

    w.close();
    dir.close();
  }

  public void testStoredTypeStringFromBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    String[] raw = {"hello", "wörld", "🦜"};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = newBytesRef(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayBinaryColumn(
                "val", type, new int[] {0, 1, 2}, values, StoredValue.Type.STRING)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(raw[i], storedFields.document(i).getField("val").stringValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeDataInputRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayBinaryColumn(
                        "val",
                        type,
                        new int[] {0},
                        new BytesRef[] {newBytesRef("x")},
                        StoredValue.Type.DATA_INPUT))));

    w.close();
    dir.close();
  }

  public void testBinaryColumnMultiDimPointsOnly() throws IOException {
    // Plain BinaryColumn with 2-D int points (fixedSize = 2 * 4 = 8). Caller pre-packs bytes via
    // IntPoint.pack; the chain writes them to points unchanged.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(2, Integer.BYTES);
    pointType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {IntPoint.pack(1, 10), IntPoint.pack(2, 20), IntPoint.pack(3, 30)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("pt", pointType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        1, searcher.count(IntPoint.newRangeQuery("pt", new int[] {1, 10}, new int[] {1, 10})));
    assertEquals(
        3, searcher.count(IntPoint.newRangeQuery("pt", new int[] {0, 0}, new int[] {10, 100})));

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumnPointsOnlyArbitraryWidth() throws IOException {
    // 3-D int points (12 bytes) via plain BinaryColumn — arbitrary widths are fine for the
    // opaque-bytes path since no numeric transform is applied.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(3, Integer.BYTES);
    type.freeze();

    int[][] raw = {{1, 2, 3}, {4, 5, 6}, {10, 20, 30}};
    BytesRef[] values = new BytesRef[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = IntPoint.pack(raw[i]);
    }
    w.addBatch(simpleBatch(raw.length, new ArrayBinaryColumn("pt", type, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        1, searcher.count(IntPoint.newRangeQuery("pt", new int[] {1, 2, 3}, new int[] {1, 2, 3})));
    assertEquals(
        3,
        searcher.count(
            IntPoint.newRangeQuery("pt", new int[] {0, 0, 0}, new int[] {100, 100, 100})));

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumnSortedDVAndPoints() throws IOException {
    // Plain BinaryColumn with SORTED DV + 1-D int point. Same BytesRef goes to both writers.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Integer.BYTES);
    type.setDocValuesType(DocValuesType.SORTED);
    type.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {IntPoint.pack(10), IntPoint.pack(20), IntPoint.pack(30)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", type, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("field", 10)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("field", 10, 30)));

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Single SORTED_SET column whose tuple cursor emits multiple values per doc. Doc-ids repeat in
   * non-decreasing order; values within a doc are deduplicated and ord-sorted by the writer.
   */
  public void testMultiValuedBinaryAcrossDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // doc 0: {a, b}; doc 1: {c}; doc 2: {b, d, e}; doc 3: {a, e}
    int[] docIds = {0, 0, 1, 2, 2, 2, 3, 3};
    BytesRef[] values = {
      newBytesRef("a"),
      newBytesRef("b"),
      newBytesRef("c"),
      newBytesRef("b"),
      newBytesRef("d"),
      newBytesRef("e"),
      newBytesRef("a"),
      newBytesRef("e"),
    };
    w.addBatch(
        simpleBatch(4, new ArrayBinaryColumn("set", SortedSetDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedSetDocValues dv = leaf.getSortedSetDocValues("set");

    String[][] expectedPerDoc = {
      {"a", "b"},
      {"c"},
      {"b", "d", "e"},
      {"a", "e"},
    };
    for (int d = 0; d < expectedPerDoc.length; d++) {
      assertEquals(d, dv.nextDoc());
      assertEquals(expectedPerDoc[d].length, dv.docValueCount());
      for (int i = 0; i < expectedPerDoc[d].length; i++) {
        long ord = dv.nextOrd();
        assertEquals(newBytesRef(expectedPerDoc[d][i]), dv.lookupOrd(ord));
      }
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }
}
