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
import java.nio.ByteOrder;
import java.util.List;
import org.apache.lucene.document.BinaryColumn;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryTupleCursor;
import org.apache.lucene.document.BinaryValuesCursor;
import org.apache.lucene.document.Column;
import org.apache.lucene.document.ColumnBatch;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongColumn;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongTupleCursor;
import org.apache.lucene.document.LongValuesCursor;
import org.apache.lucene.document.NumericBinaryColumn;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;

/** Tests for column-oriented batch indexing via {@link IndexWriter#addBatch}. */
public class TestColumnBatchIndexing extends LuceneTestCase {

  public void testNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("numeric");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Doc 0 has two values, doc 1 has one value
    int[] docIds = {0, 0, 1};
    long[] values = {5, 15, 25};
    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "sortedNumeric", SortedNumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("sortedNumeric");

    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(5, dv.nextValue());
    assertEquals(15, dv.nextValue());

    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(25, dv.nextValue());

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

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

  public void testMultipleColumns() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    int[] allDocs = {0, 1, 2};
    long[] numericValues = {100, 200, 300};
    BytesRef[] sortedValues = {newBytesRef("a"), newBytesRef("b"), newBytesRef("c")};

    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, allDocs, numericValues),
            new ArrayBinaryColumn("sorted", SortedDocValuesField.TYPE, allDocs, sortedValues)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues ndv = leaf.getNumericDocValues("numeric");
    SortedDocValues sdv = leaf.getSortedDocValues("sorted");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(numericValues[i], ndv.longValue());
      assertEquals(i, sdv.nextDoc());
      assertEquals(sortedValues[i], sdv.lookupOrd(sdv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSparseDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Only doc 1 has a value (docs 0 and 2 are missing)
    int[] docIds = {1};
    long[] values = {42};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("sparse", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("sparse");
    assertEquals(1, dv.nextDoc());
    assertEquals(42, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testParentFieldIndexed() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setParentField("_parent");
    IndexWriter w = new IndexWriter(dir, config);

    int[] docIds = {0, 1, 2};
    long[] values = {1, 2, 3};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Every batch doc should have the parent field
    NumericDocValues parentDv = leaf.getNumericDocValues("_parent");
    assertNotNull(parentDv);
    for (int i = 0; i < 3; i++) {
      assertEquals(i, parentDv.nextDoc());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testPointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Create a points-only FieldType (1 dimension, Integer.BYTES)
    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = new BytesRef(intsToBytes(new int[] {raw[i]}, ByteOrder.LITTLE_ENDIAN));
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "point",
                pointType,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 10)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 20)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 30)));
    assertEquals(0, searcher.count(IntPoint.newExactQuery("point", 99)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("point", 10, 30)));

    r.close();
    w.close();
    dir.close();
  }

  public void testPointsWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 1D int points + SORTED_NUMERIC DV via the compat layer.
    FieldType pointAndDvType = new FieldType();
    pointAndDvType.setDimensions(1, Integer.BYTES);
    pointAndDvType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    pointAndDvType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = new BytesRef(intsToBytes(new int[] {raw[i]}, ByteOrder.LITTLE_ENDIAN));
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "field",
                pointAndDvType,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    assertEquals(1, searcher.count(IntPoint.newExactQuery("field", 10)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("field", 10, 30)));

    LeafReader leaf = getOnlyLeafReader(r);
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], dv.nextValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSparsePointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    // Only doc 1 out of 3 has a point value
    int[] docIds = {1};
    BytesRef[] values = {new BytesRef(intsToBytes(new int[] {42}, ByteOrder.LITTLE_ENDIAN))};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "point",
                pointType,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 42)));
    assertEquals(0, searcher.count(IntPoint.newExactQuery("point", 0)));

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + NUMERIC doc values
    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();

    int[] docIds = {0, 1, 2};
    long[] values = {100, 200, 300};
    w.addBatch(simpleBatch(3, new ArrayLongColumn("val", storedNumericType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("val").numericValue().longValue());
    }

    // Verify doc values
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

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

  public void testStoredOnlyColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored only — no doc values, no points
    FieldType storedOnlyType = new FieldType();
    storedOnlyType.setStored(true);
    storedOnlyType.freeze();

    int[] docIds = {0, 1, 2};
    long[] values = {10, 20, 30};
    w.addBatch(simpleBatch(3, new ArrayLongColumn("stored", storedOnlyType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("stored").numericValue().longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testMixedStoredAndNonStoredColumns() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();

    int[] allDocs = {0, 1, 2};
    long[] storedValues = {100, 200, 300};
    long[] dvOnlyValues = {1, 2, 3};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("stored_field", storedNumericType, allDocs, storedValues),
            new ArrayLongColumn("dv_only", NumericDocValuesField.TYPE, allDocs, dvOnlyValues)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored field
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(storedValues[i], doc.getField("stored_field").numericValue().longValue());
      assertNull(doc.getField("dv_only")); // non-stored column should not appear
    }

    // Verify both doc values columns
    NumericDocValues storedDv = leaf.getNumericDocValues("stored_field");
    NumericDocValues dvOnly = leaf.getNumericDocValues("dv_only");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, storedDv.nextDoc());
      assertEquals(storedValues[i], storedDv.longValue());
      assertEquals(i, dvOnly.nextDoc());
      assertEquals(dvOnlyValues[i], dvOnly.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredPointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + points
    FieldType storedPointType = new FieldType();
    storedPointType.setStored(true);
    storedPointType.setDimensions(1, Integer.BYTES);
    storedPointType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = new BytesRef(intsToBytes(new int[] {raw[i]}, ByteOrder.LITTLE_ENDIAN));
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "pt",
                storedPointType,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                StoredValue.Type.INTEGER,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields — decoded as ints.
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      Document doc = storedFields.document(i);
      assertEquals(raw[i], doc.getField("pt").numericValue().intValue());
    }

    // Verify points
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("pt", 10)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("pt", 10, 30)));

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

  public void testColumnWithNoneDocValuesTypeAndNoPointsThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FieldType with NONE doc values type and no points
    FieldType badType = new FieldType();
    badType.freeze();

    int[] docIds = {0};
    long[] values = {1};
    expectThrows(
        IllegalArgumentException.class,
        () -> w.addBatch(simpleBatch(1, new ArrayLongColumn("bad", badType, docIds, values))));

    // Writer should still be usable after the failure
    w.addBatch(
        simpleBatch(
            1,
            new ArrayLongColumn(
                "numeric", NumericDocValuesField.TYPE, new int[] {0}, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("numeric");
    assertNotNull(dv);
    // The failed batch's doc was marked deleted; the successful batch's doc is still live
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredIndexedWithDocValuesAndPoints() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + indexed(DOCS, omitNorms) + SORTED_NUMERIC DV + 4-byte points
    FieldType allType = new FieldType();
    allType.setStored(true);
    allType.setIndexOptions(IndexOptions.DOCS);
    allType.setOmitNorms(true);
    allType.setTokenized(false);
    allType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    allType.setDimensions(1, Integer.BYTES);
    allType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = new BytesRef(intsToBytes(new int[] {raw[i]}, ByteOrder.LITTLE_ENDIAN));
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "field",
                allType,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                StoredValue.Type.INTEGER,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index (terms over raw LE bytes).
    for (int i = 0; i < raw.length; i++) {
      assertEquals(1, searcher.count(new TermQuery(new Term("field", values[i]))));
    }

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

  public void testDenseNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {100, 200, 300};
    w.addBatch(simpleBatch(3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseSortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30, 40, 50};
    w.addBatch(
        simpleBatch(5, new ArrayDenseLongColumn("val", SortedNumericDocValuesField.TYPE, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < 5; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(1, dv.docValueCount());
      assertEquals(values[i], dv.nextValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseColumnCountMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 2 values but batch expects 3 documents
    long[] values = {10, 20};
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values))));

    // Writer should still be usable after the failure — use a different field to avoid
    // the partially-written DV entries from the failed batch
    w.addBatch(
        simpleBatch(
            1, new ArrayDenseLongColumn("val2", NumericDocValuesField.TYPE, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseColumnTooManyValuesThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 5 values but batch only has 3 documents
    long[] values = {10, 20, 30, 40, 50};
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values))));

    // Writer should still be usable — no values were written past numDocs
    w.addBatch(
        simpleBatch(
            1, new ArrayDenseLongColumn("val2", NumericDocValuesField.TYPE, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {100, 200, 300};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDenseBinaryColumn(
                "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinarySortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30, 40, 50};
    byte[] bytes = longsToBytes(values, ByteOrder.BIG_ENDIAN);
    w.addBatch(
        simpleBatch(
            5,
            new ArrayDenseBinaryColumn(
                "val", SortedNumericDocValuesField.TYPE, ByteOrder.BIG_ENDIAN, bytes)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < 5; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(1, dv.docValueCount());
      assertEquals(values[i], dv.nextValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryColumnTooManyValuesThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 5 values but batch only has 3 documents
    long[] values = {10, 20, 30, 40, 50};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3,
                    new ArrayDenseBinaryColumn(
                        "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes))));

    // Writer should still be usable
    w.addBatch(
        simpleBatch(
            1,
            new ArrayDenseBinaryColumn(
                "val2",
                NumericDocValuesField.TYPE,
                ByteOrder.LITTLE_ENDIAN,
                longsToBytes(new long[] {42}, ByteOrder.LITTLE_ENDIAN))));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryColumnCountMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 2 values but batch expects 3 documents
    long[] values = {10, 20};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3,
                    new ArrayDenseBinaryColumn(
                        "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes))));

    // Writer should still be usable
    w.addBatch(
        simpleBatch(
            1,
            new ArrayDenseBinaryColumn(
                "val2",
                NumericDocValuesField.TYPE,
                ByteOrder.LITTLE_ENDIAN,
                longsToBytes(new long[] {42}, ByteOrder.LITTLE_ENDIAN))));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumn4ByteDenseNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    int[] values = {1, -2, 3, Integer.MIN_VALUE, Integer.MAX_VALUE};
    byte[] bytes = intsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    w.addBatch(
        simpleBatch(
            5,
            new ArrayDenseBinaryColumn(
                "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, Integer.BYTES, bytes)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue()); // sign-extended
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumn4ByteSparseNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Sparse: only docs 0 and 2 have values.
    int[] docIds = {0, 2};
    int[] raw = {-7, 9};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      byte[] b = intsToBytes(new int[] {raw[i]}, ByteOrder.BIG_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                NumericDocValuesField.TYPE,
                Integer.BYTES,
                ByteOrder.BIG_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val");
    assertEquals(0, dv.nextDoc());
    assertEquals(-7, dv.longValue());
    assertEquals(2, dv.nextDoc());
    assertEquals(9, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryColumnPointFixedSizeMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES); // expects fixedSize=4
    pointType.freeze();

    // BinaryColumn with fixedSize=8 should fail point validation.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayNumericBinaryColumn(
                        "pt",
                        pointType,
                        Long.BYTES,
                        ByteOrder.LITTLE_ENDIAN,
                        NumericBinaryColumn.NumericKind.LONG,
                        new int[] {0},
                        new BytesRef[] {
                          new BytesRef(longsToBytes(new long[] {1}, ByteOrder.LITTLE_ENDIAN))
                        }))));

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

  public void testLongColumnPointsThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Long.BYTES);
    pointType.freeze();

    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1, new ArrayLongColumn("pt", pointType, new int[] {0}, new long[] {1}))));

    w.close();
    dir.close();
  }

  public void testDenseLongColumnWithStoredFields() throws IOException {
    // Covers the "single column consumed by both passes via fresh cursors" case: a dense
    // LongColumn with stored+numeric DV. Row pass uses tuples(), column pass uses values().
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();

    long[] values = {100, 200, 300, 400};
    w.addBatch(simpleBatch(4, new ArrayDenseLongColumn("val", storedNumericType, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], storedFields.document(i).getField("val").numericValue().longValue());
    }

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeIntegerFromBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    int[] raw = {1, -2, 3};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      byte[] b = intsToBytes(new int[] {raw[i]}, ByteOrder.LITTLE_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.INT,
                StoredValue.Type.INTEGER,
                new int[] {0, 1, 2},
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(raw[i], storedFields.document(i).getField("val").numericValue().intValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeLongFromBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    long[] raw = {Long.MIN_VALUE, 0L, Long.MAX_VALUE};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      byte[] b = longsToBytes(new long[] {raw[i]}, ByteOrder.BIG_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Long.BYTES,
                ByteOrder.BIG_ENDIAN,
                NumericBinaryColumn.NumericKind.LONG,
                StoredValue.Type.LONG,
                new int[] {0, 1, 2},
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(raw[i], storedFields.document(i).getField("val").numericValue().longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeFloatFromBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    float[] raw = {1.5f, -2.25f, Float.MAX_VALUE};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      byte[] b = intsToBytes(new int[] {Float.floatToRawIntBits(raw[i])}, ByteOrder.LITTLE_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.FLOAT,
                StoredValue.Type.FLOAT,
                new int[] {0, 1, 2},
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(
          raw[i], storedFields.document(i).getField("val").numericValue().floatValue(), 0f);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeDoubleFromBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    double[] raw = {1.5d, -2.25d, Double.MAX_VALUE};
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      byte[] b =
          longsToBytes(new long[] {Double.doubleToRawLongBits(raw[i])}, ByteOrder.BIG_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Long.BYTES,
                ByteOrder.BIG_ENDIAN,
                NumericBinaryColumn.NumericKind.DOUBLE,
                StoredValue.Type.DOUBLE,
                new int[] {0, 1, 2},
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(
          raw[i], storedFields.document(i).getField("val").numericValue().doubleValue(), 0d);
    }

    r.close();
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

  public void testStoredTypeFloatWithNumericDVAndIndexed() throws IOException {
    // storedType=FLOAT on a BinaryColumn that also feeds NumericDV and inverted index.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.setIndexOptions(IndexOptions.DOCS);
    type.setOmitNorms(true);
    type.setTokenized(false);
    type.freeze();

    float[] raw = {1.5f, -2.25f, 42.0f};
    int[] rawBits = new int[raw.length];
    BytesRef[] values = new BytesRef[raw.length];
    for (int i = 0; i < raw.length; i++) {
      rawBits[i] = Float.floatToRawIntBits(raw[i]);
      byte[] b = intsToBytes(new int[] {rawBits[i]}, ByteOrder.LITTLE_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.FLOAT,
                StoredValue.Type.FLOAT,
                new int[] {0, 1, 2},
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Stored values decoded as floats.
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(
          raw[i], storedFields.document(i).getField("val").numericValue().floatValue(), 0f);
    }

    // NumericDV stores the raw int bits (sign-extended to long).
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(rawBits[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeIntegerBadFixedSizeThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    // INTEGER stored type requires fixedSize=4, but column has fixedSize=8.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayNumericBinaryColumn(
                        "val",
                        type,
                        Long.BYTES,
                        ByteOrder.LITTLE_ENDIAN,
                        NumericBinaryColumn.NumericKind.LONG,
                        StoredValue.Type.INTEGER,
                        new int[] {0},
                        new BytesRef[] {
                          new BytesRef(longsToBytes(new long[] {1}, ByteOrder.LITTLE_ENDIAN))
                        }))));

    w.close();
    dir.close();
  }

  public void testStoredTypeDoubleBadFixedSizeThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    // DOUBLE stored type requires fixedSize=8, but column has fixedSize=4.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayNumericBinaryColumn(
                        "val",
                        type,
                        Integer.BYTES,
                        ByteOrder.LITTLE_ENDIAN,
                        NumericBinaryColumn.NumericKind.INT,
                        StoredValue.Type.DOUBLE,
                        new int[] {0},
                        new BytesRef[] {
                          new BytesRef(intsToBytes(new int[] {1}, ByteOrder.LITTLE_ENDIAN))
                        }))));

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

  public void testNumericKindIntPointsAndDV() throws IOException {
    for (ByteOrder order : new ByteOrder[] {ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN}) {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

      FieldType type = new FieldType();
      type.setDimensions(1, Integer.BYTES);
      type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
      type.freeze();

      int[] raw = {-5, -1, 0, 7, Integer.MAX_VALUE};
      BytesRef[] values = new BytesRef[raw.length];
      int[] docIds = new int[raw.length];
      for (int i = 0; i < raw.length; i++) {
        docIds[i] = i;
        byte[] b = intsToBytes(new int[] {raw[i]}, order);
        values[i] = new BytesRef(b, 0, b.length);
      }
      w.addBatch(
          simpleBatch(
              raw.length,
              new ArrayNumericBinaryColumn(
                  "val",
                  type,
                  Integer.BYTES,
                  order,
                  NumericBinaryColumn.NumericKind.INT,
                  docIds,
                  values)));

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);

      SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
      for (int i = 0; i < raw.length; i++) {
        assertEquals(i, dv.nextDoc());
        assertEquals(1, dv.docValueCount());
        assertEquals(raw[i], dv.nextValue());
      }

      IndexSearcher searcher = new IndexSearcher(r);
      assertEquals(
          raw.length,
          searcher.count(IntPoint.newRangeQuery("val", Integer.MIN_VALUE, Integer.MAX_VALUE)));
      assertEquals(1, searcher.count(IntPoint.newExactQuery("val", -5)));
      assertEquals(3, searcher.count(IntPoint.newRangeQuery("val", -1, 7)));

      r.close();
      w.close();
      dir.close();
    }
  }

  public void testNumericKindLongPointsAndDV() throws IOException {
    for (ByteOrder order : new ByteOrder[] {ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN}) {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

      FieldType type = new FieldType();
      type.setDimensions(1, Long.BYTES);
      type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
      type.freeze();

      long[] raw = {Long.MIN_VALUE, -100L, 0L, 42L, Long.MAX_VALUE};
      BytesRef[] values = new BytesRef[raw.length];
      int[] docIds = new int[raw.length];
      for (int i = 0; i < raw.length; i++) {
        docIds[i] = i;
        byte[] b = longsToBytes(new long[] {raw[i]}, order);
        values[i] = new BytesRef(b, 0, b.length);
      }
      w.addBatch(
          simpleBatch(
              raw.length,
              new ArrayNumericBinaryColumn(
                  "val",
                  type,
                  Long.BYTES,
                  order,
                  NumericBinaryColumn.NumericKind.LONG,
                  docIds,
                  values)));

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);

      SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
      for (int i = 0; i < raw.length; i++) {
        assertEquals(i, dv.nextDoc());
        assertEquals(raw[i], dv.nextValue());
      }

      IndexSearcher searcher = new IndexSearcher(r);
      assertEquals(
          raw.length,
          searcher.count(LongPoint.newRangeQuery("val", Long.MIN_VALUE, Long.MAX_VALUE)));
      assertEquals(1, searcher.count(LongPoint.newExactQuery("val", Long.MIN_VALUE)));
      assertEquals(3, searcher.count(LongPoint.newRangeQuery("val", -100L, 42L)));

      r.close();
      w.close();
      dir.close();
    }
  }

  public void testNumericKindFloatPointsAndDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Float.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    float[] raw = {Float.NEGATIVE_INFINITY, -1.5f, 0.0f, 2.25f, Float.POSITIVE_INFINITY};
    int[] rawBits = new int[raw.length];
    BytesRef[] values = new BytesRef[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      rawBits[i] = Float.floatToRawIntBits(raw[i]);
      byte[] b = intsToBytes(new int[] {rawBits[i]}, ByteOrder.LITTLE_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Float.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.FLOAT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // DV stores raw IEEE bits; decode via intBitsToFloat.
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(Float.floatToRawIntBits(raw[i]), dv.nextValue());
    }

    // Points sort numerically.
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(
            FloatPoint.newRangeQuery("val", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));
    assertEquals(1, searcher.count(FloatPoint.newExactQuery("val", -1.5f)));
    assertEquals(3, searcher.count(FloatPoint.newRangeQuery("val", -1.5f, 2.25f)));

    r.close();
    w.close();
    dir.close();
  }

  public void testNumericKindDoublePointsAndDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Double.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    double[] raw = {Double.NEGATIVE_INFINITY, -1.5d, 0.0d, 2.25d, Double.POSITIVE_INFINITY};
    BytesRef[] values = new BytesRef[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      long bits = Double.doubleToRawLongBits(raw[i]);
      byte[] b = longsToBytes(new long[] {bits}, ByteOrder.BIG_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Double.BYTES,
                ByteOrder.BIG_ENDIAN,
                NumericBinaryColumn.NumericKind.DOUBLE,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(Double.doubleToRawLongBits(raw[i]), dv.nextValue());
    }

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(
            DoublePoint.newRangeQuery("val", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
    assertEquals(1, searcher.count(DoublePoint.newExactQuery("val", -1.5d)));
    assertEquals(3, searcher.count(DoublePoint.newRangeQuery("val", -1.5d, 2.25d)));

    r.close();
    w.close();
    dir.close();
  }

  public void testNumericKindPointsAndDVMultiDimRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 2D int: scenario 3 requires 1D.
    FieldType type = new FieldType();
    type.setDimensions(2, Integer.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayNumericBinaryColumn(
                        "val",
                        type,
                        8,
                        ByteOrder.LITTLE_ENDIAN,
                        NumericBinaryColumn.NumericKind.LONG,
                        new int[] {0},
                        new BytesRef[] {
                          new BytesRef(longsToBytes(new long[] {1L}, ByteOrder.LITTLE_ENDIAN))
                        }))));

    w.close();
    dir.close();
  }

  public void testNumericKindPointsAndDVWidthMismatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FLOAT kind with fixedSize=8 should throw.
    FieldType type = new FieldType();
    type.setDimensions(1, Long.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayNumericBinaryColumn(
                        "val",
                        type,
                        Long.BYTES,
                        ByteOrder.LITTLE_ENDIAN,
                        NumericBinaryColumn.NumericKind.FLOAT,
                        new int[] {0},
                        new BytesRef[] {
                          new BytesRef(longsToBytes(new long[] {1L}, ByteOrder.LITTLE_ENDIAN))
                        }))));

    w.close();
    dir.close();
  }

  public void testNumericKindDVOnlyIgnoresKind() throws IOException {
    // Scenario 2: DV only. numericKind is ignored; bytes round-trip as raw IEEE bits via byteOrder.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    float[] raw = {1.5f, -2.25f, Float.MAX_VALUE};
    BytesRef[] values = new BytesRef[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      byte[] b = intsToBytes(new int[] {Float.floatToRawIntBits(raw[i])}, ByteOrder.LITTLE_ENDIAN);
      values[i] = new BytesRef(b, 0, b.length);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            // Declared FLOAT kind but with only DV (no points) — kind should be ignored.
            new ArrayNumericBinaryColumn(
                "val",
                type,
                Float.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                NumericBinaryColumn.NumericKind.FLOAT,
                docIds,
                values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      // DV stores raw IEEE bits sign-extended from the 4-byte decode.
      assertEquals(Float.floatToRawIntBits(raw[i]), dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  /**
   * With a sparse row column, the batch must still produce {@code numDocs} documents in the
   * segment, and stored-fields for un-populated docs must be empty (not shifted, not missing). This
   * guards the row-dense framing contract: every doc-id in {@code [0, numDocs)} is framed
   * regardless of whether any row column has a value at that doc.
   */
  public void testSparseStoredFramingPreservesNumDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedOnly = new FieldType();
    storedOnly.setStored(true);
    storedOnly.freeze();

    // 5 batch docs, but only docs 1 and 3 have a stored value.
    int[] docIds = {1, 3};
    BytesRef[] values = {newBytesRef("one"), newBytesRef("three")};
    w.addBatch(simpleBatch(5, new ArrayBinaryColumn("field", storedOnly, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(5, leaf.maxDoc());

    StoredFields storedFields = leaf.storedFields();
    assertNull(storedFields.document(0).getField("field"));
    assertEquals(newBytesRef("one"), storedFields.document(1).getField("field").binaryValue());
    assertNull(storedFields.document(2).getField("field"));
    assertEquals(newBytesRef("three"), storedFields.document(3).getField("field").binaryValue());
    assertNull(storedFields.document(4).getField("field"));

    r.close();
    w.close();
    dir.close();
  }

  /**
   * With a sparse indexed row column, the segment must still have {@code numDocs} documents, and
   * the inverted index must reflect only the populated docs. Guards termsHash framing alignment.
   */
  public void testSparseIndexedFramingPreservesNumDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType indexedType = new FieldType();
    indexedType.setIndexOptions(IndexOptions.DOCS);
    indexedType.setOmitNorms(true);
    indexedType.setTokenized(false);
    indexedType.freeze();

    // 6 batch docs, only docs 2 and 5 have a term.
    int[] docIds = {2, 5};
    BytesRef[] values = {newBytesRef("a"), newBytesRef("b")};
    w.addBatch(simpleBatch(6, new ArrayBinaryColumn("tag", indexedType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(6, leaf.maxDoc());

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "a"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "b"))));

    r.close();
    w.close();
    dir.close();
  }

  /**
   * When some docs in the batch have only a DV column (no row column value), framing still happens
   * for every doc: stored fields must be empty for those docs, inverted index untouched, and DV
   * values align with their batch doc-ids.
   */
  public void testSparseRowMixedWithDenseDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedOnly = new FieldType();
    storedOnly.setStored(true);
    storedOnly.freeze();

    // Row-sparse stored column: only docs 0 and 3 have a stored value.
    int[] storedDocIds = {0, 3};
    BytesRef[] storedValues = {newBytesRef("first"), newBytesRef("fourth")};
    // Dense DV column covering every doc.
    long[] dvValues = {100, 200, 300, 400};

    w.addBatch(
        simpleBatch(
            4,
            new ArrayBinaryColumn("stored", storedOnly, storedDocIds, storedValues),
            new ArrayDenseLongColumn("dv", NumericDocValuesField.TYPE, dvValues)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(4, leaf.maxDoc());

    StoredFields storedFields = leaf.storedFields();
    assertEquals(newBytesRef("first"), storedFields.document(0).getField("stored").binaryValue());
    assertNull(storedFields.document(1).getField("stored"));
    assertNull(storedFields.document(2).getField("stored"));
    assertEquals(newBytesRef("fourth"), storedFields.document(3).getField("stored").binaryValue());

    NumericDocValues dv = leaf.getNumericDocValues("dv");
    for (int i = 0; i < dvValues.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(dvValues[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Indexing the same logical docs via {@code addBatch} with a sparse row column vs. via {@code
   * addDocument} one doc at a time must produce segments with the same {@code maxDoc} and the same
   * stored-field / inverted-index visibility. This is the golden equivalence check.
   */
  public void testSparseBatchMatchesDocByDoc() throws IOException {
    FieldType storedIndexed = new FieldType(StringField.TYPE_STORED);
    storedIndexed.freeze();

    // 7 docs; only docs 1, 2, and 5 have values for the row column.
    int[] docIds = {1, 2, 5};
    String[] values = {"alpha", "beta", "gamma"};
    int totalDocs = 7;

    // --- Batch path ---
    Directory batchDir = newDirectory();
    try (IndexWriter batchW = new IndexWriter(batchDir, newIndexWriterConfig())) {
      BytesRef[] refs = new BytesRef[values.length];
      for (int i = 0; i < values.length; i++) {
        refs[i] = newBytesRef(values[i]);
      }
      // StringField stores as STRING — use the matching storedType so stored-value round-trip is
      // comparable between the two paths.
      batchW.addBatch(
          simpleBatch(
              totalDocs,
              new ArrayBinaryColumn(
                  "field", storedIndexed, docIds, refs, StoredValue.Type.STRING)));
    }

    // --- Doc-by-doc path ---
    Directory singleDir = newDirectory();
    try (IndexWriter singleW = new IndexWriter(singleDir, newIndexWriterConfig())) {
      int next = 0;
      for (int d = 0; d < totalDocs; d++) {
        Document doc = new Document();
        if (next < docIds.length && docIds[next] == d) {
          doc.add(
              new StringField("field", values[next], org.apache.lucene.document.Field.Store.YES));
          next++;
        }
        singleW.addDocument(doc);
      }
    }

    try (DirectoryReader batchR = DirectoryReader.open(batchDir);
        DirectoryReader singleR = DirectoryReader.open(singleDir)) {
      LeafReader batchLeaf = getOnlyLeafReader(batchR);
      LeafReader singleLeaf = getOnlyLeafReader(singleR);

      assertEquals(singleLeaf.maxDoc(), batchLeaf.maxDoc());
      assertEquals(totalDocs, batchLeaf.maxDoc());

      StoredFields batchStored = batchLeaf.storedFields();
      StoredFields singleStored = singleLeaf.storedFields();
      for (int d = 0; d < totalDocs; d++) {
        IndexableField bf = batchStored.document(d).getField("field");
        IndexableField sf = singleStored.document(d).getField("field");
        if (sf == null) {
          assertNull("doc " + d + " should have no stored field", bf);
        } else {
          assertNotNull("doc " + d + " should have a stored field", bf);
          assertEquals(sf.stringValue(), bf.stringValue());
        }
      }

      IndexSearcher batchSearcher = new IndexSearcher(batchR);
      IndexSearcher singleSearcher = new IndexSearcher(singleR);
      for (String v : values) {
        Term t = new Term("field", v);
        assertEquals(singleSearcher.count(new TermQuery(t)), batchSearcher.count(new TermQuery(t)));
      }
    }

    batchDir.close();
    singleDir.close();
  }

  /** A row column that returns an out-of-order batch doc-id must be rejected. */
  public void testRowColumnOutOfOrderDocIdThrows() throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      FieldType storedOnly = new FieldType();
      storedOnly.setStored(true);
      storedOnly.freeze();

      // docIds intentionally not non-decreasing.
      int[] docIds = {2, 1};
      BytesRef[] values = {newBytesRef("a"), newBytesRef("b")};
      expectThrows(
          IllegalArgumentException.class,
          () -> w.addBatch(simpleBatch(3, new ArrayBinaryColumn("f", storedOnly, docIds, values))));
    }
    dir.close();
  }

  /** A row column that returns a batch doc-id {@code >= numDocs} must be rejected. */
  public void testRowColumnOutOfRangeDocIdThrows() throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      FieldType storedOnly = new FieldType();
      storedOnly.setStored(true);
      storedOnly.freeze();

      // Batch size 3, but the column advertises a value at doc 5.
      int[] docIds = {5};
      BytesRef[] values = {newBytesRef("oob")};
      expectThrows(
          IllegalArgumentException.class,
          () -> w.addBatch(simpleBatch(3, new ArrayBinaryColumn("f", storedOnly, docIds, values))));
    }
    dir.close();
  }

  private static byte[] intsToBytes(int[] values, ByteOrder byteOrder) {
    byte[] bytes = new byte[values.length * Integer.BYTES];
    java.lang.invoke.VarHandle vh =
        byteOrder == ByteOrder.LITTLE_ENDIAN
            ? org.apache.lucene.util.BitUtil.VH_LE_INT
            : org.apache.lucene.util.BitUtil.VH_BE_INT;
    for (int i = 0; i < values.length; i++) {
      vh.set(bytes, i * Integer.BYTES, values[i]);
    }
    return bytes;
  }

  private static byte[] longsToBytes(long[] values, ByteOrder byteOrder) {
    byte[] bytes = new byte[values.length * Long.BYTES];
    java.lang.invoke.VarHandle vh =
        byteOrder == ByteOrder.LITTLE_ENDIAN
            ? org.apache.lucene.util.BitUtil.VH_LE_LONG
            : org.apache.lucene.util.BitUtil.VH_BE_LONG;
    for (int i = 0; i < values.length; i++) {
      vh.set(bytes, i * Long.BYTES, values[i]);
    }
    return bytes;
  }

  // --- Test Column implementations backed by arrays ---

  private static ColumnBatch simpleBatch(int numDocs, Column... columns) {
    return new ColumnBatch() {
      @Override
      public int numDocs() {
        return numDocs;
      }

      @Override
      public Iterable<Column> columns() {
        return List.of(columns);
      }
    };
  }

  private static class ArrayLongColumn extends LongColumn {
    private final int[] docIds;
    private final long[] values;

    ArrayLongColumn(String name, IndexableFieldType fieldType, int[] docIds, long[] values) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }
  }

  /** Plain sparse {@link BinaryColumn} with an optional {@link StoredValue.Type} override. */
  private static class ArrayBinaryColumn extends BinaryColumn {
    private final int[] docIds;
    private final BytesRef[] values;
    private final StoredValue.Type storedType;

    ArrayBinaryColumn(String name, IndexableFieldType fieldType, int[] docIds, BytesRef[] values) {
      this(name, fieldType, docIds, values, StoredValue.Type.BINARY);
    }

    ArrayBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        int[] docIds,
        BytesRef[] values,
        StoredValue.Type storedType) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
      this.storedType = storedType;
    }

    @Override
    public StoredValue.Type storedType() {
      return storedType;
    }

    @Override
    public BinaryTupleCursor tuples() {
      return new BinaryTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef binaryValue() {
          return values[pos];
        }
      };
    }
  }

  /**
   * Sparse fixed-size {@link NumericBinaryColumn} with configurable fixedSize, byteOrder,
   * numericKind, and stored type.
   */
  private static class ArrayNumericBinaryColumn extends NumericBinaryColumn {
    private final int fixedSize;
    private final ByteOrder byteOrder;
    private final NumericKind kind;
    private final StoredValue.Type storedType;
    private final int[] docIds;
    private final BytesRef[] values;

    ArrayNumericBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        int fixedSize,
        ByteOrder byteOrder,
        NumericKind kind,
        int[] docIds,
        BytesRef[] values) {
      this(name, fieldType, fixedSize, byteOrder, kind, StoredValue.Type.BINARY, docIds, values);
    }

    ArrayNumericBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        int fixedSize,
        ByteOrder byteOrder,
        NumericKind kind,
        StoredValue.Type storedType,
        int[] docIds,
        BytesRef[] values) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.fixedSize = fixedSize;
      this.byteOrder = byteOrder;
      this.kind = kind;
      this.storedType = storedType;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public int fixedSize() {
      return fixedSize;
    }

    @Override
    public ByteOrder byteOrder() {
      return byteOrder;
    }

    @Override
    public NumericKind numericKind() {
      return kind;
    }

    @Override
    public StoredValue.Type storedType() {
      return storedType;
    }

    @Override
    public BinaryTupleCursor tuples() {
      return new BinaryTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef binaryValue() {
          return values[pos];
        }
      };
    }
  }

  /** Dense {@link LongColumn} with an optional bulk values cursor. */
  private static class ArrayDenseLongColumn extends LongColumn {
    private final long[] values;

    ArrayDenseLongColumn(String name, IndexableFieldType fieldType, long[] values) {
      super(name, fieldType, Density.DENSE);
      this.values = values;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < values.length ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }

    @Override
    public LongValuesCursor values() {
      return new LongValuesCursor() {
        final LongsRef ref = new LongsRef(values, 0, values.length);
        boolean exhausted;

        @Override
        public LongsRef nextLongs() {
          if (exhausted) return null;
          exhausted = true;
          return ref;
        }
      };
    }
  }

  /** Dense {@link NumericBinaryColumn} with fixed-size encoded numerics. */
  private static class ArrayDenseBinaryColumn extends NumericBinaryColumn {
    private final int byteWidth;
    private final ByteOrder byteOrder;
    private final byte[] bytes;

    ArrayDenseBinaryColumn(
        String name, IndexableFieldType fieldType, ByteOrder byteOrder, byte[] bytes) {
      this(name, fieldType, byteOrder, Long.BYTES, bytes);
    }

    ArrayDenseBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        ByteOrder byteOrder,
        int byteWidth,
        byte[] bytes) {
      super(name, fieldType, Density.DENSE);
      this.byteOrder = byteOrder;
      this.byteWidth = byteWidth;
      this.bytes = bytes;
    }

    @Override
    public int fixedSize() {
      return byteWidth;
    }

    @Override
    public ByteOrder byteOrder() {
      return byteOrder;
    }

    @Override
    public NumericKind numericKind() {
      return byteWidth == Integer.BYTES ? NumericKind.INT : NumericKind.LONG;
    }

    @Override
    public BinaryTupleCursor tuples() {
      return new BinaryTupleCursor() {
        final BytesRef scratch = new BytesRef();
        int pos = -1;
        final int numDocs = bytes.length / byteWidth;

        @Override
        public int nextDoc() {
          pos++;
          return pos < numDocs ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef binaryValue() {
          scratch.bytes = bytes;
          scratch.offset = pos * byteWidth;
          scratch.length = byteWidth;
          return scratch;
        }
      };
    }

    @Override
    public BinaryValuesCursor values() {
      return new BinaryValuesCursor() {
        final BytesRef ref = new BytesRef(bytes, 0, bytes.length);
        boolean exhausted;

        @Override
        public BytesRef nextBytes() {
          if (exhausted) return null;
          exhausted = true;
          return ref;
        }
      };
    }
  }
}
