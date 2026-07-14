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

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

/** Tests for {@link LongColumn} batch indexing. */
public class TestColumnBatchLongColumn extends LuceneTestCase {

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

  public void testPointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Create a points-only FieldType (1 dimension, Integer.BYTES)
    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    int[] raw = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("point", pointType, LongColumn.NumericKind.INT, docIds, values)));

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
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "field", pointAndDvType, LongColumn.NumericKind.INT, docIds, values)));

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
    long[] values = {42};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("point", pointType, LongColumn.NumericKind.INT, docIds, values)));

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
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "pt", storedPointType, LongColumn.NumericKind.INT, docIds, values)));

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

  public void testIntSparseNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Sparse: only docs 0 and 2 have values.
    int[] docIds = {0, 2};
    int[] raw = {-7, 9};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "val", NumericDocValuesField.TYPE, LongColumn.NumericKind.INT, docIds, values)));

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

  public void testLongColumnPointWidthMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES); // expects 4 bytes
    pointType.freeze();

    // LONG kind implies 8-byte point bytes; should fail validation against a 4-byte point type.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayLongColumn(
                        "pt",
                        pointType,
                        LongColumn.NumericKind.LONG,
                        new int[] {0},
                        new long[] {1}))));

    w.close();
    dir.close();
  }

  public void testLongColumnMultiDimPointsThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(2, Long.BYTES);
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

  public void testStoredTypeIntegerFromLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    int[] raw = {1, -2, 3};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "val", type, LongColumn.NumericKind.INT, new int[] {0, 1, 2}, values)));

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

  public void testStoredTypeLongFromLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    long[] raw = {Long.MIN_VALUE, 0L, Long.MAX_VALUE};
    w.addBatch(simpleBatch(3, new ArrayLongColumn("val", type, new int[] {0, 1, 2}, raw.clone())));

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

  public void testStoredTypeFloatFromLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    float[] raw = {1.5f, -2.25f, Float.MAX_VALUE};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = NumericUtils.floatToSortableInt(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "val", type, LongColumn.NumericKind.FLOAT, new int[] {0, 1, 2}, values)));

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

  public void testStoredTypeDoubleFromLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();

    double[] raw = {1.5d, -2.25d, Double.MAX_VALUE};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = NumericUtils.doubleToSortableLong(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "val", type, LongColumn.NumericKind.DOUBLE, new int[] {0, 1, 2}, values)));

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

  public void testStoredTypeFloatWithNumericDV() throws IOException {
    // FLOAT kind on a LongColumn that also feeds NumericDV.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    float[] raw = {1.5f, -2.25f, 42.0f};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = NumericUtils.floatToSortableInt(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "val", type, LongColumn.NumericKind.FLOAT, new int[] {0, 1, 2}, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Stored values decoded as floats.
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(
          raw[i], storedFields.document(i).getField("val").numericValue().floatValue(), 0f);
    }

    // NumericDV stores the sortable-int encoding sign-extended to long.
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeIntegerWithNumericDV() throws IOException {
    // INT kind on a LongColumn that also feeds NumericDV.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    int[] raw = {Integer.MIN_VALUE, -1, 0, 42, Integer.MAX_VALUE};
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.INT, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(raw[i], storedFields.document(i).getField("val").numericValue().intValue());
    }

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredTypeDoubleWithNumericDV() throws IOException {
    // DOUBLE kind on a LongColumn that also feeds NumericDV.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setStored(true);
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    double[] raw = {Double.NEGATIVE_INFINITY, -1.5d, 0.0d, 2.25d, Double.POSITIVE_INFINITY};
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = NumericUtils.doubleToSortableLong(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.DOUBLE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < raw.length; i++) {
      assertEquals(
          raw[i], storedFields.document(i).getField("val").numericValue().doubleValue(), 0d);
    }

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testNumericKindIntPointsAndDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Integer.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    int[] raw = {-5, -1, 0, 7, Integer.MAX_VALUE};
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.INT, docIds, values)));

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

  public void testNumericKindLongPointsAndDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Long.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    long[] raw = {Long.MIN_VALUE, -100L, 0L, 42L, Long.MAX_VALUE};
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
    }
    w.addBatch(simpleBatch(raw.length, new ArrayLongColumn("val", type, docIds, raw.clone())));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], dv.nextValue());
    }

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length, searcher.count(LongPoint.newRangeQuery("val", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, searcher.count(LongPoint.newExactQuery("val", Long.MIN_VALUE)));
    assertEquals(3, searcher.count(LongPoint.newRangeQuery("val", -100L, 42L)));

    r.close();
    w.close();
    dir.close();
  }

  public void testNumericKindFloatPointsAndDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Float.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    float[] raw = {Float.NEGATIVE_INFINITY, -1.5f, 0.0f, 2.25f, Float.POSITIVE_INFINITY};
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = NumericUtils.floatToSortableInt(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.FLOAT, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // DV stores the sortable-int encoding; decode via sortableIntToFloat.
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], NumericUtils.sortableIntToFloat((int) dv.nextValue()), 0f);
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
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = NumericUtils.doubleToSortableLong(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.DOUBLE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(raw[i], NumericUtils.sortableLongToDouble(dv.nextValue()), 0d);
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
                    new ArrayLongColumn(
                        "val",
                        type,
                        LongColumn.NumericKind.LONG,
                        new int[] {0},
                        new long[] {1L}))));

    w.close();
    dir.close();
  }

  public void testNumericKindPointsAndDVWidthMismatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FLOAT kind requires a 4-byte point field; declaring Long.BYTES should throw.
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
                    new ArrayLongColumn(
                        "val",
                        type,
                        LongColumn.NumericKind.FLOAT,
                        new int[] {0},
                        new long[] {1L}))));

    w.close();
    dir.close();
  }

  public void testNumericKindFloatDVOnly() throws IOException {
    // DV only (no points): LongColumn stores the long value unchanged. For FLOAT, callers feed
    // sortable-int bits in the low 32 bits, and DV reads them back sign-extended to long.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    float[] raw = {1.5f, -2.25f, Float.MAX_VALUE};
    long[] values = new long[raw.length];
    int[] docIds = new int[raw.length];
    for (int i = 0; i < raw.length; i++) {
      docIds[i] = i;
      values[i] = NumericUtils.floatToSortableInt(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayLongColumn("val", type, LongColumn.NumericKind.FLOAT, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  // -----------------------------------------------------------------------------
  // Dense 1-D points fast path (PointValuesWriter#addDense1DIntValues /
  // addDense1DLongValues, LongValuesCursor#fillIntPoints / fillLongPoints).
  // -----------------------------------------------------------------------------

  public void testDensePointsIntOnly() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Integer.BYTES);
    type.freeze();

    int[] raw = {Integer.MIN_VALUE, -7, 0, 13, Integer.MAX_VALUE};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            raw.length, new ArrayDenseLongColumn("v", type, LongColumn.NumericKind.INT, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(IntPoint.newRangeQuery("v", Integer.MIN_VALUE, Integer.MAX_VALUE)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("v", -7)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("v", 13)));
    assertEquals(0, searcher.count(IntPoint.newExactQuery("v", 99)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("v", -7, 13)));

    r.close();
    w.close();
    dir.close();
  }

  public void testDensePointsLongOnly() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Long.BYTES);
    type.freeze();

    long[] values = {Long.MIN_VALUE, -100L, 0L, 42L, Long.MAX_VALUE};
    w.addBatch(simpleBatch(values.length, new ArrayDenseLongColumn("v", type, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        values.length,
        searcher.count(LongPoint.newRangeQuery("v", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, searcher.count(LongPoint.newExactQuery("v", -100L)));
    assertEquals(3, searcher.count(LongPoint.newRangeQuery("v", -100L, 42L)));

    r.close();
    w.close();
    dir.close();
  }

  public void testDensePointsFloatOnly() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Float.BYTES);
    type.freeze();

    float[] raw = {Float.NEGATIVE_INFINITY, -1.5f, 0.0f, 2.25f, Float.POSITIVE_INFINITY};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = NumericUtils.floatToSortableInt(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length, new ArrayDenseLongColumn("v", type, LongColumn.NumericKind.FLOAT, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(
            FloatPoint.newRangeQuery("v", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));
    assertEquals(1, searcher.count(FloatPoint.newExactQuery("v", -1.5f)));
    assertEquals(3, searcher.count(FloatPoint.newRangeQuery("v", -1.5f, 2.25f)));

    r.close();
    w.close();
    dir.close();
  }

  public void testDensePointsDoubleOnly() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Double.BYTES);
    type.freeze();

    double[] raw = {Double.NEGATIVE_INFINITY, -1.5, 0.0, 2.25, Double.POSITIVE_INFINITY};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = NumericUtils.doubleToSortableLong(raw[i]);
    }
    w.addBatch(
        simpleBatch(
            raw.length,
            new ArrayDenseLongColumn("v", type, LongColumn.NumericKind.DOUBLE, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(
            DoublePoint.newRangeQuery("v", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
    assertEquals(1, searcher.count(DoublePoint.newExactQuery("v", -1.5)));
    assertEquals(3, searcher.count(DoublePoint.newRangeQuery("v", -1.5, 2.25)));

    r.close();
    w.close();
    dir.close();
  }

  public void testDensePointsIntWithSortedNumericDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Integer.BYTES);
    type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    type.freeze();

    int[] raw = {-5, -1, 0, 7, Integer.MAX_VALUE};
    long[] values = new long[raw.length];
    for (int i = 0; i < raw.length; i++) {
      values[i] = raw[i];
    }
    w.addBatch(
        simpleBatch(
            raw.length, new ArrayDenseLongColumn("v", type, LongColumn.NumericKind.INT, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("v");
    for (int i = 0; i < raw.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(1, dv.docValueCount());
      assertEquals(raw[i], dv.nextValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        raw.length,
        searcher.count(IntPoint.newRangeQuery("v", Integer.MIN_VALUE, Integer.MAX_VALUE)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("v", -1, 7)));

    r.close();
    w.close();
    dir.close();
  }

  public void testDensePointsLongWithNumericDV() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Long.BYTES);
    type.setDocValuesType(DocValuesType.NUMERIC);
    type.freeze();

    long[] values = {Long.MIN_VALUE, -100L, 0L, 42L, Long.MAX_VALUE};
    w.addBatch(simpleBatch(values.length, new ArrayDenseLongColumn("v", type, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("v");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(
        values.length,
        searcher.count(LongPoint.newRangeQuery("v", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(3, searcher.count(LongPoint.newRangeQuery("v", -100L, 42L)));

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Dense long batch larger than {@code POINTS_BUFFER_LONG_VALUES} (512) so the chunked encoding
   * loop in {@code PointValuesWriter#addDense1DLongValues} runs more than once.
   */
  public void testDensePointsLongLargeBatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType type = new FieldType();
    type.setDimensions(1, Long.BYTES);
    type.freeze();

    final int n = 1100; // > 2 * 512 chunks
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = i * 1_000_000L - 100L; // mix of negative and positive
    }
    w.addBatch(simpleBatch(n, new ArrayDenseLongColumn("v", type, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(n, searcher.count(LongPoint.newRangeQuery("v", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, searcher.count(LongPoint.newExactQuery("v", values[0])));
    assertEquals(1, searcher.count(LongPoint.newExactQuery("v", values[n - 1])));
    assertEquals(1, searcher.count(LongPoint.newExactQuery("v", values[511]))); // chunk boundary
    assertEquals(1, searcher.count(LongPoint.newExactQuery("v", values[512])));
    assertEquals(0, searcher.count(LongPoint.newExactQuery("v", -1L)));
    assertEquals(n, searcher.count(LongPoint.newRangeQuery("v", values[0], values[n - 1])));

    r.close();
    w.close();
    dir.close();
  }
}
