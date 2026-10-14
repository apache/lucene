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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentOrder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestSegmentReordering extends LuceneTestCase {

  public void testSingleValuedNumericSorts() throws Exception {

    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, iwc);

    for (int i = 0; i < 500; i++) {
      Document doc = new Document();
      doc.add(new LongField("long_points", i, Field.Store.NO));
      doc.add(NumericDocValuesField.indexedField("long_skipper", i));
      doc.add(new DoubleField("double_points", i * 1.5, Field.Store.NO));
      doc.add(
          NumericDocValuesField.indexedField(
              "double_skipper", NumericUtils.doubleToSortableLong(i * 1.5)));
      doc.add(new IntField("int_points", i, Field.Store.NO));
      doc.add(NumericDocValuesField.indexedField("int_skipper", i));
      doc.add(new FloatField("float_points", i * 1.5f, Field.Store.NO));
      doc.add(
          NumericDocValuesField.indexedField(
              "float_skipper", NumericUtils.floatToSortableInt(i * 1.5f)));
      writer.addDocument(doc);
      if (i % 125 == 0) {
        writer.commit();
      }
    }
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);

    assertSegmentOrder(reader, sort("long_points", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("long_skipper", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("int_points", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("int_skipper", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("double_points", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("double_skipper", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("float_points", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, sort("float_skipper", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);

    assertSegmentOrder(reader, sort("long_points", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("long_skipper", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("int_points", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("int_skipper", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("double_points", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("double_skipper", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("float_points", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, sort("float_skipper", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);

    reader.close();
    dir.close();
  }

  public void testMultiValuedSegmentSorts() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, iwc);

    for (int i = 0; i < 1000; i += 2) {
      Document doc = new Document();
      doc.add(new LongField("long_points", i, Field.Store.NO));
      doc.add(SortedNumericDocValuesField.indexedField("long_skipper", i));
      doc.add(new LongField("long_points", i + 1, Field.Store.NO));
      doc.add(SortedNumericDocValuesField.indexedField("long_skipper", i + 1));

      doc.add(new DoubleField("double_points", i * 1.5, Field.Store.NO));
      doc.add(
          SortedNumericDocValuesField.indexedField(
              "double_skipper", NumericUtils.doubleToSortableLong(i * 1.5)));
      doc.add(new DoubleField("double_points", i * 1.5 + 1, Field.Store.NO));
      doc.add(
          SortedNumericDocValuesField.indexedField(
              "double_skipper", NumericUtils.doubleToSortableLong(i * 1.5 + 1)));
      doc.add(new IntField("int_points", i, Field.Store.NO));
      doc.add(SortedNumericDocValuesField.indexedField("int_skipper", i));
      doc.add(new IntField("int_points", i + 1, Field.Store.NO));
      doc.add(SortedNumericDocValuesField.indexedField("int_skipper", i + 1));

      doc.add(new FloatField("float_points", i * 1.5f, Field.Store.NO));
      doc.add(
          SortedNumericDocValuesField.indexedField(
              "float_skipper", NumericUtils.floatToSortableInt(i * 1.5f)));
      doc.add(new FloatField("float_points", i * 1.5f + 1, Field.Store.NO));
      doc.add(
          SortedNumericDocValuesField.indexedField(
              "float_skipper", NumericUtils.floatToSortableInt(i * 1.5f + 1)));

      writer.addDocument(doc);
      if (i % 250 == 0) {
        writer.commit();
      }
    }
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);

    assertSegmentOrder(reader, msort("long_points", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("long_skipper", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("int_points", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("int_skipper", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("double_points", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("double_skipper", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("float_points", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(reader, msort("float_skipper", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);

    assertSegmentOrder(reader, msort("long_points", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("long_skipper", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("int_points", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("int_skipper", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("double_points", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        reader, msort("double_skipper", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("float_points", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(reader, msort("float_skipper", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);

    reader.close();
    dir.close();
  }

  private static Sort msort(String field, SortField.Type type, boolean reverse) {
    return new Sort(new SortedNumericSortField(field, type, reverse));
  }

  private static Sort sort(String field, SortField.Type type, boolean reverse) {
    return sort(field, type, reverse, null);
  }

  private static Sort sort(
      String field, SortField.Type type, boolean reverse, Object missingValue) {
    SortField sf = new SortField(field, type, reverse, missingValue);
    return new Sort(sf);
  }

  public void testNumericSegmentSortsWithMissingValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, iwc);
    for (int i = 0; i < 500; i++) {
      Document doc = new Document();
      doc.add(new StringField("string", "foo", Field.Store.NO));
      if (i != 200) {
        doc.add(new LongField("long_points", i, Field.Store.NO));
        doc.add(NumericDocValuesField.indexedField("long_skipper", i));
        doc.add(new DoubleField("double_points", i * 1.5, Field.Store.NO));
        doc.add(
            NumericDocValuesField.indexedField(
                "double_skipper", NumericUtils.doubleToSortableLong(i * 1.5)));
        doc.add(new IntField("int_points", i, Field.Store.NO));
        doc.add(NumericDocValuesField.indexedField("int_skipper", i));
        doc.add(new FloatField("float_points", i * 1.5f, Field.Store.NO));
        doc.add(
            NumericDocValuesField.indexedField(
                "float_skipper", NumericUtils.floatToSortableInt(i * 1.5f)));
      }
      writer.addDocument(doc);
      if (i % 125 == 0) {
        writer.commit();
      }
    }
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);

    assertSegmentOrder(
        reader, sort("long_points", SortField.Type.LONG, true, Long.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        reader, sort("long_points", SortField.Type.LONG, false, Long.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        reader, sort("long_points", SortField.Type.LONG, true, Long.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        reader, sort("long_points", SortField.Type.LONG, false, Long.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        reader, sort("long_skipper", SortField.Type.LONG, true, Long.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        reader, sort("long_skipper", SortField.Type.LONG, false, Long.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        reader, sort("long_skipper", SortField.Type.LONG, true, Long.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        reader, sort("long_skipper", SortField.Type.LONG, false, Long.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        reader, sort("int_points", SortField.Type.INT, true, Integer.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        reader, sort("int_points", SortField.Type.INT, false, Integer.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        reader, sort("int_points", SortField.Type.INT, true, Integer.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        reader, sort("int_points", SortField.Type.INT, false, Integer.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        reader, sort("int_skipper", SortField.Type.INT, true, Integer.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        reader, sort("int_skipper", SortField.Type.INT, false, Integer.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        reader, sort("int_skipper", SortField.Type.INT, true, Integer.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        reader, sort("int_skipper", SortField.Type.INT, false, Integer.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        reader,
        sort("double_points", SortField.Type.DOUBLE, true, Double.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("double_points", SortField.Type.DOUBLE, false, Double.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        reader,
        sort("double_points", SortField.Type.DOUBLE, true, Double.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("double_points", SortField.Type.DOUBLE, false, Double.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        reader,
        sort("double_skipper", SortField.Type.DOUBLE, true, Double.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("double_skipper", SortField.Type.DOUBLE, false, Double.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        reader,
        sort("double_skipper", SortField.Type.DOUBLE, true, Double.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("double_skipper", SortField.Type.DOUBLE, false, Double.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        reader,
        sort("float_points", SortField.Type.FLOAT, true, Float.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("float_points", SortField.Type.FLOAT, false, Float.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        reader,
        sort("float_points", SortField.Type.FLOAT, true, Float.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("float_points", SortField.Type.FLOAT, false, Float.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        reader,
        sort("float_skipper", SortField.Type.FLOAT, true, Float.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("float_skipper", SortField.Type.FLOAT, false, Float.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        reader,
        sort("float_skipper", SortField.Type.FLOAT, true, Float.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        reader,
        sort("float_skipper", SortField.Type.FLOAT, false, Float.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    reader.close();
    dir.close();
  }

  private void assertSegmentOrder(IndexReader reader, Sort sort, int... expectedOrds)
      throws IOException {

    List<LeafReaderContext> leaves = reader.leaves();
    assertEquals(expectedOrds.length, leaves.size());
    IndexReader reorderedReader = SegmentOrder.fromSort(sort).reorder(reader);

    List<LeafReaderContext> reorderedLeaves = reorderedReader.leaves();
    for (int i = 0; i < expectedOrds.length; i++) {
      assertEquals(reorderedLeaves.get(i).reader(), leaves.get(expectedOrds[i]).reader());
    }
  }
}
