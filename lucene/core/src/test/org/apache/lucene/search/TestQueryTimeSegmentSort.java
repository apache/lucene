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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestQueryTimeSegmentSort extends LuceneTestCase {

  public void testQueryTimeSegmentSorts() throws Exception {

    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

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
    IndexSearcher searcher = new IndexSearcher(reader);

    assertSegmentOrder(searcher, sort("long_points", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("long_skipper", SortField.Type.LONG, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("int_points", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("int_skipper", SortField.Type.INT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("double_points", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        searcher, sort("double_skipper", SortField.Type.DOUBLE, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("float_points", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);
    assertSegmentOrder(searcher, sort("float_skipper", SortField.Type.FLOAT, true), 4, 3, 2, 1, 0);

    assertSegmentOrder(searcher, sort("long_points", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(searcher, sort("long_skipper", SortField.Type.LONG, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(searcher, sort("int_points", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(searcher, sort("int_skipper", SortField.Type.INT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("double_points", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("double_skipper", SortField.Type.DOUBLE, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(searcher, sort("float_points", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);
    assertSegmentOrder(searcher, sort("float_skipper", SortField.Type.FLOAT, false), 0, 1, 2, 3, 4);

    reader.close();
    dir.close();
  }

  private static Sort sort(String field, SortField.Type type, boolean reverse) {
    return sort(field, type, reverse, null);
  }

  private static Sort sort(
      String field, SortField.Type type, boolean reverse, Object missingValue) {
    SortField sf = new SortField(field, type, reverse);
    if (missingValue != null) {
      sf.setMissingValue(missingValue);
    }
    return new Sort(sf);
  }

  public void testQueryTimeSegmentSortsWithMissingValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
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
    IndexSearcher searcher = new IndexSearcher(reader);

    assertSegmentOrder(
        searcher, sort("long_points", SortField.Type.LONG, true, Long.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        searcher, sort("long_points", SortField.Type.LONG, false, Long.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("long_points", SortField.Type.LONG, true, Long.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        searcher, sort("long_points", SortField.Type.LONG, false, Long.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        searcher, sort("long_skipper", SortField.Type.LONG, true, Long.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        searcher, sort("long_skipper", SortField.Type.LONG, false, Long.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("long_skipper", SortField.Type.LONG, true, Long.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        searcher, sort("long_skipper", SortField.Type.LONG, false, Long.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        searcher, sort("int_points", SortField.Type.INT, true, Integer.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        searcher, sort("int_points", SortField.Type.INT, false, Integer.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("int_points", SortField.Type.INT, true, Integer.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        searcher, sort("int_points", SortField.Type.INT, false, Integer.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        searcher, sort("int_skipper", SortField.Type.INT, true, Integer.MAX_VALUE), 2, 4, 3, 1, 0);
    assertSegmentOrder(
        searcher, sort("int_skipper", SortField.Type.INT, false, Integer.MAX_VALUE), 0, 1, 2, 3, 4);
    assertSegmentOrder(
        searcher, sort("int_skipper", SortField.Type.INT, true, Integer.MIN_VALUE), 4, 3, 2, 1, 0);
    assertSegmentOrder(
        searcher, sort("int_skipper", SortField.Type.INT, false, Integer.MIN_VALUE), 2, 0, 1, 3, 4);

    assertSegmentOrder(
        searcher,
        sort("double_points", SortField.Type.DOUBLE, true, Double.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("double_points", SortField.Type.DOUBLE, false, Double.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        searcher,
        sort("double_points", SortField.Type.DOUBLE, true, Double.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("double_points", SortField.Type.DOUBLE, false, Double.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        searcher,
        sort("double_skipper", SortField.Type.DOUBLE, true, Double.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("double_skipper", SortField.Type.DOUBLE, false, Double.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        searcher,
        sort("double_skipper", SortField.Type.DOUBLE, true, Double.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("double_skipper", SortField.Type.DOUBLE, false, Double.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        searcher,
        sort("float_points", SortField.Type.FLOAT, true, Float.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("float_points", SortField.Type.FLOAT, false, Float.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        searcher,
        sort("float_points", SortField.Type.FLOAT, true, Float.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("float_points", SortField.Type.FLOAT, false, Float.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    assertSegmentOrder(
        searcher,
        sort("float_skipper", SortField.Type.FLOAT, true, Float.POSITIVE_INFINITY),
        2,
        4,
        3,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("float_skipper", SortField.Type.FLOAT, false, Float.POSITIVE_INFINITY),
        0,
        1,
        2,
        3,
        4);
    assertSegmentOrder(
        searcher,
        sort("float_skipper", SortField.Type.FLOAT, true, Float.NEGATIVE_INFINITY),
        4,
        3,
        2,
        1,
        0);
    assertSegmentOrder(
        searcher,
        sort("float_skipper", SortField.Type.FLOAT, false, Float.NEGATIVE_INFINITY),
        2,
        0,
        1,
        3,
        4);

    reader.close();
    dir.close();
  }

  private void assertSegmentOrder(IndexSearcher searcher, Sort sort, int... expectedOrds)
      throws IOException {
    SegmentOrderCollectorManager manager = new SegmentOrderCollectorManager(sort, false);
    List<LeafReaderContext> leaves = searcher.search(new MatchAllDocsQuery(), manager);
    for (int i = 0; i < leaves.size(); i++) {
      assertEquals(expectedOrds[i], leaves.get(i).ord);
    }

    // If we're not pruning then we don't sort the leaves
    manager = new SegmentOrderCollectorManager(sort, true);
    leaves = searcher.search(new MatchAllDocsQuery(), manager);
    for (int i = 0; i < leaves.size(); i++) {
      assertEquals(i, leaves.get(i).ord);
    }
  }

  private static class SegmentOrderCollector implements Collector {

    List<LeafReaderContext> leaves = new ArrayList<>();
    final Sort sort;
    final boolean collectAll;

    private SegmentOrderCollector(Sort sort, boolean collectAll) {
      this.sort = sort;
      this.collectAll = collectAll;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      leaves.add(context);
      return new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) throws IOException {}

        @Override
        public void collect(int doc) throws IOException {}
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return collectAll ? ScoreMode.COMPLETE : ScoreMode.TOP_DOCS;
    }

    @Override
    public Comparator<LeafReaderContext> getLeafReaderComparator() {
      return collectAll ? null : sort.getLeafReaderComparator();
    }
  }

  private static class SegmentOrderCollectorManager
      implements CollectorManager<SegmentOrderCollector, List<LeafReaderContext>> {

    final Sort sort;
    final boolean collectAll;

    private SegmentOrderCollectorManager(Sort sort, boolean collectAll) {
      this.sort = sort;
      this.collectAll = collectAll;
    }

    @Override
    public SegmentOrderCollector newCollector() throws IOException {
      return new SegmentOrderCollector(sort, collectAll);
    }

    @Override
    public List<LeafReaderContext> reduce(Collection<SegmentOrderCollector> collectors)
        throws IOException {
      return collectors.stream().flatMap(c -> c.leaves.stream()).toList();
    }
  }
}
