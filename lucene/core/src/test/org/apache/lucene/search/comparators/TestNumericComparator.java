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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestNumericComparator extends LuceneTestCase {

  public void testEmptyCompetitiveIteratorOptimization() throws Exception {
    final int numDocs = atLeast(1000);
    try (var dir = newDirectory()) {
      try (var writer =
          new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (int i = 0; i < numDocs; i++) {
          var doc = new Document();

          doc.add(NumericDocValuesField.indexedField("long_field_1", i));
          doc.add(new LongPoint("long_field_2", i));
          doc.add(new NumericDocValuesField("long_field_2", i));

          doc.add(NumericDocValuesField.indexedField("int_field_1", i));
          doc.add(new IntPoint("int_field_2", i));
          doc.add(new NumericDocValuesField("int_field_2", i));

          doc.add(NumericDocValuesField.indexedField("float_field_1", i));
          doc.add(new FloatPoint("float_field_2", i));
          doc.add(new NumericDocValuesField("float_field_2", i));

          doc.add(NumericDocValuesField.indexedField("double_field_1", i));
          doc.add(new DoublePoint("double_field_2", i));
          doc.add(new NumericDocValuesField("double_field_2", i));

          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        try (var reader = DirectoryReader.open(writer)) {
          assertEquals(1, reader.leaves().size());
          var leafContext = reader.leaves().get(0);

          // long field 1 ascending:
          assertLongField("long_field_1", false, -1, leafContext);
          // long field 1 descending:
          assertLongField("long_field_1", true, numDocs + 1, leafContext);
          // long field 2 ascending:
          assertLongField("long_field_2", false, -1, leafContext);
          // long field 2 descending:
          assertLongField("long_field_2", true, numDocs + 1, leafContext);

          // int field 1 ascending:
          assertIntField("int_field_1", false, -1, leafContext);
          // int field 1 descending:
          assertIntField("int_field_1", true, numDocs + 1, leafContext);
          // int field 2 ascending:
          assertIntField("int_field_2", false, -1, leafContext);
          // int field 2 descending:
          assertIntField("int_field_2", true, numDocs + 1, leafContext);

          // float field 1 ascending:
          assertFloatField("float_field_1", false, -1, leafContext);
          // float field 1 descending:
          assertFloatField("float_field_1", true, numDocs + 1, leafContext);
          // float field 2 ascending:
          assertFloatField("float_field_2", false, -1, leafContext);
          // float field 2 descending:
          assertFloatField("float_field_2", true, numDocs + 1, leafContext);

          // double field 1 ascending:
          assertDoubleField("double_field_1", false, -1, leafContext);
          // double field 1 descending:
          assertDoubleField("double_field_1", true, numDocs + 1, leafContext);
          // double field 2 ascending:
          assertDoubleField("double_field_2", false, -1, leafContext);
          // double field 2 descending:
          assertDoubleField("double_field_2", true, numDocs + 1, leafContext);
        }
      }
    }
  }

  public void testEmptyCompetitiveIteratorOptimizationWithMissingValue() throws Exception {
    final int numDocs = atLeast(1000);
    try (var dir = newDirectory()) {
      try (var writer =
          new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        // index docs without missing values:
        for (int i = 0; i < numDocs; i++) {
          var doc = new Document();

          doc.add(NumericDocValuesField.indexedField("long_field_1", i));
          doc.add(new LongPoint("long_field_2", i));
          doc.add(new NumericDocValuesField("long_field_2", i));

          writer.addDocument(doc);
          if (i % 100 == 0) {
            writer.flush();
          }
        }

        // Index one doc with without long_field_1 and long_field_2 fields
        var doc = new Document();
        doc.add(new NumericDocValuesField("another_field", numDocs));
        writer.addDocument(doc);
        writer.flush();

        try (var reader = DirectoryReader.open(writer)) {
          var indexSearcher = newSearcher(reader);
          indexSearcher.setQueryCache(null);
          {
            var sortField =
                new SortField("long_field_1", SortField.Type.LONG, false, Long.MIN_VALUE);
            var topDocs = indexSearcher.search(MatchAllDocsQuery.INSTANCE, 3, new Sort(sortField));
            assertEquals(numDocs, topDocs.scoreDocs[0].doc);
            assertEquals(Long.MIN_VALUE, ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
            assertEquals(0, topDocs.scoreDocs[1].doc);
            assertEquals(0L, ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
            assertEquals(1, topDocs.scoreDocs[2].doc);
            assertEquals(1L, ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);
          }
          {
            var sortField =
                new SortField("long_field_2", SortField.Type.LONG, false, Long.MIN_VALUE);
            var topDocs = indexSearcher.search(MatchAllDocsQuery.INSTANCE, 3, new Sort(sortField));
            assertEquals(numDocs, topDocs.scoreDocs[0].doc);
            assertEquals(Long.MIN_VALUE, ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
            assertEquals(0, topDocs.scoreDocs[1].doc);
            assertEquals(0L, ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
            assertEquals(1, topDocs.scoreDocs[2].doc);
            assertEquals(1L, ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);
          }
        }
      }
    }
  }

  public void testEmptyCompetitiveIteratorOptimizationAndHitsThresholdReached() throws Exception {
    final int numDocs =
        TestUtil.nextInt(random(), 128, 512); // Below IndexSearcher.DEFAULT_HITS_THRESHOLD
    try (var dir = newDirectory()) {
      try (var writer =
          new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (int i = 0; i < numDocs; i++) {
          var doc = new Document();

          doc.add(NumericDocValuesField.indexedField("field_1", i));
          doc.add(new LongPoint("field_2", i));
          doc.add(new NumericDocValuesField("field_2", i));

          writer.addDocument(doc);
          if (i % 100 == 0) {
            writer.flush();
          }
        }

        try (var reader = DirectoryReader.open(writer)) {
          var indexSearcher = newSearcher(reader);
          indexSearcher.setQueryCache(null);
          for (String field : new String[] {"field_1", "field_2"}) {
            var sortField = new SortField(field, SortField.Type.LONG, false);
            var topDocs = indexSearcher.search(MatchAllDocsQuery.INSTANCE, 3, new Sort(sortField));
            assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation());
            assertEquals(numDocs, topDocs.totalHits.value());
            assertEquals(0, topDocs.scoreDocs[0].doc);
            assertEquals(0L, ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
            assertEquals(1, topDocs.scoreDocs[1].doc);
            assertEquals(1L, ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
            assertEquals(2, topDocs.scoreDocs[2].doc);
            assertEquals(2L, ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);
          }
        }
      }
    }
  }

  private static void assertLongField(
      String fieldName, boolean reverse, int bottom, LeafReaderContext leafContext)
      throws IOException {
    var comparator1 =
        (LongComparator)
            new SortField(fieldName, SortField.Type.LONG, reverse)
                .getComparator(1, Pruning.GREATER_THAN_OR_EQUAL_TO);
    comparator1.queueFull = true;
    comparator1.hitsThresholdReached = true;
    comparator1.bottom = bottom;
    var leafComparator = comparator1.getLeafComparator(leafContext);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, leafComparator.competitiveIterator().nextDoc());
  }

  private static void assertIntField(
      String fieldName, boolean reverse, int bottom, LeafReaderContext leafContext)
      throws IOException {
    var comparator1 =
        (IntComparator)
            new SortField(fieldName, SortField.Type.INT, reverse)
                .getComparator(1, Pruning.GREATER_THAN_OR_EQUAL_TO);
    comparator1.queueFull = true;
    comparator1.hitsThresholdReached = true;
    comparator1.bottom = bottom;
    var leafComparator = comparator1.getLeafComparator(leafContext);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, leafComparator.competitiveIterator().nextDoc());
  }

  private static void assertFloatField(
      String fieldName, boolean reverse, int bottom, LeafReaderContext leafContext)
      throws IOException {
    var comparator1 =
        (FloatComparator)
            new SortField(fieldName, SortField.Type.FLOAT, reverse)
                .getComparator(1, Pruning.GREATER_THAN_OR_EQUAL_TO);
    comparator1.queueFull = true;
    comparator1.hitsThresholdReached = true;
    comparator1.bottom = bottom;
    var leafComparator = comparator1.getLeafComparator(leafContext);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, leafComparator.competitiveIterator().nextDoc());
  }

  private static void assertDoubleField(
      String fieldName, boolean reverse, int bottom, LeafReaderContext leafContext)
      throws IOException {
    var comparator1 =
        (DoubleComparator)
            new SortField(fieldName, SortField.Type.DOUBLE, reverse)
                .getComparator(1, Pruning.GREATER_THAN_OR_EQUAL_TO);
    comparator1.queueFull = true;
    comparator1.hitsThresholdReached = true;
    comparator1.bottom = bottom;
    var leafComparator = comparator1.getLeafComparator(leafContext);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, leafComparator.competitiveIterator().nextDoc());
  }
}
