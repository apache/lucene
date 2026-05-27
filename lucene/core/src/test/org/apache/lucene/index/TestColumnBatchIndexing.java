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

import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayBinaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ThrowingBinaryColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ThrowingDenseLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ThrowingFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ThrowingLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.floatVectorType;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Generic / cross-cutting tests for column-oriented batch indexing via {@link
 * IndexWriter#addBatch}. Type-specific tests live in {@code
 * org.apache.lucene.document.column.TestColumnBatch{Long,Binary,Vector}Column}.
 */
public class TestColumnBatchIndexing extends LuceneTestCase {

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

  public void testAddBatchOverMaxDocsRollsBackReservations() throws IOException {
    final int maxDocs = 10;
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      IndexWriter.setMaxDocs(maxDocs);
      try {
        // Batch is larger than the cap — must throw without reserving any docs permanently.
        int oversized = maxDocs + 5;
        int[] docIds = new int[oversized];
        long[] values = new long[oversized];
        for (int i = 0; i < oversized; i++) {
          docIds[i] = i;
          values[i] = i;
        }
        IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class,
                () ->
                    w.addBatch(
                        simpleBatch(
                            oversized,
                            new ArrayLongColumn("v", NumericDocValuesField.TYPE, docIds, values))));
        assertTrue(e.getMessage(), e.getMessage().contains("number of documents"));

        // If reservations leaked, even a single addDocument would now fail. Fill the index up
        // to the cap and then verify the next addDocument is the one that fails.
        for (int i = 0; i < maxDocs; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("v", i));
          w.addDocument(doc);
        }
        IllegalArgumentException overflow =
            expectThrows(
                IllegalArgumentException.class,
                () -> {
                  Document doc = new Document();
                  doc.add(new NumericDocValuesField("v", 999));
                  w.addDocument(doc);
                });
        assertTrue(overflow.getMessage(), overflow.getMessage().contains("number of documents"));
      } finally {
        IndexWriter.setMaxDocs(IndexWriter.MAX_DOCS);
      }
    }
    dir.close();
  }

  public void testParentFieldNameRejectedAsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setParentField("_parent");
    try (IndexWriter w = new IndexWriter(dir, config)) {
      int[] docIds = {0, 1, 2};
      long[] values = {1, 2, 3};
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  w.addBatch(
                      simpleBatch(
                          3,
                          new ArrayLongColumn(
                              "_parent", NumericDocValuesField.TYPE, docIds, values))));
      assertTrue(e.getMessage(), e.getMessage().contains("_parent"));
      assertTrue(e.getMessage(), e.getMessage().contains("reserved"));
    }
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

  public void testSparseStoredFieldPreservesNumDocs() throws IOException {
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

  public void testSparseIndexedFieldsPreservesNumDocs() throws IOException {
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

  public void testSparseBatchMatchesDocByDoc() throws IOException {
    FieldType storedIndexed = new FieldType(StringField.TYPE_STORED);
    storedIndexed.freeze();

    // 7 docs; only docs 1, 2, and 5 have values for the row column.
    int[] docIds = {1, 2, 5};
    String[] values = {"alpha", "beta", "gamma"};
    int totalDocs = 7;

    // --- Batch path ---
    Directory batchDir = newDirectory();
    IndexWriterConfig batchIwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    batchIwc.setRAMBufferSizeMB(16);
    batchIwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (IndexWriter batchW = new IndexWriter(batchDir, batchIwc)) {
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

    Directory singleDir = newDirectory();
    IndexWriterConfig singleIwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    singleIwc.setRAMBufferSizeMB(16);
    singleIwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (IndexWriter singleW = new IndexWriter(singleDir, singleIwc)) {
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
      singleW.forceMerge(1);
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

  public void testRowColumnOutOfOrderDocIdThrows() throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      FieldType storedOnly = new FieldType();
      storedOnly.setStored(true);
      storedOnly.freeze();

      int[] docIds = {2, 1};
      BytesRef[] values = {newBytesRef("a"), newBytesRef("b")};
      expectThrows(
          IllegalArgumentException.class,
          () -> w.addBatch(simpleBatch(3, new ArrayBinaryColumn("f", storedOnly, docIds, values))));
    }
    dir.close();
  }

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

  public void testVectorMixedWithLongAndBinary() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.DOT_PRODUCT);
    float[][] vectors = {{0.6f, 0.8f}, {0.8f, 0.6f}, {1.0f, 0.0f}};
    long[] longs = {10, 20, 30};
    BytesRef[] bins = {newBytesRef("a"), newBytesRef("b"), newBytesRef("c")};
    int[] ids = {0, 1, 2};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDenseFloatVectorColumn("v", vectorType, vectors),
            new ArrayLongColumn("num", NumericDocValuesField.TYPE, ids, longs),
            new ArrayBinaryColumn("bin", BinaryDocValuesField.TYPE, ids, bins)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues nums = leaf.getNumericDocValues("num");
    BinaryDocValues binDv = leaf.getBinaryDocValues("bin");
    FloatVectorValues vec = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = vec.iterator();
    for (int i = 0; i < 3; i++) {
      assertEquals(i, nums.nextDoc());
      assertEquals(longs[i], nums.longValue());
      assertEquals(i, binDv.nextDoc());
      assertEquals(bins[i], binDv.binaryValue());
      assertEquals(i, it.nextDoc());
      assertArrayEquals(vectors[i], vec.vectorValue(it.index()), 0f);
    }
    r.close();
    w.close();
    dir.close();
  }

  public void testVectorAcrossMultipleBatches() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(new TieredMergePolicy()));

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    float[][] firstBatch = {{1f, 1f}, {2f, 2f}};
    float[][] secondBatch = {{3f, 3f}, {4f, 4f}, {5f, 5f}};
    w.addBatch(simpleBatch(2, new ArrayDenseFloatVectorColumn("v", vectorType, firstBatch)));
    w.addBatch(simpleBatch(3, new ArrayDenseFloatVectorColumn("v", vectorType, secondBatch)));

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = values.iterator();
    List<float[]> collected = new ArrayList<>();
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      collected.add(values.vectorValue(it.index()).clone());
    }
    // Merge order is not guaranteed when batches flush to separate segments,
    // so compare as a sorted set rather than by doc-ID position.
    Comparator<float[]> byFirst = Comparator.comparingDouble(v -> v[0]);
    collected.sort(byFirst);
    float[][] expected = {
      firstBatch[0], firstBatch[1], secondBatch[0], secondBatch[1], secondBatch[2]
    };
    Arrays.sort(expected, byFirst);
    assertEquals(expected.length, collected.size());
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], collected.get(i), 0f);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testEmptyVectorColumnRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // A field type alone is not enough — every batch must have at least one column with data,
    // and a vector-only column with no values is the equivalent of "no documents have this
    // vector".
    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] anchorIds = {0, 1};
    long[] anchorVals = {0L, 1L};
    w.addBatch(
        simpleBatch(
            2,
            new ArrayFloatVectorColumn("v", vectorType, new int[0], new float[0][]),
            new ArrayLongColumn("anchor", NumericDocValuesField.TYPE, anchorIds, anchorVals)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    if (values != null) {
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.iterator().nextDoc());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testParentFieldWithVectorBatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setParentField("_parent");
    IndexWriter w = new IndexWriter(dir, config);

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    float[][] vectors = {{1f, 0f}, {0f, 1f}, {1f, 1f}};
    w.addBatch(simpleBatch(3, new ArrayDenseFloatVectorColumn("v", vectorType, vectors)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues parentDv = leaf.getNumericDocValues("_parent");
    assertNotNull(parentDv);
    for (int i = 0; i < 3; i++) {
      assertEquals(i, parentDv.nextDoc());
    }
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int i = 0; i < 3; i++) {
      assertEquals(i, it.nextDoc());
      assertArrayEquals(vectors[i], values.vectorValue(it.index()), 0f);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testLongTupleCursorThrowMidBatchRecovers() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // 5-doc batch; LongTupleCursor#nextDoc throws at index 2 (during column pass).
    int[] docIds = {0, 1, 2, 3, 4};
    long[] values = {10, 20, 30, 40, 50};
    IllegalStateException expected = new IllegalStateException("boom-long");
    RuntimeException actual =
        expectThrows(
            RuntimeException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        5,
                        new ThrowingLongColumn(
                            "v", NumericDocValuesField.TYPE, docIds, values, 2, expected))));
    assertSame(expected, actual);
    // Force the failed batch's DWPT to flush so the cleanup-induced fully-deleted
    // segment is pruned before the recovery starts a new one.
    w.commit();

    // Recovery: another batch and a doc-by-doc add must succeed.
    int[] recoveryIds = {0, 1};
    long[] recoveryVals = {100, 200};
    w.addBatch(
        simpleBatch(
            2, new ArrayLongColumn("v", NumericDocValuesField.TYPE, recoveryIds, recoveryVals)));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("v", 999));
    w.addDocument(doc);

    // The failed batch's 5 reserved doc-ids are all marked deleted, the resulting
    // fully-deleted segment is pruned at flush time, so only the recovery work
    // remains visible.
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(2 + 1, leaf.maxDoc());
    assertEquals(2 + 1, leaf.numDocs());

    NumericDocValues dv = leaf.getNumericDocValues("v");
    assertEquals(0, dv.nextDoc());
    assertEquals(100, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(200, dv.longValue());
    assertEquals(2, dv.nextDoc());
    assertEquals(999, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testLongValuesCursorThrowMidBatchRecovers() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // 6-doc dense batch; LongValuesCursor#fillDocValues throws partway (after 3 fills).
    long[] values = {1, 2, 3, 4, 5, 6};
    IllegalStateException expected = new IllegalStateException("boom-dense");
    RuntimeException actual =
        expectThrows(
            RuntimeException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        6,
                        new ThrowingDenseLongColumn(
                            "v", NumericDocValuesField.TYPE, values, 3, expected))));
    assertSame(expected, actual);
    // Force the failed batch's DWPT to flush so the cleanup-induced fully-deleted
    // segment is pruned before the recovery starts a new one.
    w.commit();

    // Recovery via a single addDocument followed by another addBatch.
    Document doc = new Document();
    doc.add(new NumericDocValuesField("v", 777));
    w.addDocument(doc);
    int[] recoveryIds = {0, 1};
    long[] recoveryVals = {111, 222};
    w.addBatch(
        simpleBatch(
            2, new ArrayLongColumn("v", NumericDocValuesField.TYPE, recoveryIds, recoveryVals)));

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(1 + 2, leaf.maxDoc());
    assertEquals(1 + 2, leaf.numDocs());

    NumericDocValues dv = leaf.getNumericDocValues("v");
    assertEquals(0, dv.nextDoc());
    assertEquals(777, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(111, dv.longValue());
    assertEquals(2, dv.nextDoc());
    assertEquals(222, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryTupleCursorThrowMidBatchRecovers() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    int[] docIds = {0, 1, 2, 3};
    BytesRef[] values = {
      newBytesRef("a"), newBytesRef("b"), newBytesRef("c"), newBytesRef("d"),
    };
    IllegalStateException expected = new IllegalStateException("boom-binary");
    RuntimeException actual =
        expectThrows(
            RuntimeException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        4,
                        new ThrowingBinaryColumn(
                            "b", BinaryDocValuesField.TYPE, docIds, values, 1, expected))));
    assertSame(expected, actual);
    // Force the failed batch's DWPT to flush so the cleanup-induced fully-deleted
    // segment is pruned before the recovery starts a new one.
    w.commit();

    int[] recoveryIds = {0};
    BytesRef[] recoveryVals = {newBytesRef("ok")};
    w.addBatch(
        simpleBatch(
            1, new ArrayBinaryColumn("b", BinaryDocValuesField.TYPE, recoveryIds, recoveryVals)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(1, leaf.maxDoc());
    assertEquals(1, leaf.numDocs());

    BinaryDocValues dv = leaf.getBinaryDocValues("b");
    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef("ok"), dv.binaryValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testVectorTupleCursorThrowMidBatchRecovers() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {0, 1, 2};
    float[][] values = {{1f, 1f}, {2f, 2f}, {3f, 3f}};
    IllegalStateException expected = new IllegalStateException("boom-vector");
    RuntimeException actual =
        expectThrows(
            RuntimeException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        3,
                        new ThrowingFloatVectorColumn(
                            "v", vectorType, docIds, values, 1, expected))));
    assertSame(expected, actual);
    // Force the failed batch's DWPT to flush so the cleanup-induced fully-deleted
    // segment is pruned before the recovery starts a new one.
    w.commit();

    // Recover with a clean vector batch.
    float[][] recovery = {{4f, 4f}};
    w.addBatch(simpleBatch(1, new ArrayDenseFloatVectorColumn("v", vectorType, recovery)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(1, leaf.maxDoc());
    assertEquals(1, leaf.numDocs());

    FloatVectorValues vv = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = vv.iterator();
    assertEquals(0, it.nextDoc());
    assertArrayEquals(recovery[0], vv.vectorValue(it.index()), 0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testEmptyBatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    w.addBatch(simpleBatch(0));

    // Writer still functions for follow-up work.
    w.addBatch(
        simpleBatch(
            1,
            new ArrayLongColumn("v", NumericDocValuesField.TYPE, new int[] {0}, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(1, leaf.maxDoc());
    NumericDocValues dv = leaf.getNumericDocValues("v");
    assertEquals(0, dv.nextDoc());
    assertEquals(42, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testMultipleBatchesIntoOneSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1, 2}, new long[] {10, 20, 30})));
    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {40, 50})));

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(5, leaf.maxDoc());
    NumericDocValues dv = leaf.getNumericDocValues("v");
    long[] expected = {10, 20, 30, 40, 50};
    for (int i = 0; i < expected.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(expected[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Stored fields appearing in a later batch are correctly aligned to their docIDs even when
   * earlier batches in the same segment had no stored columns.
   */
  public void testStoredFieldsAppearAfterStoredFreeBatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // Batch A: 3 docs, indexed-only string column (no stored).
    BytesRef[] ids = {newBytesRef("a"), newBytesRef("b"), newBytesRef("c")};
    w.addBatch(
        simpleBatch(
            3, new ArrayBinaryColumn("id", StringField.TYPE_NOT_STORED, new int[] {0, 1, 2}, ids)));

    // Batch B: 3 docs, one stored numeric column.
    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();
    long[] storedValues = {100, 200, 300};
    w.addBatch(
        simpleBatch(
            3, new ArrayLongColumn("v", storedNumericType, new int[] {0, 1, 2}, storedValues)));

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(6, leaf.maxDoc());

    StoredFields storedFields = leaf.storedFields();
    // Docs 0..2 (batch A) had no stored column — empty frames retroactively filled in.
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertNull("doc " + i + " should have no stored 'v' field", doc.getField("v"));
    }
    // Docs 3..5 (batch B) have the stored numeric value.
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(3 + i);
      assertEquals(storedValues[i], doc.getField("v").numericValue().longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  /**
   * When no batch in the segment has a stored column, the segment opens cleanly and stored-field
   * reads return empty docs.
   */
  public void testNoStoredFieldsAcrossSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(new TieredMergePolicy()));

    // Two indexed-only batches (no stored column anywhere).
    BytesRef[] idsA = {newBytesRef("a"), newBytesRef("b")};
    w.addBatch(
        simpleBatch(
            2, new ArrayBinaryColumn("id", StringField.TYPE_NOT_STORED, new int[] {0, 1}, idsA)));
    BytesRef[] idsB = {newBytesRef("c"), newBytesRef("d"), newBytesRef("e")};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayBinaryColumn("id", StringField.TYPE_NOT_STORED, new int[] {0, 1, 2}, idsB)));

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(5, leaf.maxDoc());

    // Indexed values are still searchable.
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(1, s.count(new TermQuery(new Term("id", "a"))));
    assertEquals(1, s.count(new TermQuery(new Term("id", "e"))));

    // Stored reads return empty docs for every batch-local doc-id.
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < leaf.maxDoc(); i++) {
      Document doc = storedFields.document(i);
      assertTrue("doc " + i + " should have no stored fields", doc.getFields().isEmpty());
    }

    r.close();
    w.close();
    dir.close();
  }

  /** Interleaving addBatch and addDocument keeps doc-ids contiguous and values consistent. */
  public void testAddBatchInterleavedWithAddDocument() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // batch (3) → doc → batch (2) → doc, expecting docs 0..6 in order.
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1, 2}, new long[] {10, 20, 30})));
    Document d3 = new Document();
    d3.add(new NumericDocValuesField("v", 40));
    w.addDocument(d3);
    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {50, 60})));
    Document d6 = new Document();
    d6.add(new NumericDocValuesField("v", 70));
    w.addDocument(d6);

    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(7, leaf.maxDoc());
    NumericDocValues dv = leaf.getNumericDocValues("v");
    long[] expected = {10, 20, 30, 40, 50, 60, 70};
    for (int i = 0; i < expected.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(expected[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testAddBatchWithDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType idType = new FieldType(StringField.TYPE_NOT_STORED);
    idType.freeze();

    BytesRef[] ids = {
      newBytesRef("a"), newBytesRef("b"), newBytesRef("c"), newBytesRef("d"),
    };
    int[] docIds = {0, 1, 2, 3};
    w.addBatch(simpleBatch(4, new ArrayBinaryColumn("id", idType, docIds, ids)));

    w.deleteDocuments(new Term("id", "b"));
    w.deleteDocuments(new TermQuery(new Term("id", "d")));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(0, s.count(new TermQuery(new Term("id", "b"))));
    assertEquals(0, s.count(new TermQuery(new Term("id", "d"))));
    assertEquals(1, s.count(new TermQuery(new Term("id", "a"))));
    assertEquals(1, s.count(new TermQuery(new Term("id", "c"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testIndexSortWithAddBatch() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);

    // Out-of-order: input is 30, 10, 20 — segment should reorder to 10, 20, 30.
    long[] values = {30, 10, 20};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("sort", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());

    NumericDocValues dv = leaf.getNumericDocValues("sort");
    long[] sorted = {10, 20, 30};
    for (int i = 0; i < sorted.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(sorted[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testForceMergeMixedOriginSegments() throws IOException {
    Directory dir = newDirectory();
    // Pin a non-mock merge policy: MockRandomMergePolicy.reorder may reverse doc-ids during
    // merge, which is legitimate behavior but defeats the strict per-doc-id assertions below.
    // Also disable auto-flush so the 3 addDocument calls land in a single segment; otherwise a
    // randomized tiny maxBufferedDocs splits them into multiple small segments and TMP's
    // size-sorted forceMerge can interleave them with the addBatch segment.
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, iwc);

    // Segment A via addDocument: values 10, 20, 30.
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("v", 10L * (i + 1)));
      w.addDocument(doc);
    }
    w.commit();

    // Segment B via addBatch: values 40, 50, 60.
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1, 2}, new long[] {40, 50, 60})));
    w.commit();

    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(6, leaf.maxDoc());

    NumericDocValues dv = leaf.getNumericDocValues("v");
    long[] expected = {10, 20, 30, 40, 50, 60};
    for (int i = 0; i < expected.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(expected[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  /**
   * Multiple threads calling addBatch concurrently on the same writer produce a consistent index
   * with every doc reachable by its unique id and the expected total doc count.
   */
  public void testConcurrentAddBatch() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    final int numThreads = 4;
    final int batchesPerThread = 5;
    final int docsPerBatch = 10;
    final CountDownLatch start = new CountDownLatch(1);
    Thread[] threads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      final int threadIdx = t;
      threads[t] =
          new Thread(
              () -> {
                try {
                  start.await();
                  int[] docIds = new int[docsPerBatch];
                  for (int i = 0; i < docsPerBatch; i++) {
                    docIds[i] = i;
                  }
                  for (int b = 0; b < batchesPerThread; b++) {
                    BytesRef[] ids = new BytesRef[docsPerBatch];
                    for (int i = 0; i < docsPerBatch; i++) {
                      ids[i] = newBytesRef("t" + threadIdx + "_b" + b + "_d" + i);
                    }
                    w.addBatch(
                        simpleBatch(
                            docsPerBatch,
                            new ArrayBinaryColumn("id", StringField.TYPE_NOT_STORED, docIds, ids)));
                  }
                } catch (Exception e) {
                  throw new AssertionError(e);
                }
              });
      threads[t].start();
    }
    start.countDown();
    for (Thread th : threads) {
      th.join();
    }

    int totalDocs = numThreads * batchesPerThread * docsPerBatch;
    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(totalDocs, r.numDocs());
    IndexSearcher s = new IndexSearcher(r);
    for (int t = 0; t < numThreads; t++) {
      for (int b = 0; b < batchesPerThread; b++) {
        for (int i = 0; i < docsPerBatch; i++) {
          Term term = new Term("id", "t" + t + "_b" + b + "_d" + i);
          assertEquals("missing " + term, 1, s.count(new TermQuery(term)));
        }
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  /** Per-doc payload covering the union of column types we stress in the parity test. */
  private static final class ParityDoc {
    Long num; // numeric DV
    long[] snum; // multi-valued sorted-numeric DV
    BytesRef binStored; // binary DV + stored
    BytesRef[] set; // multi-valued sorted-set DV
    Long lpoint; // single 1-D long point
    BytesRef tag; // indexed StringField for term queries
    float[] vec; // float vector
  }

  private static ParityDoc randomParityDoc(Random r) {
    ParityDoc d = new ParityDoc();
    if (r.nextFloat() < 0.7f) d.num = (long) r.nextInt(1000);
    if (r.nextFloat() < 0.5f) {
      int n = TestUtil.nextInt(r, 1, 3);
      d.snum = new long[n];
      for (int i = 0; i < n; i++) d.snum[i] = (long) r.nextInt(1000);
      java.util.Arrays.sort(d.snum);
    }
    if (r.nextFloat() < 0.6f) {
      d.binStored = newBytesRef("b" + r.nextInt(50));
    }
    if (r.nextFloat() < 0.5f) {
      int n = TestUtil.nextInt(r, 1, 3);
      java.util.TreeSet<String> uniq = new java.util.TreeSet<>();
      while (uniq.size() < n) uniq.add("s" + r.nextInt(20));
      d.set = uniq.stream().map(LuceneTestCase::newBytesRef).toArray(BytesRef[]::new);
    }
    if (r.nextFloat() < 0.6f) d.lpoint = (long) r.nextInt(10000);
    if (r.nextFloat() < 0.7f) d.tag = newBytesRef("tag" + r.nextInt(15));
    if (r.nextFloat() < 0.5f) {
      d.vec = new float[] {r.nextFloat(), r.nextFloat()};
    }
    return d;
  }

  private static void buildDocByDocIndex(Directory dir, ParityDoc[] docs) throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (ParityDoc d : docs) {
        Document doc = new Document();
        if (d.num != null) doc.add(new NumericDocValuesField("num", d.num));
        if (d.snum != null) {
          for (long v : d.snum) doc.add(new SortedNumericDocValuesField("snum", v));
        }
        if (d.binStored != null) {
          doc.add(new BinaryDocValuesField("bin_stored", d.binStored));
          doc.add(new StoredField("bin_stored", d.binStored));
        }
        if (d.set != null) {
          for (BytesRef v : d.set) doc.add(new SortedSetDocValuesField("set", v));
        }
        if (d.lpoint != null) doc.add(new LongPoint("lpoint", d.lpoint));
        if (d.tag != null) doc.add(new StringField("tag", d.tag, Field.Store.NO));
        if (d.vec != null) doc.add(new KnnFloatVectorField("vec", d.vec));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  /**
   * Builds an index by partitioning the docs into random-sized batches and feeding each batch
   * through addBatch. Every per-batch column carries only the batch-local docs that actually have a
   * value for that field; columns absent from a batch (no doc has a value) are simply not included.
   */
  private static void buildBatchIndex(Directory dir, ParityDoc[] docs, Random r)
      throws IOException {
    // Partition into 1..min(4, numDocs) contiguous batches.
    int numBatches = TestUtil.nextInt(r, 1, Math.min(4, Math.max(1, docs.length)));
    int[] starts = new int[numBatches + 1];
    starts[0] = 0;
    starts[numBatches] = docs.length;
    for (int i = 1; i < numBatches; i++) {
      starts[i] = TestUtil.nextInt(r, starts[i - 1] + 1, docs.length - (numBatches - i));
    }

    // Field types used on the batch path.
    FieldType binStoredType = new FieldType();
    binStoredType.setDocValuesType(DocValuesType.BINARY);
    binStoredType.setStored(true);
    binStoredType.freeze();
    FieldType lpointType = new FieldType();
    lpointType.setDimensions(1, Long.BYTES);
    lpointType.freeze();
    FieldType tagType = StringField.TYPE_NOT_STORED;
    FieldType vecType = new FieldType();
    vecType.setVectorAttributes(2, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN);
    vecType.freeze();

    IndexWriterConfig iwc =
        new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int bi = 0; bi < numBatches; bi++) {
        int from = starts[bi];
        int toExcl = starts[bi + 1];
        int batchSize = toExcl - from;
        List<Column> cols = new ArrayList<>();

        // num: single-valued NUMERIC
        addLongTupleColumn(cols, "num", NumericDocValuesField.TYPE, docs, from, toExcl, d -> d.num);

        // snum: multi-valued SORTED_NUMERIC
        addLongMultiColumn(
            cols, "snum", SortedNumericDocValuesField.TYPE, docs, from, toExcl, d -> d.snum);

        // bin_stored: BINARY DV + stored
        addBinaryTupleColumn(
            cols, "bin_stored", binStoredType, docs, from, toExcl, d -> d.binStored);

        // set: multi-valued SORTED_SET
        addBinaryMultiColumn(
            cols, "set", SortedSetDocValuesField.TYPE, docs, from, toExcl, d -> d.set);

        // lpoint: 1-D long point
        addLongTupleColumn(
            cols,
            "lpoint",
            lpointType,
            LongColumn.NumericKind.LONG,
            docs,
            from,
            toExcl,
            d -> d.lpoint);

        // tag: indexed StringField
        addBinaryTupleColumn(cols, "tag", tagType, docs, from, toExcl, d -> d.tag);

        // vec: float vectors (sparse)
        addFloatVectorColumn(cols, "vec", vecType, docs, from, toExcl);

        w.addBatch(
            new org.apache.lucene.document.column.ColumnBatch() {
              @Override
              public int numDocs() {
                return batchSize;
              }

              @Override
              public Iterable<Column> columns() {
                return cols;
              }
            });
      }
      w.forceMerge(1);
    }
  }

  private interface DocFn<T> {
    T apply(ParityDoc d);
  }

  private static void addLongTupleColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      ParityDoc[] docs,
      int from,
      int toExcl,
      DocFn<Long> getter) {
    addLongTupleColumn(cols, name, type, LongColumn.NumericKind.LONG, docs, from, toExcl, getter);
  }

  private static void addLongTupleColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      LongColumn.NumericKind kind,
      ParityDoc[] docs,
      int from,
      int toExcl,
      DocFn<Long> getter) {
    int n = 0;
    for (int i = from; i < toExcl; i++) if (getter.apply(docs[i]) != null) n++;
    if (n == 0) return;
    int[] docIds = new int[n];
    long[] values = new long[n];
    int p = 0;
    for (int i = from; i < toExcl; i++) {
      Long v = getter.apply(docs[i]);
      if (v != null) {
        docIds[p] = i - from;
        values[p] = v;
        p++;
      }
    }
    cols.add(new ArrayLongColumn(name, type, kind, docIds, values));
  }

  private static void addLongMultiColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      ParityDoc[] docs,
      int from,
      int toExcl,
      DocFn<long[]> getter) {
    int n = 0;
    for (int i = from; i < toExcl; i++) {
      long[] v = getter.apply(docs[i]);
      if (v != null) n += v.length;
    }
    if (n == 0) return;
    int[] docIds = new int[n];
    long[] values = new long[n];
    int p = 0;
    for (int i = from; i < toExcl; i++) {
      long[] v = getter.apply(docs[i]);
      if (v != null) {
        for (long x : v) {
          docIds[p] = i - from;
          values[p] = x;
          p++;
        }
      }
    }
    cols.add(new ArrayLongColumn(name, type, docIds, values));
  }

  private static void addBinaryTupleColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      ParityDoc[] docs,
      int from,
      int toExcl,
      DocFn<BytesRef> getter) {
    int n = 0;
    for (int i = from; i < toExcl; i++) if (getter.apply(docs[i]) != null) n++;
    if (n == 0) return;
    int[] docIds = new int[n];
    BytesRef[] values = new BytesRef[n];
    int p = 0;
    for (int i = from; i < toExcl; i++) {
      BytesRef v = getter.apply(docs[i]);
      if (v != null) {
        docIds[p] = i - from;
        values[p] = v;
        p++;
      }
    }
    cols.add(new ArrayBinaryColumn(name, type, docIds, values));
  }

  private static void addBinaryMultiColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      ParityDoc[] docs,
      int from,
      int toExcl,
      DocFn<BytesRef[]> getter) {
    int n = 0;
    for (int i = from; i < toExcl; i++) {
      BytesRef[] v = getter.apply(docs[i]);
      if (v != null) n += v.length;
    }
    if (n == 0) return;
    int[] docIds = new int[n];
    BytesRef[] values = new BytesRef[n];
    int p = 0;
    for (int i = from; i < toExcl; i++) {
      BytesRef[] v = getter.apply(docs[i]);
      if (v != null) {
        for (BytesRef x : v) {
          docIds[p] = i - from;
          values[p] = x;
          p++;
        }
      }
    }
    cols.add(new ArrayBinaryColumn(name, type, docIds, values));
  }

  private static void addFloatVectorColumn(
      List<Column> cols,
      String name,
      IndexableFieldType type,
      ParityDoc[] docs,
      int from,
      int toExcl) {
    int n = 0;
    for (int i = from; i < toExcl; i++) if (docs[i].vec != null) n++;
    if (n == 0) return;
    int[] docIds = new int[n];
    float[][] values = new float[n][];
    int p = 0;
    for (int i = from; i < toExcl; i++) {
      if (docs[i].vec != null) {
        docIds[p] = i - from;
        values[p] = docs[i].vec;
        p++;
      }
    }
    cols.add(new ArrayFloatVectorColumn(name, type, docIds, values));
  }

  public void testRandomizedParityVsDocByDoc() throws IOException {
    Random r = random();
    int numDocs = TestUtil.nextInt(r, 30, 80);
    ParityDoc[] docs = new ParityDoc[numDocs];
    for (int i = 0; i < numDocs; i++) {
      docs[i] = randomParityDoc(r);
    }

    Directory batchDir = newDirectory();
    Directory docDir = newDirectory();
    buildBatchIndex(batchDir, docs, r);
    buildDocByDocIndex(docDir, docs);

    try (DirectoryReader bR = DirectoryReader.open(batchDir);
        DirectoryReader dR = DirectoryReader.open(docDir)) {
      LeafReader bL = getOnlyLeafReader(bR);
      LeafReader dL = getOnlyLeafReader(dR);

      assertEquals("maxDoc", dL.maxDoc(), bL.maxDoc());
      assertEquals("numDocs", dL.numDocs(), bL.numDocs());
      assertEquals(numDocs, bL.maxDoc());

      // Numeric DV
      assertNumericDocValuesEqual(dL.getNumericDocValues("num"), bL.getNumericDocValues("num"));
      // Sorted Numeric DV
      assertSortedNumericDocValuesEqual(
          dL.getSortedNumericDocValues("snum"), bL.getSortedNumericDocValues("snum"));
      // Binary DV (paired with stored)
      assertBinaryDocValuesEqual(
          dL.getBinaryDocValues("bin_stored"), bL.getBinaryDocValues("bin_stored"));
      // Sorted Set DV
      assertSortedSetDocValuesEqual(
          dL.getSortedSetDocValues("set"), bL.getSortedSetDocValues("set"));

      // Stored fields per doc
      StoredFields bSF = bL.storedFields();
      StoredFields dSF = dL.storedFields();
      for (int i = 0; i < numDocs; i++) {
        BytesRef expected = docs[i].binStored;
        IndexableField dF = dSF.document(i).getField("bin_stored");
        IndexableField bF = bSF.document(i).getField("bin_stored");
        if (expected == null) {
          assertNull("doc " + i + " bin_stored doc-path", dF);
          assertNull("doc " + i + " bin_stored batch-path", bF);
        } else {
          assertEquals(expected, dF.binaryValue());
          assertEquals(expected, bF.binaryValue());
        }
      }

      // Point query parity
      IndexSearcher dS = new IndexSearcher(dR);
      IndexSearcher bS = new IndexSearcher(bR);
      for (long pq : new long[] {0L, 100L, 5000L, 9999L}) {
        assertEquals(
            "lpoint exact " + pq,
            dS.count(LongPoint.newExactQuery("lpoint", pq)),
            bS.count(LongPoint.newExactQuery("lpoint", pq)));
      }
      assertEquals(
          "lpoint range",
          dS.count(LongPoint.newRangeQuery("lpoint", 0L, 9999L)),
          bS.count(LongPoint.newRangeQuery("lpoint", 0L, 9999L)));

      // Term query parity for the inverted "tag" field.
      for (int i = 0; i < 15; i++) {
        Term t = new Term("tag", "tag" + i);
        assertEquals("tag " + t, dS.count(new TermQuery(t)), bS.count(new TermQuery(t)));
      }

      // Vector parity per doc (codec may reorder storage; iterate by doc-id).
      FloatVectorValues dV = dL.getFloatVectorValues("vec");
      FloatVectorValues bV = bL.getFloatVectorValues("vec");
      assertVectorsEqual(dV, bV, docs);
    }
    batchDir.close();
    docDir.close();
  }

  private static void assertNumericDocValuesEqual(NumericDocValues a, NumericDocValues b)
      throws IOException {
    if (a == null && b == null) return;
    assertNotNull(a);
    assertNotNull(b);
    int da, db;
    while ((da = a.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      db = b.nextDoc();
      assertEquals(da, db);
      assertEquals("num docID " + da, a.longValue(), b.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, b.nextDoc());
  }

  private static void assertSortedNumericDocValuesEqual(
      SortedNumericDocValues a, SortedNumericDocValues b) throws IOException {
    if (a == null && b == null) return;
    assertNotNull(a);
    assertNotNull(b);
    int da, db;
    while ((da = a.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      db = b.nextDoc();
      assertEquals(da, db);
      int ca = a.docValueCount();
      assertEquals("snum count docID " + da, ca, b.docValueCount());
      for (int i = 0; i < ca; i++) {
        assertEquals("snum value docID " + da, a.nextValue(), b.nextValue());
      }
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, b.nextDoc());
  }

  private static void assertBinaryDocValuesEqual(BinaryDocValues a, BinaryDocValues b)
      throws IOException {
    if (a == null && b == null) return;
    assertNotNull(a);
    assertNotNull(b);
    int da, db;
    while ((da = a.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      db = b.nextDoc();
      assertEquals(da, db);
      assertEquals("bin docID " + da, a.binaryValue(), b.binaryValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, b.nextDoc());
  }

  private static void assertSortedSetDocValuesEqual(SortedSetDocValues a, SortedSetDocValues b)
      throws IOException {
    if (a == null && b == null) return;
    assertNotNull(a);
    assertNotNull(b);
    int da, db;
    while ((da = a.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      db = b.nextDoc();
      assertEquals(da, db);
      int ca = a.docValueCount();
      assertEquals("set count docID " + da, ca, b.docValueCount());
      for (int i = 0; i < ca; i++) {
        assertEquals("set value docID " + da, a.lookupOrd(a.nextOrd()), b.lookupOrd(b.nextOrd()));
      }
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, b.nextDoc());
  }

  private static void assertVectorsEqual(
      FloatVectorValues dV, FloatVectorValues bV, ParityDoc[] docs) throws IOException {
    if (dV == null && bV == null) return;
    assertNotNull(dV);
    assertNotNull(bV);
    KnnVectorValues.DocIndexIterator dIt = dV.iterator();
    KnnVectorValues.DocIndexIterator bIt = bV.iterator();
    int nDoc;
    while ((nDoc = dIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(nDoc, bIt.nextDoc());
      float[] expected = docs[nDoc].vec;
      assertNotNull("expected vector at doc " + nDoc, expected);
      assertArrayEquals(expected, dV.vectorValue(dIt.index()), 0f);
      assertArrayEquals(expected, bV.vectorValue(bIt.index()), 0f);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, bIt.nextDoc());
  }

  public void testDuplicateColumnNameRejected() throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      int[] ids = {0, 1};
      long[] vals = {1L, 2L};
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  w.addBatch(
                      simpleBatch(
                          2,
                          new ArrayLongColumn("field", NumericDocValuesField.TYPE, ids, vals),
                          new ArrayLongColumn("field", NumericDocValuesField.TYPE, ids, vals))));
      assertTrue(e.getMessage(), e.getMessage().contains("field"));

      // Writer still usable after validation failure.
      w.addBatch(
          simpleBatch(
              1,
              new ArrayLongColumn("v", NumericDocValuesField.TYPE, new int[] {0}, new long[] {7})));
      w.forceMerge(1);
      DirectoryReader r = DirectoryReader.open(w);
      // After forceMerge, fully-deleted segments are pruned; only the recovery doc remains.
      assertEquals(1, r.numDocs());
      LeafReader leaf = getOnlyLeafReader(r);
      NumericDocValues dv = leaf.getNumericDocValues("v");
      assertNotNull(dv);
      assertTrue(dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(7, dv.longValue());
      r.close();
    }
    dir.close();
  }

  public void testSameFieldNameAcrossBatchesAllowed() throws IOException {
    Directory dir = newDirectory();
    // Pin a stable IWC so randomized small buffers / merge policies cannot reorder
    // this test asserts doc-id ordering.
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      w.addBatch(
          simpleBatch(
              1,
              new ArrayLongColumn(
                  "f", NumericDocValuesField.TYPE, new int[] {0}, new long[] {1L})));
      w.addBatch(
          simpleBatch(
              1,
              new ArrayLongColumn(
                  "f", NumericDocValuesField.TYPE, new int[] {0}, new long[] {2L})));
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(r);
        NumericDocValues dv = leaf.getNumericDocValues("f");
        assertNotNull(dv);
        assertEquals(0, dv.nextDoc());
        assertEquals(1L, dv.longValue());
        assertEquals(1, dv.nextDoc());
        assertEquals(2L, dv.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
      }
    }
    dir.close();
  }
}
