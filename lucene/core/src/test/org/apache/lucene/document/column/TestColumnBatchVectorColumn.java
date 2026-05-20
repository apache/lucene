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
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayByteVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseByteVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayDenseFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.byteVectorType;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.floatVectorType;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link VectorColumn} batch indexing. */
public class TestColumnBatchVectorColumn extends LuceneTestCase {

  public void testDenseFloatVectorColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(3, VectorSimilarityFunction.EUCLIDEAN);
    float[][] vectors = {
      {1f, 2f, 3f}, {4f, 5f, 6f}, {7f, 8f, 9f},
    };
    w.addBatch(simpleBatch(3, new ArrayDenseFloatVectorColumn("v", vectorType, vectors)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    assertNotNull(values);
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(i, it.nextDoc());
      assertArrayEquals(vectors[i], values.vectorValue(it.index()), 0f);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseByteVectorColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = byteVectorType(4, VectorSimilarityFunction.EUCLIDEAN);
    byte[][] vectors = {
      {1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12},
    };
    w.addBatch(simpleBatch(3, new ArrayDenseByteVectorColumn("v", vectorType, vectors)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    ByteVectorValues values = leaf.getByteVectorValues("v");
    assertNotNull(values);
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(i, it.nextDoc());
      assertArrayEquals(vectors[i], values.vectorValue(it.index()));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSparseFloatVectorColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {0, 2, 5, 9};
    float[][] vectors = {{1f, 1f}, {2f, 2f}, {3f, 3f}, {4f, 4f}};
    // pair with a sparse long column so the batch has a defined doc count > vector count
    int[] anchorIds = {0, 9};
    long[] anchorVals = {0L, 9L};
    w.addBatch(
        simpleBatch(
            10,
            new ArrayFloatVectorColumn("v", vectorType, docIds, vectors),
            new ArrayLongColumn("anchor", NumericDocValuesField.TYPE, anchorIds, anchorVals)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    assertNotNull(values);
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int i = 0; i < docIds.length; i++) {
      assertEquals(docIds[i], it.nextDoc());
      assertArrayEquals(vectors[i], values.vectorValue(it.index()), 0f);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSparseByteVectorColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = byteVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {1, 4};
    byte[][] vectors = {{1, 2}, {3, 4}};
    int[] anchorIds = {0, 5};
    long[] anchorVals = {0L, 5L};
    w.addBatch(
        simpleBatch(
            6,
            new ArrayByteVectorColumn("v", vectorType, docIds, vectors),
            new ArrayLongColumn("anchor", NumericDocValuesField.TYPE, anchorIds, anchorVals)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    ByteVectorValues values = leaf.getByteVectorValues("v");
    assertNotNull(values);
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int i = 0; i < docIds.length; i++) {
      assertEquals(docIds[i], it.nextDoc());
      assertArrayEquals(vectors[i], values.vectorValue(it.index()));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testFloatVectorEncodingMismatchFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FieldType says FLOAT32 but column carries byte[] vectors.
    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    byte[][] vectors = {{1, 2}, {3, 4}};
    expectThrows(
        ClassCastException.class,
        () -> w.addBatch(simpleBatch(2, new ArrayDenseByteVectorColumn("v", vectorType, vectors))));
    w.rollback();
    dir.close();
  }

  public void testWrongDimensionFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(3, VectorSimilarityFunction.EUCLIDEAN);
    float[][] vectors = {{1f, 2f, 3f}, {4f, 5f}};
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(2, new ArrayDenseFloatVectorColumn("v", vectorType, vectors))));
    assertTrue(e.getMessage(), e.getMessage().contains("expected dimension 3"));
    w.rollback();
    dir.close();
  }

  public void testZeroDimensionFieldTypeFails() {
    FieldType bad = new FieldType();
    // No vector attributes set -> vectorDimension() == 0
    bad.setDocValuesType(DocValuesType.NUMERIC);
    bad.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> new ArrayDenseFloatVectorColumn("v", bad, new float[][] {{1f, 2f}}));
    assertTrue(e.getMessage(), e.getMessage().contains("vectorDimension() > 0"));
  }

  public void testVectorWithDocValuesRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType bad = new FieldType();
    bad.setVectorAttributes(2, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN);
    bad.setDocValuesType(DocValuesType.NUMERIC);
    bad.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        1, new ArrayDenseFloatVectorColumn("v", bad, new float[][] {{1f, 2f}}))));
    assertTrue(e.getMessage(), e.getMessage().contains("must be vector-only"));
    w.rollback();
    dir.close();
  }

  public void testVectorWithStoredRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType bad = new FieldType();
    bad.setVectorAttributes(2, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN);
    bad.setStored(true);
    bad.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        1, new ArrayDenseFloatVectorColumn("v", bad, new float[][] {{1f, 2f}}))));
    assertTrue(e.getMessage(), e.getMessage().contains("must be vector-only"));
    w.rollback();
    dir.close();
  }

  public void testVectorWithIndexOptionsRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType bad = new FieldType();
    bad.setVectorAttributes(2, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN);
    bad.setIndexOptions(IndexOptions.DOCS);
    bad.setTokenized(false);
    bad.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        1, new ArrayDenseFloatVectorColumn("v", bad, new float[][] {{1f, 2f}}))));
    assertTrue(e.getMessage(), e.getMessage().contains("must be vector-only"));
    w.rollback();
    dir.close();
  }

  public void testVectorWithPointsRejected() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType bad = new FieldType();
    bad.setVectorAttributes(2, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN);
    bad.setDimensions(1, Integer.BYTES);
    bad.freeze();
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(
                        1, new ArrayDenseFloatVectorColumn("v", bad, new float[][] {{1f, 2f}}))));
    assertTrue(e.getMessage(), e.getMessage().contains("must be vector-only"));
    w.rollback();
    dir.close();
  }

  public void testDuplicateDocIDFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {0, 0};
    float[][] vectors = {{1f, 2f}, {3f, 4f}};
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(2, new ArrayFloatVectorColumn("v", vectorType, docIds, vectors))));
    assertTrue(e.getMessage(), e.getMessage().contains("strictly increasing"));
    w.rollback();
    dir.close();
  }

  public void testDecreasingDocIDFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {3, 1};
    float[][] vectors = {{1f, 2f}, {3f, 4f}};
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(4, new ArrayFloatVectorColumn("v", vectorType, docIds, vectors))));
    assertTrue(e.getMessage(), e.getMessage().contains("strictly increasing"));
    w.rollback();
    dir.close();
  }

  public void testVectorOutOfRangeDocIDFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    int[] docIds = {0, 5};
    float[][] vectors = {{1f, 2f}, {3f, 4f}};
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(3, new ArrayFloatVectorColumn("v", vectorType, docIds, vectors))));
    assertTrue(e.getMessage(), e.getMessage().contains("out of range"));
    w.rollback();
    dir.close();
  }

  public void testDenseVectorColumnTooFewValuesFails() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    // 2 values declared DENSE but the batch has 3 docs.
    float[][] vectors = {{1f, 2f}, {3f, 4f}};
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                w.addBatch(
                    simpleBatch(3, new ArrayDenseFloatVectorColumn("v", vectorType, vectors))));
    assertTrue(e.getMessage(), e.getMessage().contains("Dense column"));
    w.rollback();
    dir.close();
  }

  public void testVectorColumnSchemaConsistencyAcrossBatches() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType float32Type = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    w.addBatch(
        simpleBatch(
            1, new ArrayDenseFloatVectorColumn("v", float32Type, new float[][] {{1f, 2f}})));

    FieldType byteType = byteVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1, new ArrayDenseByteVectorColumn("v", byteType, new byte[][] {{1, 2}}))));
    w.rollback();
    dir.close();
  }

  /**
   * Vectors added via addBatch can be deleted via {@code deleteDocuments(Term)} when an indexed
   * column is added alongside, and live-doc filtering hides the deleted vectors from search.
   */
  public void testVectorWithDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    FieldType idType = new FieldType(StringField.TYPE_NOT_STORED);
    idType.freeze();

    float[][] vectors = {{1f, 1f}, {2f, 2f}, {3f, 3f}, {4f, 4f}};
    BytesRef[] ids = {
      newBytesRef("a"), newBytesRef("b"), newBytesRef("c"), newBytesRef("d"),
    };
    int[] docIds = {0, 1, 2, 3};
    w.addBatch(
        simpleBatch(
            4,
            new ArrayDenseFloatVectorColumn("v", vectorType, vectors),
            new ArrayBinaryColumn("id", idType, docIds, ids)));

    w.deleteDocuments(new Term("id", "b"));
    w.deleteDocuments(new Term("id", "c"));
    // Commit to materialize deletes into the segment before opening the reader.
    w.commit();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(2, leaf.numDocs());

    // Iterate the vector values. Either the deletes have been physically merged out (so the
    // iterator only sees 2 docs) or the segment retains the deleted slots (in which case the
    // live-docs bitset filters them). Either way, live count must be 2 and deleted vectors
    // must not appear among the live ones.
    Bits live = leaf.getLiveDocs();
    int liveCount = 0;
    FloatVectorValues vv = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = vv.iterator();
    int doc;
    while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (live == null || live.get(doc)) {
        liveCount++;
      }
    }
    assertEquals(2, liveCount);

    r.close();
    w.close();
    dir.close();
  }

  /** forceMerge(1) of two vector batches yields a single segment containing all vectors. */
  public void testVectorForceMerge() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);

    float[][] firstBatch = {{1f, 1f}, {2f, 2f}};
    w.addBatch(simpleBatch(2, new ArrayDenseFloatVectorColumn("v", vectorType, firstBatch)));
    w.commit();

    float[][] secondBatch = {{3f, 3f}, {4f, 4f}, {5f, 5f}};
    w.addBatch(simpleBatch(3, new ArrayDenseFloatVectorColumn("v", vectorType, secondBatch)));
    w.commit();

    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(5, leaf.maxDoc());

    FloatVectorValues vv = leaf.getFloatVectorValues("v");
    KnnVectorValues.DocIndexIterator it = vv.iterator();
    int count = 0;
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      count++;
    }
    assertEquals(5, count);

    r.close();
    w.close();
    dir.close();
  }
}
