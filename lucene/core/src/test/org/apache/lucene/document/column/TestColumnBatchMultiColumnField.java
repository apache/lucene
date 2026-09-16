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
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayFloatVectorColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.ArrayLongColumn;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.floatVectorType;
import static org.apache.lucene.document.column.ColumnBatchTestUtil.simpleBatch;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Tests for composing multiple columns under a single field name in {@link ColumnBatch#columns()}.
 */
public class TestColumnBatchMultiColumnField extends LuceneTestCase {

  /** Returns a frozen {@link FieldType} for an untokenized inverted-only field. */
  private static FieldType invertedType() {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS);
    type.setTokenized(false);
    type.setOmitNorms(true);
    type.freeze();
    return type;
  }

  /** Returns a frozen {@link FieldType} for a stored-only (non-indexed) field. */
  private static FieldType storedBinaryType() {
    FieldType type = new FieldType();
    type.setStored(true);
    type.freeze();
    return type;
  }

  private IndexWriterConfig stableIwc() {
    IndexWriterConfig iwc =
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new TieredMergePolicy());
    iwc.setRAMBufferSizeMB(16);
    iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    return iwc;
  }

  public void testComposeStoredAndInverted() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    BytesRef[] stored = {newBytesRef("Hello World"), newBytesRef("Goodbye World")};
    // Index the lower-cased tokens so the stored and indexed representations differ.
    BytesRef[] indexed = {newBytesRef("hello"), newBytesRef("goodbye")};

    w.addBatch(
        simpleBatch(
            2,
            new ArrayBinaryColumn("body", storedBinaryType(), new int[] {0, 1}, stored),
            new ArrayBinaryColumn("body", invertedType(), new int[] {0, 1}, indexed)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    // Inverted postings come from the inverted column.
    assertEquals(1, s.count(new TermQuery(new Term("body", "hello"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "goodbye"))));
    // The upper-cased original is not indexed.
    assertEquals(0, s.count(new TermQuery(new Term("body", "Hello"))));

    // Stored values come from the stored column.
    StoredFields storedFields = leaf.storedFields();
    assertEquals(stored[0], storedFields.document(0).getField("body").binaryValue());
    assertEquals(stored[1], storedFields.document(1).getField("body").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeDocValuesAndInverted() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    BytesRef[] tokens = {newBytesRef("alpha"), newBytesRef("beta")};
    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "x", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {42L, 99L}),
            new ArrayBinaryColumn("x", invertedType(), new int[] {0, 1}, tokens)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    // Inverted postings come from the binary column.
    assertEquals(1, s.count(new TermQuery(new Term("x", "alpha"))));
    assertEquals(1, s.count(new TermQuery(new Term("x", "beta"))));

    // Numeric doc values come from the long column.
    NumericDocValues dv = leaf.getNumericDocValues("x");
    assertNotNull(dv);
    assertEquals(0, dv.nextDoc());
    assertEquals(42L, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(99L, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeOrderIndependent() throws IOException {
    BytesRef stored = newBytesRef("Hello World");
    BytesRef indexed = newBytesRef("hello");

    Directory storedFirst = indexComposedBinaryColumns(true, stored, indexed);
    Directory invertedFirst = indexComposedBinaryColumns(false, stored, indexed);

    for (Directory d : new Directory[] {storedFirst, invertedFirst}) {
      DirectoryReader r = DirectoryReader.open(d);
      LeafReader leaf = getOnlyLeafReader(r);
      IndexSearcher s = new IndexSearcher(r);
      assertEquals(1, s.count(new TermQuery(new Term("body", "hello"))));
      assertEquals(0, s.count(new TermQuery(new Term("body", "Hello"))));
      assertEquals(stored, leaf.storedFields().document(0).getField("body").binaryValue());
      r.close();
      d.close();
    }
  }

  private Directory indexComposedBinaryColumns(
      boolean storedFirst, BytesRef stored, BytesRef indexed) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Column storedCol =
        new ArrayBinaryColumn("body", storedBinaryType(), new int[] {0}, new BytesRef[] {stored});
    Column invertedCol =
        new ArrayBinaryColumn("body", invertedType(), new int[] {0}, new BytesRef[] {indexed});
    w.addBatch(
        storedFirst
            ? simpleBatch(1, storedCol, invertedCol)
            : simpleBatch(1, invertedCol, storedCol));
    w.close();
    return dir;
  }

  public void testComposeDocValuesAndPoints() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "n", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {7L, 8L}),
            new ArrayLongColumn(
                "n",
                pointType,
                LongColumn.NumericKind.INT,
                new int[] {0, 1},
                new long[] {7L, 8L})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    // Points come from the point column.
    assertEquals(1, s.count(IntPoint.newExactQuery("n", 7)));
    assertEquals(2, s.count(IntPoint.newRangeQuery("n", 7, 8)));

    // NUMERIC doc values come from the doc-values column.
    NumericDocValues dv = leaf.getNumericDocValues("n");
    assertNotNull(dv);
    assertEquals(0, dv.nextDoc());
    assertEquals(7L, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(8L, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeDocValuesAndVector() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    float[][] vectors = {{1f, 2f}, {3f, 4f}};

    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "v", NumericDocValuesField.TYPE, new int[] {0, 1}, new long[] {11L, 22L}),
            new ArrayFloatVectorColumn("v", vectorType, new int[] {0, 1}, vectors)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("v");
    assertNotNull(dv);
    assertEquals(0, dv.nextDoc());
    assertEquals(11L, dv.longValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(22L, dv.longValue());

    FloatVectorValues fvv = leaf.getFloatVectorValues("v");
    assertNotNull(fvv);
    KnnVectorValues.DocIndexIterator it = fvv.iterator();
    for (int i = 0; i < 2; i++) {
      assertEquals(i, it.nextDoc());
      assertArrayEquals(vectors[i], fvv.vectorValue(it.index()), 0f);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeSparseDisjointDocIds() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    w.addBatch(
        simpleBatch(
            2,
            new ArrayBinaryColumn(
                "body", storedBinaryType(), new int[] {0}, new BytesRef[] {newBytesRef("kept")}),
            new ArrayBinaryColumn(
                "body", invertedType(), new int[] {1}, new BytesRef[] {newBytesRef("token")})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    // The token is indexed on doc 1 only.
    assertEquals(1, s.count(new TermQuery(new Term("body", "token"))));

    StoredFields storedFields = leaf.storedFields();
    // doc 0 carries the stored value; doc 1 has no stored "body".
    assertEquals(newBytesRef("kept"), storedFields.document(0).getField("body").binaryValue());
    assertNull(storedFields.document(1).getField("body"));

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeAcrossBatches() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, stableIwc());

    w.addBatch(
        simpleBatch(
            1,
            new ArrayBinaryColumn(
                "body", storedBinaryType(), new int[] {0}, new BytesRef[] {newBytesRef("Hello")}),
            new ArrayBinaryColumn(
                "body", invertedType(), new int[] {0}, new BytesRef[] {newBytesRef("hello")})));
    w.addBatch(
        simpleBatch(
            1,
            new ArrayBinaryColumn(
                "body", storedBinaryType(), new int[] {0}, new BytesRef[] {newBytesRef("World")}),
            new ArrayBinaryColumn(
                "body", invertedType(), new int[] {0}, new BytesRef[] {newBytesRef("world")})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    assertEquals(1, s.count(new TermQuery(new Term("body", "hello"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "world"))));

    StoredFields storedFields = leaf.storedFields();
    assertEquals(newBytesRef("Hello"), storedFields.document(0).getField("body").binaryValue());
    assertEquals(newBytesRef("World"), storedFields.document(1).getField("body").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testComposeMixedWithAddDocument() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, stableIwc());

    // A field type that is both stored and inverted, matching the merged schema produced by
    // composing storedBinaryType() + invertedType() in a batch.
    FieldType combined = new FieldType();
    combined.setStored(true);
    combined.setIndexOptions(IndexOptions.DOCS);
    combined.setTokenized(false);
    combined.setOmitNorms(true);
    combined.freeze();

    // doc 0 via addDocument establishes the FieldInfo (and the frozen-field fast path).
    Document doc = new Document();
    doc.add(new Field("body", "alpha", combined));
    w.addDocument(doc);

    // doc 1 via a composed batch for the same field name, in the same segment.
    w.addBatch(
        simpleBatch(
            1,
            new ArrayBinaryColumn(
                "body", storedBinaryType(), new int[] {0}, new BytesRef[] {newBytesRef("Beta")}),
            new ArrayBinaryColumn(
                "body", invertedType(), new int[] {0}, new BytesRef[] {newBytesRef("beta")})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher s = new IndexSearcher(r);

    assertEquals(1, s.count(new TermQuery(new Term("body", "alpha"))));
    assertEquals(1, s.count(new TermQuery(new Term("body", "beta"))));

    StoredFields storedFields = leaf.storedFields();
    assertEquals("alpha", storedFields.document(0).getField("body").stringValue());
    assertEquals(newBytesRef("Beta"), storedFields.document(1).getField("body").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testRejectsDuplicateInversionFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayBinaryColumn(
                        "body", invertedType(), new int[] {0}, new BytesRef[] {newBytesRef("a")}),
                    new ArrayBinaryColumn(
                        "body",
                        invertedType(),
                        new int[] {0},
                        new BytesRef[] {newBytesRef("b")}))));
    w.close();
    dir.close();
  }

  public void testRejectsDuplicateStoredFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayBinaryColumn(
                        "body",
                        storedBinaryType(),
                        new int[] {0},
                        new BytesRef[] {newBytesRef("a")}),
                    new ArrayBinaryColumn(
                        "body",
                        storedBinaryType(),
                        new int[] {0},
                        new BytesRef[] {newBytesRef("b")}))));
    w.close();
    dir.close();
  }

  public void testRejectsDuplicateDocValuesFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayLongColumn(
                        "x", NumericDocValuesField.TYPE, new int[] {0}, new long[] {1L}),
                    new ArrayLongColumn(
                        "x", NumericDocValuesField.TYPE, new int[] {0}, new long[] {2L}))));
    w.close();
    dir.close();
  }

  public void testRejectsDuplicatePointsFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayLongColumn(
                        "p", pointType, LongColumn.NumericKind.INT, new int[] {0}, new long[] {1L}),
                    new ArrayLongColumn(
                        "p",
                        pointType,
                        LongColumn.NumericKind.INT,
                        new int[] {0},
                        new long[] {2L}))));
    w.close();
    dir.close();
  }

  public void testRejectsDuplicateVectorFeature() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldType vectorType = floatVectorType(2, VectorSimilarityFunction.EUCLIDEAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    1,
                    new ArrayFloatVectorColumn(
                        "v", vectorType, new int[] {0}, new float[][] {{1f, 2f}}),
                    new ArrayFloatVectorColumn(
                        "v", vectorType, new int[] {0}, new float[][] {{3f, 4f}}))));
    w.close();
    dir.close();
  }
}
