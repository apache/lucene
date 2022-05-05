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
package org.apache.lucene.tests.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;

/**
 * Base class aiming at testing {@link KnnVectorsFormat vectors formats}. To test a new format, all
 * you need is to register a new {@link Codec} which uses it and extend this class and override
 * {@link #getCodec()}.
 *
 * @lucene.experimental
 */
public abstract class BaseKnnVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new KnnVectorField("v2", randomVector(30), VectorSimilarityFunction.EUCLIDEAN));
  }

  public void testFieldConstructor() {
    float[] v = new float[1];
    KnnVectorField field = new KnnVectorField("f", v);
    assertEquals(1, field.fieldType().vectorDimension());
    assertEquals(VectorSimilarityFunction.EUCLIDEAN, field.fieldType().vectorSimilarityFunction());
    assertSame(v, field.vectorValue());
  }

  public void testFieldConstructorExceptions() {
    expectThrows(IllegalArgumentException.class, () -> new KnnVectorField(null, new float[1]));
    expectThrows(IllegalArgumentException.class, () -> new KnnVectorField("f", null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new KnnVectorField("f", new float[1], (VectorSimilarityFunction) null));
    expectThrows(IllegalArgumentException.class, () -> new KnnVectorField("f", new float[0]));
    expectThrows(
        IllegalArgumentException.class,
        () -> new KnnVectorField("f", new float[VectorValues.MAX_DIMENSIONS + 1]));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new KnnVectorField("f", new float[VectorValues.MAX_DIMENSIONS + 1], (FieldType) null));
  }

  public void testFieldSetValue() {
    KnnVectorField field = new KnnVectorField("f", new float[1]);
    float[] v1 = new float[1];
    field.setVectorValue(v1);
    assertSame(v1, field.vectorValue());
    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[2]));
    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(null));
  }

  // Illegal schema change tests:
  public void testIllegalDimChangeTwoDocs() throws Exception {
    // illegal change in the same segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[3], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "Inconsistency of field data structures across documents for field [f] of doc [1]."
              + " vector dimension: expected '4', but it has '3'.";
      assertEquals(errMsg, expected.getMessage());
    }

    // illegal change in a different segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[3], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "cannot change field \"f\" from vector dimension=4, vector similarity function=DOT_PRODUCT "
              + "to inconsistent vector dimension=3, vector similarity function=DOT_PRODUCT";
      assertEquals(errMsg, expected.getMessage());
    }
  }

  public void testIllegalSimilarityFunctionChange() throws Exception {
    // illegal change in the same segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "Inconsistency of field data structures across documents for field [f] of doc [1]."
              + " vector similarity function: expected 'DOT_PRODUCT', but it has 'EUCLIDEAN'.";
      assertEquals(errMsg, expected.getMessage());
    }

    // illegal change a different segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "cannot change field \"f\" from vector dimension=4, vector similarity function=DOT_PRODUCT "
              + "to inconsistent vector dimension=4, vector similarity function=EUCLIDEAN";
      assertEquals(errMsg, expected.getMessage());
    }
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new KnnVectorField("f", new float[1], VectorSimilarityFunction.DOT_PRODUCT));
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addDocument(doc2));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=1, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testIllegalSimilarityFunctionChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addDocument(doc2));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=4, vector similarity function=EUCLIDEAN",
            expected.getMessage());
      }
    }
  }

  public void testAddIndexesDirectory0() throws Exception {
    String fieldName = "field";
    Document doc = new Document();
    doc.add(new KnnVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w2)) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertEquals(0, vectorValues.nextDoc());
          assertEquals(0, vectorValues.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory1() throws Exception {
    String fieldName = "field";
    Document doc = new Document();
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.addDocument(doc);
      }
      doc.add(new KnnVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w2)) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertNotEquals(NO_MORE_DOCS, vectorValues.nextDoc());
          assertEquals(0, vectorValues.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory01() throws Exception {
    String fieldName = "field";
    float[] vector = new float[1];
    Document doc = new Document();
    doc.add(new KnnVectorField(fieldName, vector, VectorSimilarityFunction.DOT_PRODUCT));
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        vector[0] = 1;
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w2)) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertEquals(0, vectorValues.nextDoc());
          // The merge order is randomized, we might get 0 first, or 1
          float value = vectorValues.vectorValue()[0];
          assertTrue(value == 0 || value == 1);
          assertEquals(1, vectorValues.nextDoc());
          value += vectorValues.vectorValue()[0];
          assertEquals(1, value, 0);
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[5], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        IllegalArgumentException expected =
            expectThrows(
                IllegalArgumentException.class, () -> w2.addIndexes(new Directory[] {dir}));
        assertEquals(
            "cannot change field \"f\" from vector dimension=5, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testIllegalSimilarityFunctionChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addIndexes(dir));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector similarity function=EUCLIDEAN "
                + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[5], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(
                  IllegalArgumentException.class,
                  () -> w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)}));
          assertEquals(
              "cannot change field \"f\" from vector dimension=5, vector similarity function=DOT_PRODUCT "
                  + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
              expected.getMessage());
        }
      }
    }
  }

  public void testIllegalSimilarityFunctionChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(
                  IllegalArgumentException.class,
                  () -> w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)}));
          assertEquals(
              "cannot change field \"f\" from vector dimension=4, vector similarity function=EUCLIDEAN "
                  + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
              expected.getMessage());
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[5], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(IllegalArgumentException.class, () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals(
              "cannot change field \"f\" from vector dimension=5, vector similarity function=DOT_PRODUCT "
                  + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
              expected.getMessage());
        }
      }
    }
  }

  public void testIllegalSimilarityFunctionChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(IllegalArgumentException.class, () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals(
              "cannot change field \"f\" from vector dimension=4, vector similarity function=EUCLIDEAN "
                  + "to inconsistent vector dimension=4, vector similarity function=DOT_PRODUCT",
              expected.getMessage());
        }
      }
    }
  }

  public void testIllegalMultipleValues() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
      assertEquals(
          "VectorValuesField \"f\" appears more than once in this document (only one value is allowed per field)",
          expected.getMessage());
    }
  }

  public void testIllegalDimensionTooLarge() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      expectThrows(
          IllegalArgumentException.class,
          () ->
              doc.add(
                  new KnnVectorField(
                      "f",
                      new float[VectorValues.MAX_DIMENSIONS + 1],
                      VectorSimilarityFunction.DOT_PRODUCT)));

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[1], VectorSimilarityFunction.EUCLIDEAN));
      w.addDocument(doc2);
    }
  }

  public void testIllegalEmptyVector() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      Exception e =
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  doc.add(
                      new KnnVectorField("f", new float[0], VectorSimilarityFunction.EUCLIDEAN)));
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new KnnVectorField("f", new float[1], VectorSimilarityFunction.EUCLIDEAN));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  // Write vectors, one segment with with SimpleText, another with default codec, then forceMerge
  public void testDifferentCodecs2() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("SimpleText"));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidKnnVectorFieldUsage() {
    KnnVectorField field =
        new KnnVectorField("field", new float[2], VectorSimilarityFunction.EUCLIDEAN);

    expectThrows(IllegalArgumentException.class, () -> field.setIntValue(14));

    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[1]));

    assertNull(field.numericValue());
  }

  public void testDeleteAllVectorDocs() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(new KnnVectorField("v", new float[] {2, 3, 5}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();

      try (DirectoryReader r = DirectoryReader.open(w)) {
        VectorValues values = getOnlyLeafReader(r).getVectorValues("v");
        assertNotNull(values);
        assertEquals(1, values.size());
      }
      w.deleteDocuments(new Term("id", "0"));
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(r);
        VectorValues values = leafReader.getVectorValues("v");
        assertNotNull(values);
        assertEquals(0, values.size());

        // assert that knn search doesn't fail on a field with all deleted docs
        TopDocs results =
            leafReader.searchNearestVectors(
                "v", randomVector(3), 1, leafReader.getLiveDocs(), Integer.MAX_VALUE);
        assertEquals(0, results.scoreDocs.length);
      }
    }
  }

  public void testKnnVectorFieldMissingFromOneSegment() throws Exception {
    try (Directory dir = FSDirectory.open(createTempDir());
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(
          new KnnVectorField("v0", new float[] {2, 3, 5}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(
          new KnnVectorField("v1", new float[] {2, 3, 5}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.forceMerge(1);
    }
  }

  public void testSparseVectors() throws Exception {
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    int[] fieldDocCounts = new int[numFields];
    double[] fieldTotals = new double[numFields];
    int[] fieldDims = new int[numFields];
    VectorSimilarityFunction[] fieldSearchStrategies = new VectorSimilarityFunction[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      fieldSearchStrategies[i] =
          VectorSimilarityFunction.values()[
              random().nextInt(VectorSimilarityFunction.values().length)];
    }
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            float[] v = randomVector(fieldDims[field]);
            doc.add(new KnnVectorField(fieldName, v, fieldSearchStrategies[field]));
            fieldDocCounts[field]++;
            fieldTotals[field] += v[0];
          }
        }
        w.addDocument(doc);
      }

      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          double checksum = 0;
          String fieldName = "int" + field;
          for (LeafReaderContext ctx : r.leaves()) {
            VectorValues vectors = ctx.reader().getVectorValues(fieldName);
            if (vectors != null) {
              docCount += vectors.size();
              while (vectors.nextDoc() != NO_MORE_DOCS) {
                checksum += vectors.vectorValue()[0];
              }
            }
          }
          assertEquals(fieldDocCounts[field], docCount);
          assertEquals(fieldTotals[field], checksum, 1e-5);
        }
      }
    }
  }

  public void testIndexedValueNotAliased() throws Exception {
    // We copy indexed values (as for BinaryDocValues) so the input float[] can be reused across
    // calls to IndexWriter.addDocument.
    String fieldName = "field";
    float[] v = {0};
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc1 = new Document();
      doc1.add(new KnnVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      v[0] = 1;
      Document doc2 = new Document();
      doc2.add(new KnnVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc1);
      iw.addDocument(doc2);
      v[0] = 2;
      Document doc3 = new Document();
      doc3.add(new KnnVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc3);
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader r = getOnlyLeafReader(reader);
        VectorValues vectorValues = r.getVectorValues(fieldName);
        vectorValues.nextDoc();
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(2, vectorValues.vectorValue()[0], 0);
      }
    }
  }

  public void testSortedIndex() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      add(iw, fieldName, 1, 1, new float[] {-1, 0});
      add(iw, fieldName, 4, 4, new float[] {0, 1});
      add(iw, fieldName, 3, 3, null);
      add(iw, fieldName, 2, 2, new float[] {1, 0});
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        VectorValues vectorValues = leaf.getVectorValues(fieldName);
        assertEquals(2, vectorValues.dimension());
        assertEquals(3, vectorValues.size());
        assertEquals("1", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(-1f, vectorValues.vectorValue()[0], 0);
        assertEquals("2", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        assertEquals("4", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(0, vectorValues.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
      }
    }
  }

  public void testIndexMultipleKnnVectorFields() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      float[] v = new float[] {1};
      doc.add(new KnnVectorField("field1", v, VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnVectorField("field2", new float[] {1, 2, 3}, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc);
      v[0] = 2;
      iw.addDocument(doc);
      doc = new Document();
      doc.add(
          new KnnVectorField(
              "field3", new float[] {1, 2, 3}, VectorSimilarityFunction.DOT_PRODUCT));
      iw.addDocument(doc);
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader leaf = reader.leaves().get(0).reader();

        VectorValues vectorValues = leaf.getVectorValues("field1");
        assertEquals(1, vectorValues.dimension());
        assertEquals(2, vectorValues.size());
        vectorValues.nextDoc();
        assertEquals(1f, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(2f, vectorValues.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());

        VectorValues vectorValues2 = leaf.getVectorValues("field2");
        assertEquals(3, vectorValues2.dimension());
        assertEquals(2, vectorValues2.size());
        vectorValues2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue()[1], 0);
        vectorValues2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue()[1], 0);
        assertEquals(NO_MORE_DOCS, vectorValues2.nextDoc());

        VectorValues vectorValues3 = leaf.getVectorValues("field3");
        assertEquals(3, vectorValues3.dimension());
        assertEquals(1, vectorValues3.size());
        vectorValues3.nextDoc();
        assertEquals(1f, vectorValues3.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues3.nextDoc());
      }
    }
  }

  /**
   * Index random vectors, sometimes skipping documents, sometimes deleting a document, sometimes
   * merging, sometimes sorting the index, and verify that the expected values can be read back
   * consistently.
   */
  public void testRandom() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    if (random().nextBoolean()) {
      iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[] scratch = new float[dimension];
      int numValues = 0;
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = randomVector(dimension);
          ++numValues;
        }
        if (random().nextBoolean() && values[i] != null) {
          // sometimes use a shared scratch array
          System.arraycopy(values[i], 0, scratch, 0, scratch.length);
          add(iw, fieldName, i, scratch, VectorSimilarityFunction.EUCLIDEAN);
        } else {
          add(iw, fieldName, i, values[i], VectorSimilarityFunction.EUCLIDEAN);
        }
        if (random().nextInt(10) == 2) {
          // sometimes delete a random document
          int idToDelete = random().nextInt(i + 1);
          iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
          // and remember that it was deleted
          if (values[idToDelete] != null) {
            values[idToDelete] = null;
            --numValues;
          }
        }
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      int numDeletes = 0;
      try (IndexReader reader = DirectoryReader.open(iw)) {
        int valueCount = 0, totalSize = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
          VectorValues vectorValues = ctx.reader().getVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          totalSize += vectorValues.size();
          int docId;
          while ((docId = vectorValues.nextDoc()) != NO_MORE_DOCS) {
            float[] v = vectorValues.vectorValue();
            assertEquals(dimension, v.length);
            String idString = ctx.reader().document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(docId)) {
              assertArrayEquals(idString, values[id], v, 0);
              ++valueCount;
            } else {
              ++numDeletes;
              assertNull(values[id]);
            }
          }
        }
        assertEquals(numValues, valueCount);
        assertEquals(numValues, totalSize - numDeletes);
      }
    }
  }

  /**
   * Tests whether {@link KnnVectorsReader#search} implementations obey the limit on the number of
   * visited vectors. This test is a best-effort attempt to capture the right behavior, and isn't
   * meant to define a strict requirement on behavior.
   */
  public void testSearchWithVisitedLimit() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = 300;
      int dimension = 10;
      for (int i = 0; i < numDoc; i++) {
        float[] value;
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          value = randomVector(dimension);
        } else {
          value = null;
        }
        add(iw, fieldName, i, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      iw.forceMerge(1);

      // randomly delete some documents
      for (int i = 0; i < 30; i++) {
        int idToDelete = random().nextInt(numDoc);
        iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
      }

      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          VectorValues vectorValues = ctx.reader().getVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }

          // check the limit is hit when it's very small
          int k = 5 + random().nextInt(45);
          int visitedLimit = k + random().nextInt(5);
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomVector(dimension), k, liveDocs, visitedLimit);
          assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, results.totalHits.relation);
          assertEquals(visitedLimit, results.totalHits.value);

          // check the limit is not hit when it clearly exceeds the number of vectors
          k = vectorValues.size();
          visitedLimit = k + 30;
          results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomVector(dimension), k, liveDocs, visitedLimit);
          assertEquals(TotalHits.Relation.EQUAL_TO, results.totalHits.relation);
          assertTrue(results.totalHits.value <= visitedLimit);
        }
      }
    }
  }

  /**
   * Index random vectors, sometimes skipping documents, sometimes updating a document, sometimes
   * merging, sometimes sorting the index, using an HNSW similarity function so as to also produce a
   * graph, and verify that the expected values can be read back consistently.
   */
  public void testRandomWithUpdatesAndGraph() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[][] id2value = new float[numDoc][];
      int[] id2ord = new int[numDoc];
      for (int i = 0; i < numDoc; i++) {
        int id = random().nextInt(numDoc);
        float[] value;
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          value = randomVector(dimension);
        } else {
          value = null;
        }
        id2value[id] = value;
        id2ord[id] = i;
        add(iw, fieldName, id, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          VectorValues vectorValues = ctx.reader().getVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          int docId;
          int numLiveDocsWithVectors = 0;
          while ((docId = vectorValues.nextDoc()) != NO_MORE_DOCS) {
            float[] v = vectorValues.vectorValue();
            assertEquals(dimension, v.length);
            String idString = ctx.reader().document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (liveDocs == null || liveDocs.get(docId)) {
              assertArrayEquals(
                  "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                  id2value[id],
                  v,
                  0);
              numLiveDocsWithVectors++;
            } else {
              if (id2value[id] != null) {
                assertFalse(Arrays.equals(id2value[id], v));
              }
            }
          }

          if (numLiveDocsWithVectors == 0) {
            continue;
          }

          // assert that searchNearestVectors returns the expected number of documents,
          // in descending score order
          int size = ctx.reader().getVectorValues(fieldName).size();
          int k = random().nextInt(size / 2 + 1) + 1;
          if (k > numLiveDocsWithVectors) {
            k = numLiveDocsWithVectors;
          }
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomVector(dimension), k, liveDocs, Integer.MAX_VALUE);
          assertEquals(Math.min(k, size), results.scoreDocs.length);
          for (int i = 0; i < k - 1; i++) {
            assertTrue(results.scoreDocs[i].score >= results.scoreDocs[i + 1].score);
          }
        }
      }
    }
  }

  private void add(
      IndexWriter iw,
      String field,
      int id,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    add(iw, field, id, random().nextInt(100), vector, similarityFunction);
  }

  private void add(IndexWriter iw, String field, int id, int sortkey, float[] vector)
      throws IOException {
    add(iw, field, id, sortkey, vector, VectorSimilarityFunction.EUCLIDEAN);
  }

  private void add(
      IndexWriter iw,
      String field,
      int id,
      int sortkey,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new KnnVectorField(field, vector, similarityFunction));
    }
    doc.add(new NumericDocValuesField("sortkey", sortkey));
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    Term idTerm = new Term("id", idString);
    iw.updateDocument(idTerm, doc);
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat();
    }
    VectorUtil.l2normalize(v);
    return v;
  }

  public void testCheckIndexIncludesVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnVectorField("v1", randomVector(3), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);

        doc.add(new KnnVectorField("v2", randomVector(3), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, true, output);
      assertEquals(1, status.segmentInfos.size());
      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
      // total 3 vector values were indexed:
      assertEquals(3, segStatus.vectorValuesStatus.totalVectorValues);
      // ... across 2 fields:
      assertEquals(2, segStatus.vectorValuesStatus.totalKnnVectorFields);

      // Make sure CheckIndex in fact declares that it is testing vectors!
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors..."));
    }
  }

  public void testSimilarityFunctionIdentifiers() {
    // make sure we don't accidentally mess up similarity function identifiers by re-ordering their
    // enumerators
    assertEquals(0, VectorSimilarityFunction.EUCLIDEAN.ordinal());
    assertEquals(1, VectorSimilarityFunction.DOT_PRODUCT.ordinal());
    assertEquals(2, VectorSimilarityFunction.COSINE.ordinal());
    assertEquals(3, VectorSimilarityFunction.values().length);
  }

  public void testAdvance() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        int numdocs = atLeast(1500);
        String fieldName = "field";
        for (int i = 0; i < numdocs; i++) {
          Document doc = new Document();
          // randomly add a vector field
          if (random().nextInt(4) == 3) {
            doc.add(
                new KnnVectorField(fieldName, new float[4], VectorSimilarityFunction.EUCLIDEAN));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          int[] vectorDocs = new int[vectorValues.size() + 1];
          int cur = -1;
          while (++cur < vectorValues.size() + 1) {
            vectorDocs[cur] = vectorValues.nextDoc();
            if (cur != 0) {
              assertTrue(vectorDocs[cur] > vectorDocs[cur - 1]);
            }
          }
          vectorValues = r.getVectorValues(fieldName);
          cur = -1;
          for (int i = 0; i < numdocs; i++) {
            // randomly advance to i
            if (random().nextInt(4) == 3) {
              while (vectorDocs[++cur] < i)
                ;
              assertEquals(vectorDocs[cur], vectorValues.advance(i));
              assertEquals(vectorDocs[cur], vectorValues.docID());
              if (vectorValues.docID() == NO_MORE_DOCS) {
                break;
              }
              // make i equal to docid so that it is greater than docId in the next loop iteration
              i = vectorValues.docID();
            }
          }
        }
      }
    }
  }

  public void testVectorValuesReportCorrectDocs() throws Exception {
    final int numDocs = atLeast(1000);
    final int dim = random().nextInt(20) + 1;
    final VectorSimilarityFunction similarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];

    double fieldValuesCheckSum = 0;
    int fieldDocCount = 0;
    long fieldSumDocIDs = 0;

    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        int docID = random().nextInt(numDocs);
        doc.add(new StoredField("id", docID));
        if (random().nextInt(4) == 3) {
          float[] vector = randomVector(dim);
          doc.add(new KnnVectorField("knn_vector", vector, similarityFunction));
          fieldValuesCheckSum += vector[0];
          fieldDocCount++;
          fieldSumDocIDs += docID;
        }
        w.addDocument(doc);
      }

      if (random().nextBoolean()) {
        w.forceMerge(1);
      }

      try (IndexReader r = w.getReader()) {
        double checksum = 0;
        int docCount = 0;
        long sumDocIds = 0;
        for (LeafReaderContext ctx : r.leaves()) {
          VectorValues vectors = ctx.reader().getVectorValues("knn_vector");
          if (vectors != null) {
            docCount += vectors.size();
            while (vectors.nextDoc() != NO_MORE_DOCS) {
              checksum += vectors.vectorValue()[0];
              Document doc = ctx.reader().document(vectors.docID(), Set.of("id"));
              sumDocIds += Integer.parseInt(doc.get("id"));
            }
          }
        }
        assertEquals(fieldValuesCheckSum, checksum, 1e-3);
        assertEquals(fieldDocCount, docCount);
        assertEquals(fieldSumDocIDs, sumDocIds);
      }
    }
  }
}
