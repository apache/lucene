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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.NnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NnVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Base class aiming at testing {@link NnVectorsFormat vectors formats}. To test a new format, all
 * you need is to register a new {@link Codec} which uses it and extend this class and override
 * {@link #getCodec()}.
 *
 * @lucene.experimental
 */
public abstract class BaseNnVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new NnVectorField("v2", randomVector(30), NnVectors.SimilarityFunction.NONE));
  }

  public void testFieldConstructor() {
    float[] v = new float[1];
    NnVectorField field = new NnVectorField("f", v);
    assertEquals(1, field.fieldType().vectorDimension());
    assertEquals(
        NnVectors.SimilarityFunction.EUCLIDEAN, field.fieldType().vectorSimilarityFunction());
    assertSame(v, field.vectorValue());
  }

  public void testFieldConstructorExceptions() {
    expectThrows(IllegalArgumentException.class, () -> new NnVectorField(null, new float[1]));
    expectThrows(IllegalArgumentException.class, () -> new NnVectorField("f", null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new NnVectorField("f", new float[1], (NnVectors.SimilarityFunction) null));
    expectThrows(IllegalArgumentException.class, () -> new NnVectorField("f", new float[0]));
    expectThrows(
        IllegalArgumentException.class,
        () -> new NnVectorField("f", new float[NnVectors.MAX_DIMENSIONS + 1]));
    expectThrows(
        IllegalArgumentException.class,
        () -> new NnVectorField("f", new float[NnVectors.MAX_DIMENSIONS + 1], (FieldType) null));
  }

  public void testFieldSetValue() {
    NnVectorField field = new NnVectorField("f", new float[1]);
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
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[3], NnVectors.SimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "Inconsistency of field data structures across documents for field [f] of doc [1].";
      assertEquals(errMsg, expected.getMessage());
    }

    // illegal change in a different segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[3], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "Inconsistency of field data structures across documents for field [f] of doc [1].";
      assertEquals(errMsg, expected.getMessage());
    }

    // illegal change a different segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new NnVectorField("f", new float[1], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
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
    doc.add(new NnVectorField(fieldName, new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          NnVectors nnVectors = r.getNnVectors(fieldName);
          assertEquals(0, nnVectors.nextDoc());
          assertEquals(0, nnVectors.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, nnVectors.nextDoc());
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
      doc.add(new NnVectorField(fieldName, new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          NnVectors nnVectors = r.getNnVectors(fieldName);
          assertNotEquals(NO_MORE_DOCS, nnVectors.nextDoc());
          assertEquals(0, nnVectors.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, nnVectors.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory01() throws Exception {
    String fieldName = "field";
    float[] vector = new float[1];
    Document doc = new Document();
    doc.add(new NnVectorField(fieldName, vector, NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          NnVectors nnVectors = r.getNnVectors(fieldName);
          assertEquals(0, nnVectors.nextDoc());
          // The merge order is randomized, we might get 0 first, or 1
          float value = nnVectors.vectorValue()[0];
          assertTrue(value == 0 || value == 1);
          assertEquals(1, nnVectors.nextDoc());
          value += nnVectors.vectorValue()[0];
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[5], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[5], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[5], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.EUCLIDEAN));
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
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
      assertEquals(
          "NnVectorField \"f\" appears more than once in this document (only one value is allowed per field)",
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
                  new NnVectorField(
                      "f",
                      new float[NnVectors.MAX_DIMENSIONS + 1],
                      NnVectors.SimilarityFunction.DOT_PRODUCT)));

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[1], NnVectors.SimilarityFunction.EUCLIDEAN));
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
                  doc.add(new NnVectorField("f", new float[0], NnVectors.SimilarityFunction.NONE)));
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new NnVectorField("f", new float[1], NnVectors.SimilarityFunction.NONE));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
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
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new NnVectorField("f", new float[4], NnVectors.SimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidNnVectorFieldUsage() {
    NnVectorField field =
        new NnVectorField("field", new float[2], NnVectors.SimilarityFunction.NONE);

    expectThrows(IllegalArgumentException.class, () -> field.setIntValue(14));

    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[1]));

    assertNull(field.numericValue());
  }

  public void testDeleteAllNnVectorDocs() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(
          new NnVectorField("v", new float[] {2, 3, 5}, NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();

      try (DirectoryReader r = w.getReader()) {
        NnVectors values = getOnlyLeafReader(r).getNnVectors("v");
        assertNotNull(values);
        assertEquals(1, values.size());
      }
      w.deleteDocuments(new Term("id", "0"));
      w.forceMerge(1);
      try (DirectoryReader r = w.getReader()) {
        NnVectors values = getOnlyLeafReader(r).getNnVectors("v");
        assertNotNull(values);
        assertEquals(0, values.size());
      }
    }
  }

  public void testNnVectorFieldMissingFromOneSegment() throws Exception {
    try (Directory dir = FSDirectory.open(createTempDir());
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(
          new NnVectorField("v0", new float[] {2, 3, 5}, NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(
          new NnVectorField("v1", new float[] {2, 3, 5}, NnVectors.SimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.forceMerge(1);
    }
  }

  public void testSparseVectors() throws Exception {
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    int[] fieldDocCounts = new int[numFields];
    float[] fieldTotals = new float[numFields];
    int[] fieldDims = new int[numFields];
    NnVectors.SimilarityFunction[] fieldSearchStrategies =
        new NnVectors.SimilarityFunction[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      fieldSearchStrategies[i] =
          NnVectors.SimilarityFunction.values()[
              random().nextInt(NnVectors.SimilarityFunction.values().length)];
    }
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            float[] v = randomVector(fieldDims[field]);
            doc.add(new NnVectorField(fieldName, v, fieldSearchStrategies[field]));
            fieldDocCounts[field]++;
            fieldTotals[field] += v[0];
          }
        }
        w.addDocument(doc);
      }

      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          float checksum = 0;
          String fieldName = "int" + field;
          for (LeafReaderContext ctx : r.leaves()) {
            NnVectors vectors = ctx.reader().getNnVectors(fieldName);
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
      doc1.add(new NnVectorField(fieldName, v, NnVectors.SimilarityFunction.EUCLIDEAN));
      v[0] = 1;
      Document doc2 = new Document();
      doc2.add(new NnVectorField(fieldName, v, NnVectors.SimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc1);
      iw.addDocument(doc2);
      v[0] = 2;
      Document doc3 = new Document();
      doc3.add(new NnVectorField(fieldName, v, NnVectors.SimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc3);
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        LeafReader r = getOnlyLeafReader(reader);
        NnVectors nnVectors = r.getNnVectors(fieldName);
        nnVectors.nextDoc();
        assertEquals(1, nnVectors.vectorValue()[0], 0);
        nnVectors.nextDoc();
        assertEquals(1, nnVectors.vectorValue()[0], 0);
        nnVectors.nextDoc();
        assertEquals(2, nnVectors.vectorValue()[0], 0);
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
      try (IndexReader reader = iw.getReader()) {
        LeafReader leaf = getOnlyLeafReader(reader);

        NnVectors nnVectors = leaf.getNnVectors(fieldName);
        assertEquals(2, nnVectors.dimension());
        assertEquals(3, nnVectors.size());
        assertEquals("1", leaf.document(nnVectors.nextDoc()).get("id"));
        assertEquals(-1f, nnVectors.vectorValue()[0], 0);
        assertEquals("2", leaf.document(nnVectors.nextDoc()).get("id"));
        assertEquals(1, nnVectors.vectorValue()[0], 0);
        assertEquals("4", leaf.document(nnVectors.nextDoc()).get("id"));
        assertEquals(0, nnVectors.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, nnVectors.nextDoc());

        RandomAccessNnVectors ra = ((RandomAccessNnVectorsProducer) nnVectors).randomAccess();
        assertEquals(-1f, ra.vectorValue(0)[0], 0);
        assertEquals(1f, ra.vectorValue(1)[0], 0);
        assertEquals(0f, ra.vectorValue(2)[0], 0);
      }
    }
  }

  public void testIndexMultipleNnVectorFields() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      float[] v = new float[] {1};
      doc.add(new NnVectorField("field1", v, NnVectors.SimilarityFunction.EUCLIDEAN));
      doc.add(
          new NnVectorField("field2", new float[] {1, 2, 3}, NnVectors.SimilarityFunction.NONE));
      iw.addDocument(doc);
      v[0] = 2;
      iw.addDocument(doc);
      doc = new Document();
      doc.add(
          new NnVectorField(
              "field3", new float[] {1, 2, 3}, NnVectors.SimilarityFunction.DOT_PRODUCT));
      iw.addDocument(doc);
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        LeafReader leaf = reader.leaves().get(0).reader();

        NnVectors nnVectors = leaf.getNnVectors("field1");
        assertEquals(1, nnVectors.dimension());
        assertEquals(2, nnVectors.size());
        nnVectors.nextDoc();
        assertEquals(1f, nnVectors.vectorValue()[0], 0);
        nnVectors.nextDoc();
        assertEquals(2f, nnVectors.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, nnVectors.nextDoc());

        NnVectors nnVectors2 = leaf.getNnVectors("field2");
        assertEquals(3, nnVectors2.dimension());
        assertEquals(2, nnVectors2.size());
        nnVectors2.nextDoc();
        assertEquals(2f, nnVectors2.vectorValue()[1], 0);
        nnVectors2.nextDoc();
        assertEquals(2f, nnVectors2.vectorValue()[1], 0);
        assertEquals(NO_MORE_DOCS, nnVectors2.nextDoc());

        NnVectors nnVectors3 = leaf.getNnVectors("field3");
        assertEquals(3, nnVectors3.dimension());
        assertEquals(1, nnVectors3.size());
        nnVectors3.nextDoc();
        assertEquals(1f, nnVectors3.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, nnVectors3.nextDoc());
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
          add(iw, fieldName, i, scratch, NnVectors.SimilarityFunction.NONE);
        } else {
          add(iw, fieldName, i, values[i], NnVectors.SimilarityFunction.NONE);
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
      try (IndexReader reader = iw.getReader()) {
        int valueCount = 0, totalSize = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
          NnVectors nnVectors = ctx.reader().getNnVectors(fieldName);
          if (nnVectors == null) {
            continue;
          }
          totalSize += nnVectors.size();
          int docId;
          while ((docId = nnVectors.nextDoc()) != NO_MORE_DOCS) {
            float[] v = nnVectors.vectorValue();
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
        add(iw, fieldName, id, value, NnVectors.SimilarityFunction.EUCLIDEAN);
      }
      try (IndexReader reader = iw.getReader()) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          NnVectors nnVectors = ctx.reader().getNnVectors(fieldName);
          if (nnVectors == null) {
            continue;
          }
          int docId;
          while ((docId = nnVectors.nextDoc()) != NO_MORE_DOCS) {
            float[] v = nnVectors.vectorValue();
            assertEquals(dimension, v.length);
            String idString = ctx.reader().document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (liveDocs == null || liveDocs.get(docId)) {
              assertArrayEquals(
                  "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                  id2value[id],
                  v,
                  0);
            } else {
              if (id2value[id] != null) {
                assertFalse(Arrays.equals(id2value[id], v));
              }
            }
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
      NnVectors.SimilarityFunction similarityFunction)
      throws IOException {
    add(iw, field, id, random().nextInt(100), vector, similarityFunction);
  }

  private void add(IndexWriter iw, String field, int id, int sortkey, float[] vector)
      throws IOException {
    add(iw, field, id, sortkey, vector, NnVectors.SimilarityFunction.NONE);
  }

  private void add(
      IndexWriter iw,
      String field,
      int id,
      int sortkey,
      float[] vector,
      NnVectors.SimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new NnVectorField(field, vector, similarityFunction));
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
        doc.add(new NnVectorField("v1", randomVector(3), NnVectors.SimilarityFunction.NONE));
        w.addDocument(doc);

        doc.add(new NnVectorField("v2", randomVector(3), NnVectors.SimilarityFunction.NONE));
        w.addDocument(doc);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, output);
      assertEquals(1, status.segmentInfos.size());
      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
      // total 3 vector values were indexed:
      assertEquals(3, segStatus.nnVectorsStatus.totalNnVectors);
      // ... across 2 fields:
      assertEquals(2, segStatus.nnVectorsStatus.totalNnVectorFields);

      // Make sure CheckIndex in fact declares that it is testing vectors!
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors..."));
    }
  }

  public void testSimilarityFunctionIdentifiers() {
    // make sure we don't accidentally mess up similarity function identifiers by re-ordering their
    // enumerators
    assertEquals(0, NnVectors.SimilarityFunction.NONE.ordinal());
    assertEquals(1, NnVectors.SimilarityFunction.EUCLIDEAN.ordinal());
    assertEquals(2, NnVectors.SimilarityFunction.DOT_PRODUCT.ordinal());
    assertEquals(3, NnVectors.SimilarityFunction.values().length);
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
            doc.add(new NnVectorField(fieldName, new float[4], NnVectors.SimilarityFunction.NONE));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (IndexReader reader = w.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          NnVectors nnVectors = r.getNnVectors(fieldName);
          int[] vectorDocs = new int[nnVectors.size() + 1];
          int cur = -1;
          while (++cur < nnVectors.size() + 1) {
            vectorDocs[cur] = nnVectors.nextDoc();
            if (cur != 0) {
              assertTrue(vectorDocs[cur] > vectorDocs[cur - 1]);
            }
          }
          nnVectors = r.getNnVectors(fieldName);
          cur = -1;
          for (int i = 0; i < numdocs; i++) {
            // randomly advance to i
            if (random().nextInt(4) == 3) {
              while (vectorDocs[++cur] < i)
                ;
              assertEquals(vectorDocs[cur], nnVectors.advance(i));
              assertEquals(vectorDocs[cur], nnVectors.docID());
              if (nnVectors.docID() == NO_MORE_DOCS) {
                break;
              }
              // make i equal to docid so that it is greater than docId in the next loop iteration
              i = nnVectors.docID();
            }
          }
        }
      }
    }
  }
}
