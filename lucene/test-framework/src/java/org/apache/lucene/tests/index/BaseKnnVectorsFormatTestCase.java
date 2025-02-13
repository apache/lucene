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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.Version;
import org.junit.Before;

/**
 * Base class aiming at testing {@link KnnVectorsFormat vectors formats}. To test a new format, all
 * you need is to register a new {@link Codec} which uses it and extend this class and override
 * {@link #getCodec()}.
 *
 * @lucene.experimental
 */
public abstract class BaseKnnVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

  private VectorEncoding vectorEncoding;
  private VectorSimilarityFunction similarityFunction;

  @Before
  public void init() {
    vectorEncoding = randomVectorEncoding();
    similarityFunction = randomSimilarity();
  }

  @Override
  protected void addRandomFields(Document doc) {
    switch (vectorEncoding) {
      case BYTE -> doc.add(new KnnByteVectorField("v2", randomVector8(30), similarityFunction));
      case FLOAT32 ->
          doc.add(new KnnFloatVectorField("v2", randomNormalizedVector(30), similarityFunction));
    }
  }

  @Override
  protected boolean mergeIsStable() {
    // suppress this test from base class: merges for knn graphs are not stable due to connected
    // components
    // logic
    return false;
  }

  private int getVectorsMaxDimensions(String fieldName) {
    return Codec.getDefault().knnVectorsFormat().getMaxDimensions(fieldName);
  }

  public void testFieldConstructor() {
    float[] v = new float[1];
    KnnFloatVectorField field = new KnnFloatVectorField("f", v);
    assertEquals(1, field.fieldType().vectorDimension());
    assertEquals(VectorSimilarityFunction.EUCLIDEAN, field.fieldType().vectorSimilarityFunction());
    assertSame(v, field.vectorValue());
  }

  public void testFieldConstructorExceptions() {
    expectThrows(IllegalArgumentException.class, () -> new KnnFloatVectorField(null, new float[1]));
    expectThrows(IllegalArgumentException.class, () -> new KnnFloatVectorField("f", null));
    expectThrows(
        IllegalArgumentException.class,
        () -> new KnnFloatVectorField("f", new float[1], (VectorSimilarityFunction) null));
    expectThrows(IllegalArgumentException.class, () -> new KnnFloatVectorField("f", new float[0]));
  }

  public void testFieldSetValue() {
    KnnFloatVectorField field = new KnnFloatVectorField("f", new float[1]);
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
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[6], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "Inconsistency of field data structures across documents for field [f] of doc [1]."
              + " vector dimension: expected '4', but it has '6'.";
      assertEquals(errMsg, expected.getMessage());
    }

    // illegal change in a different segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[6], VectorSimilarityFunction.DOT_PRODUCT));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
              + "to inconsistent vector dimension=6, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT";
      assertEquals(errMsg, expected.getMessage());
    }
  }

  public void testIllegalSimilarityFunctionChange() throws Exception {
    // illegal change in the same segment
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
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
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc2));
      String errMsg =
          "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
              + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=EUCLIDEAN";
      assertEquals(errMsg, expected.getMessage());
    }
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new KnnFloatVectorField("f", new float[2], VectorSimilarityFunction.DOT_PRODUCT));
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addDocument(doc2));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=2, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testMergingWithDifferentKnnFields() throws Exception {
    try (var dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      Codec codec = getCodec();
      if (codec.knnVectorsFormat() instanceof PerFieldKnnVectorsFormat perFieldKnnVectorsFormat) {
        final KnnVectorsFormat format =
            perFieldKnnVectorsFormat.getKnnVectorsFormatForField("field");
        iwc.setCodec(
            new FilterCodec(codec.getName(), codec) {
              @Override
              public KnnVectorsFormat knnVectorsFormat() {
                return format;
              }
            });
      }
      TestMergeScheduler mergeScheduler = new TestMergeScheduler();
      iwc.setMergeScheduler(mergeScheduler);
      iwc.setMergePolicy(new ForceMergePolicy(iwc.getMergePolicy()));
      try (var writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 10; i++) {
          var doc = new Document();
          doc.add(new KnnFloatVectorField("field", new float[] {i, i + 1, i + 2, i + 3}));
          writer.addDocument(doc);
        }
        writer.commit();
        for (int i = 0; i < 10; i++) {
          var doc = new Document();
          doc.add(new KnnFloatVectorField("otherVector", new float[] {i, i, i, i}));
          writer.addDocument(doc);
        }
        writer.commit();
        writer.forceMerge(1);
        assertNull(mergeScheduler.ex.get());
      }
    }
  }

  public void testMergingWithDifferentByteKnnFields() throws Exception {
    try (var dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      Codec codec = getCodec();
      if (codec.knnVectorsFormat() instanceof PerFieldKnnVectorsFormat perFieldKnnVectorsFormat) {
        final KnnVectorsFormat format =
            perFieldKnnVectorsFormat.getKnnVectorsFormatForField("field");
        iwc.setCodec(
            new FilterCodec(codec.getName(), codec) {
              @Override
              public KnnVectorsFormat knnVectorsFormat() {
                return format;
              }
            });
      }
      TestMergeScheduler mergeScheduler = new TestMergeScheduler();
      iwc.setMergeScheduler(mergeScheduler);
      iwc.setMergePolicy(new ForceMergePolicy(iwc.getMergePolicy()));
      try (var writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 10; i++) {
          var doc = new Document();
          doc.add(
              new KnnByteVectorField("field", new byte[] {(byte) i, (byte) i, (byte) i, (byte) i}));
          writer.addDocument(doc);
        }
        writer.commit();
        for (int i = 0; i < 10; i++) {
          var doc = new Document();
          doc.add(
              new KnnByteVectorField(
                  "otherVector", new byte[] {(byte) i, (byte) i, (byte) i, (byte) i}));
          writer.addDocument(doc);
        }
        writer.commit();
        writer.forceMerge(1);
        assertNull(mergeScheduler.ex.get());
      }
    }
  }

  private static final class TestMergeScheduler extends MergeScheduler {
    AtomicReference<Exception> ex = new AtomicReference<>();

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      while (true) {
        MergePolicy.OneMerge merge = mergeSource.getNextMerge();
        if (merge == null) {
          break;
        }
        try {
          mergeSource.merge(merge);
        } catch (IllegalStateException | IllegalArgumentException e) {
          ex.set(e);
          break;
        }
      }
    }

    @Override
    public void close() {}
  }

  @SuppressWarnings("unchecked")
  public void testWriterRamEstimate() throws Exception {
    final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
    final KnnVectorsFormat format = codec.knnVectorsFormat();
    try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
      final long ramBytesUsed = writer.ramBytesUsed();
      int dim = random().nextInt(64) + 1;
      if (dim % 2 == 1) {
        ++dim;
      }
      int numDocs = atLeast(100);
      KnnFieldVectorsWriter<float[]> fieldWriter =
          (KnnFieldVectorsWriter<float[]>)
              writer.addField(
                  new FieldInfo(
                      "fieldA",
                      0,
                      false,
                      false,
                      false,
                      IndexOptions.NONE,
                      DocValuesType.NONE,
                      DocValuesSkipIndexType.NONE,
                      -1,
                      Map.of(),
                      0,
                      0,
                      0,
                      dim,
                      VectorEncoding.FLOAT32,
                      VectorSimilarityFunction.DOT_PRODUCT,
                      false,
                      false));
      for (int i = 0; i < numDocs; i++) {
        fieldWriter.addValue(i, randomVector(dim));
      }
      final long ramBytesUsed2 = writer.ramBytesUsed();
      assertTrue(ramBytesUsed2 > ramBytesUsed);
      assertTrue(ramBytesUsed2 > (long) dim * numDocs * Float.BYTES);
    }
    dir.close();
  }

  public void testIllegalSimilarityFunctionChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addDocument(doc2));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=EUCLIDEAN",
            expected.getMessage());
      }
    }
  }

  public void testAddIndexesDirectory0() throws Exception {
    String fieldName = "field";
    Document doc = new Document();
    doc.add(new KnnFloatVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
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
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          assertEquals(0, iterator.nextDoc());
          assertEquals(0, vectorValues.vectorValue(0)[0], 0);
          assertEquals(NO_MORE_DOCS, iterator.nextDoc());
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
      doc.add(
          new KnnFloatVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w2)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          assertNotEquals(NO_MORE_DOCS, iterator.nextDoc());
          assertEquals(0, vectorValues.vectorValue(iterator.index())[0], 0);
          assertEquals(NO_MORE_DOCS, iterator.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory01() throws Exception {
    String fieldName = "field";
    float[] vector = new float[2];
    Document doc = new Document();
    doc.add(new KnnFloatVectorField(fieldName, vector, VectorSimilarityFunction.DOT_PRODUCT));
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        vector[0] = 1;
        vector[1] = 1;
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w2)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          assertEquals(0, iterator.nextDoc());
          // The merge order is randomized, we might get 0 first, or 1
          float value = vectorValues.vectorValue(0)[0];
          assertTrue(value == 0 || value == 1);
          assertEquals(1, iterator.nextDoc());
          value += vectorValues.vectorValue(1)[0];
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
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[6], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        IllegalArgumentException expected =
            expectThrows(
                IllegalArgumentException.class, () -> w2.addIndexes(new Directory[] {dir}));
        assertEquals(
            "cannot change field \"f\" from vector dimension=6, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
                + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testIllegalSimilarityFunctionChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        IllegalArgumentException expected =
            expectThrows(IllegalArgumentException.class, () -> w2.addIndexes(dir));
        assertEquals(
            "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=EUCLIDEAN "
                + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
            expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[6], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(
                  IllegalArgumentException.class,
                  () -> w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)}));
          assertEquals(
              "cannot change field \"f\" from vector dimension=6, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
                  + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
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
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(
                  IllegalArgumentException.class,
                  () -> w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)}));
          assertEquals(
              "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=EUCLIDEAN "
                  + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
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
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[6], VectorSimilarityFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(IllegalArgumentException.class, () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals(
              "cannot change field \"f\" from vector dimension=6, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT "
                  + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
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
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected =
              expectThrows(IllegalArgumentException.class, () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals(
              "cannot change field \"f\" from vector dimension=4, vector encoding=FLOAT32, vector similarity function=EUCLIDEAN "
                  + "to inconsistent vector dimension=4, vector encoding=FLOAT32, vector similarity function=DOT_PRODUCT",
              expected.getMessage());
        }
      }
    }
  }

  public void testIllegalMultipleValues() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
      doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
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
      doc.add(
          new KnnFloatVectorField(
              "f",
              new float[getVectorsMaxDimensions("f") + 1],
              VectorSimilarityFunction.DOT_PRODUCT));
      Exception exc = expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
      assertTrue(
          exc.getMessage()
              .contains("vector's dimensions must be <= [" + getVectorsMaxDimensions("f") + "]"));

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[2], VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc2);

      Document doc3 = new Document();
      doc3.add(
          new KnnFloatVectorField(
              "f",
              new float[getVectorsMaxDimensions("f") + 1],
              VectorSimilarityFunction.DOT_PRODUCT));
      exc = expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc3));
      assertTrue(
          exc.getMessage()
                  .contains("Inconsistency of field data structures across documents for field [f]")
              || exc.getMessage()
                  .contains(
                      "vector's dimensions must be <= [" + getVectorsMaxDimensions("f") + "]"));
      w.flush();

      Document doc4 = new Document();
      doc4.add(
          new KnnFloatVectorField(
              "f",
              new float[getVectorsMaxDimensions("f") + 1],
              VectorSimilarityFunction.DOT_PRODUCT));
      exc = expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc4));
      assertTrue(
          exc.getMessage()
              .contains("vector's dimensions must be <= [" + getVectorsMaxDimensions("f") + "]"));
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
                      new KnnFloatVectorField(
                          "f", new float[0], VectorSimilarityFunction.EUCLIDEAN)));
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField("f", new float[2], VectorSimilarityFunction.EUCLIDEAN));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  // Write vectors, one segment with SimpleText, another with default codec, then forceMerge
  public void testDifferentCodecs2() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("SimpleText"));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidKnnVectorFieldUsage() {
    KnnFloatVectorField field =
        new KnnFloatVectorField("field", new float[2], VectorSimilarityFunction.EUCLIDEAN);

    expectThrows(IllegalArgumentException.class, () -> field.setIntValue(14));

    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[1]));

    assertNull(field.numericValue());
  }

  public void testDeleteAllVectorDocs() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(
          new KnnFloatVectorField(
              "v", new float[] {2, 3, 5, 6}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();

      try (DirectoryReader r = DirectoryReader.open(w)) {
        FloatVectorValues values = getOnlyLeafReader(r).getFloatVectorValues("v");
        assertNotNull(values);
        assertEquals(1, values.size());
      }
      w.deleteDocuments(new Term("id", "0"));
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(r);
        FloatVectorValues values = leafReader.getFloatVectorValues("v");
        assertNotNull(values);
        assertEquals(0, values.size());

        // assert that knn search doesn't fail on a field with all deleted docs
        TopDocs results =
            leafReader.searchNearestVectors(
                "v", randomNormalizedVector(4), 1, leafReader.getLiveDocs(), Integer.MAX_VALUE);
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
          new KnnFloatVectorField(
              "v0", new float[] {2, 3, 5, 6}, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(
          new KnnFloatVectorField(
              "v1", new float[] {2, 3, 5, 6}, VectorSimilarityFunction.DOT_PRODUCT));
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
    VectorSimilarityFunction[] fieldSimilarityFunctions = new VectorSimilarityFunction[numFields];
    VectorEncoding[] fieldVectorEncodings = new VectorEncoding[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      if (fieldDims[i] % 2 != 0) {
        fieldDims[i]++;
      }
      fieldSimilarityFunctions[i] = randomSimilarity();
      fieldVectorEncodings[i] = randomVectorEncoding();
    }
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            switch (fieldVectorEncodings[field]) {
              case BYTE -> {
                byte[] b = randomVector8(fieldDims[field]);
                doc.add(new KnnByteVectorField(fieldName, b, fieldSimilarityFunctions[field]));
                fieldTotals[field] += b[0];
              }
              case FLOAT32 -> {
                float[] v = randomNormalizedVector(fieldDims[field]);
                doc.add(new KnnFloatVectorField(fieldName, v, fieldSimilarityFunctions[field]));
                fieldTotals[field] += v[0];
              }
            }
            fieldDocCounts[field]++;
          }
        }
        w.addDocument(doc);
      }
      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          double checksum = 0;
          String fieldName = "int" + field;
          switch (fieldVectorEncodings[field]) {
            case BYTE -> {
              for (LeafReaderContext ctx : r.leaves()) {
                ByteVectorValues byteVectorValues = ctx.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                  docCount += byteVectorValues.size();
                  KnnVectorValues.DocIndexIterator iterator = byteVectorValues.iterator();
                  while (true) {
                    if (!(iterator.nextDoc() != NO_MORE_DOCS)) break;
                    checksum += byteVectorValues.vectorValue(iterator.index())[0];
                  }
                }
              }
            }
            case FLOAT32 -> {
              for (LeafReaderContext ctx : r.leaves()) {
                FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
                if (vectorValues != null) {
                  docCount += vectorValues.size();
                  KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                  while (true) {
                    if (!(iterator.nextDoc() != NO_MORE_DOCS)) break;
                    checksum += vectorValues.vectorValue(iterator.index())[0];
                  }
                }
              }
            }
          }
          assertEquals(fieldDocCounts[field], docCount);
          // Account for quantization done when indexing fields w/BYTE encoding
          double delta = fieldVectorEncodings[field] == VectorEncoding.BYTE ? numDocs * 0.01 : 1e-5;
          assertEquals(fieldTotals[field], checksum, delta);
        }
      }
    }
  }

  public void testFloatVectorScorerIteration() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    if (random().nextBoolean()) {
      iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = randomNormalizedVector(dimension);
        }
        add(iw, fieldName, i, values[i], similarityFunction);
        if (random().nextInt(10) == 2) {
          iw.deleteDocuments(new Term("id", Integer.toString(random().nextInt(i + 1))));
        }
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      float[] vectorToScore = randomNormalizedVector(dimension);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          if (vectorValues.size() == 0) {
            assertNull(vectorValues.scorer(vectorToScore));
            continue;
          }
          VectorScorer scorer = vectorValues.scorer(vectorToScore);
          assertNotNull(scorer);
          DocIdSetIterator iterator = scorer.iterator();
          assertSame(iterator, scorer.iterator());
          assertNotSame(iterator, scorer);
          // verify scorer iteration scores are valid & iteration with vectorValues is consistent
          KnnVectorValues.DocIndexIterator valuesIterator = vectorValues.iterator();
          while (iterator.nextDoc() != NO_MORE_DOCS) {
            if (!(valuesIterator.nextDoc() != NO_MORE_DOCS)) break;
            float score = scorer.score();
            assertTrue(score >= 0f);
            assertEquals(iterator.docID(), valuesIterator.docID());
          }
          // verify that a new scorer can be obtained after iteration
          VectorScorer newScorer = vectorValues.scorer(vectorToScore);
          assertNotNull(newScorer);
          assertNotSame(scorer, newScorer);
          assertNotSame(iterator, newScorer.iterator());
        }
      }
    }
  }

  public void testByteVectorScorerIteration() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    if (random().nextBoolean()) {
      iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      byte[][] values = new byte[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = randomVector8(dimension);
        }
        add(iw, fieldName, i, values[i], similarityFunction);
        if (random().nextInt(10) == 2) {
          iw.deleteDocuments(new Term("id", Integer.toString(random().nextInt(i + 1))));
        }
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      byte[] vectorToScore = randomVector8(dimension);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          ByteVectorValues vectorValues = ctx.reader().getByteVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          if (vectorValues.size() == 0) {
            assertNull(vectorValues.scorer(vectorToScore));
            continue;
          }
          VectorScorer scorer = vectorValues.scorer(vectorToScore);
          assertNotNull(scorer);
          DocIdSetIterator iterator = scorer.iterator();
          assertSame(iterator, scorer.iterator());
          assertNotSame(iterator, scorer);
          // verify scorer iteration scores are valid & iteration with vectorValues is consistent
          KnnVectorValues.DocIndexIterator valuesIterator = vectorValues.iterator();
          while (iterator.nextDoc() != NO_MORE_DOCS) {
            if (!(valuesIterator.nextDoc() != NO_MORE_DOCS)) break;
            float score = scorer.score();
            assertTrue(score >= 0f);
            assertEquals(iterator.docID(), valuesIterator.docID());
          }
          // verify that a new scorer can be obtained after iteration
          VectorScorer newScorer = vectorValues.scorer(vectorToScore);
          assertNotNull(newScorer);
          assertNotSame(scorer, newScorer);
          assertNotSame(iterator, newScorer.iterator());
        }
      }
    }
  }

  public void testEmptyFloatVectorData() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      var doc1 = new Document();
      doc1.add(new StringField("id", "0", Field.Store.NO));
      doc1.add(new KnnFloatVectorField("v", new float[] {2, 3, 5, 6}, DOT_PRODUCT));
      w.addDocument(doc1);

      var doc2 = new Document();
      doc2.add(new StringField("id", "1", Field.Store.NO));
      w.addDocument(doc2);

      w.deleteDocuments(new Term("id", Integer.toString(0)));
      w.commit();
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        FloatVectorValues values = r.getFloatVectorValues("v");
        assertNotNull(values);
        assertEquals(0, values.size());
        assertNull(values.scorer(new float[] {2, 3, 5, 6}));
      }
    }
  }

  public void testEmptyByteVectorData() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      var doc1 = new Document();
      doc1.add(new StringField("id", "0", Field.Store.NO));
      doc1.add(new KnnByteVectorField("v", new byte[] {2, 3, 5, 6}, DOT_PRODUCT));
      w.addDocument(doc1);

      var doc2 = new Document();
      doc2.add(new StringField("id", "1", Field.Store.NO));
      w.addDocument(doc2);

      w.deleteDocuments(new Term("id", Integer.toString(0)));
      w.commit();
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        ByteVectorValues values = r.getByteVectorValues("v");
        assertNotNull(values);
        assertEquals(0, values.size());
        assertNull(values.scorer(new byte[] {2, 3, 5, 6}));
      }
    }
  }

  protected VectorSimilarityFunction randomSimilarity() {
    return VectorSimilarityFunction.values()[
        random().nextInt(VectorSimilarityFunction.values().length)];
  }

  /**
   * This method is overrideable since old codec versions only support {@link
   * VectorEncoding#FLOAT32}.
   */
  protected VectorEncoding randomVectorEncoding() {
    return VectorEncoding.values()[random().nextInt(VectorEncoding.values().length)];
  }

  public void testIndexedValueNotAliased() throws Exception {
    // We copy indexed values (as for BinaryDocValues) so the input float[] can be reused across
    // calls to IndexWriter.addDocument.
    String fieldName = "field";
    float[] v = {0, 0};
    try (Directory dir = newDirectory();
        IndexWriter iw =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setMaxBufferedDocs(3)
                    .setRAMBufferSizeMB(-1))) {
      Document doc1 = new Document();
      doc1.add(new KnnFloatVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      v[0] = 1;
      Document doc2 = new Document();
      doc2.add(new KnnFloatVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc1);
      iw.addDocument(doc2);
      v[0] = 2;
      Document doc3 = new Document();
      doc3.add(new KnnFloatVectorField(fieldName, v, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc3);
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader r = getOnlyLeafReader(reader);
        FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
        assertEquals(3, vectorValues.size());
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        iterator.nextDoc();
        assertEquals(0, iterator.index());
        assertEquals(1, vectorValues.vectorValue(0)[0], 0);
        iterator.nextDoc();
        assertEquals(1, iterator.index());
        assertEquals(1, vectorValues.vectorValue(1)[0], 0);
        iterator.nextDoc();
        assertEquals(2, iterator.index());
        assertEquals(2, vectorValues.vectorValue(2)[0], 0);
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
      add(iw, fieldName, 3, 3, (float[]) null);
      add(iw, fieldName, 2, 2, new float[] {1, 0});
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        StoredFields storedFields = leaf.storedFields();
        FloatVectorValues vectorValues = leaf.getFloatVectorValues(fieldName);
        assertEquals(2, vectorValues.dimension());
        assertEquals(3, vectorValues.size());
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        assertEquals("1", storedFields.document(iterator.nextDoc()).get("id"));
        assertEquals(-1f, vectorValues.vectorValue(0)[0], 0);
        assertEquals("2", storedFields.document(iterator.nextDoc()).get("id"));
        assertEquals(1, vectorValues.vectorValue(1)[0], 0);
        assertEquals("4", storedFields.document(iterator.nextDoc()).get("id"));
        assertEquals(0, vectorValues.vectorValue(2)[0], 0);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
      }
    }
  }

  public void testSortedIndexBytes() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      add(iw, fieldName, 1, 1, new byte[] {-1, 0});
      add(iw, fieldName, 4, 4, new byte[] {0, 1});
      add(iw, fieldName, 3, 3, (byte[]) null);
      add(iw, fieldName, 2, 2, new byte[] {1, 0});
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        StoredFields storedFields = leaf.storedFields();
        ByteVectorValues vectorValues = leaf.getByteVectorValues(fieldName);
        assertEquals(2, vectorValues.dimension());
        assertEquals(3, vectorValues.size());
        assertEquals("1", storedFields.document(vectorValues.iterator().nextDoc()).get("id"));
        assertEquals(-1, vectorValues.vectorValue(0)[0], 0);
        assertEquals("2", storedFields.document(vectorValues.iterator().nextDoc()).get("id"));
        assertEquals(1, vectorValues.vectorValue(1)[0], 0);
        assertEquals("4", storedFields.document(vectorValues.iterator().nextDoc()).get("id"));
        assertEquals(0, vectorValues.vectorValue(2)[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues.iterator().nextDoc());
      }
    }
  }

  public void testIndexMultipleKnnVectorFields() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw =
            new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
      Document doc = new Document();
      float[] v = new float[] {1, 2};
      doc.add(new KnnFloatVectorField("field1", v, VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnFloatVectorField(
              "field2", new float[] {1, 2, 3, 4}, VectorSimilarityFunction.EUCLIDEAN));
      iw.addDocument(doc);
      v[0] = 2;
      iw.addDocument(doc);
      doc = new Document();
      doc.add(
          new KnnFloatVectorField(
              "field3", new float[] {1, 2, 3, 4}, VectorSimilarityFunction.DOT_PRODUCT));
      iw.addDocument(doc);
      iw.forceMerge(1);
      try (IndexReader reader = DirectoryReader.open(iw)) {
        LeafReader leaf = reader.leaves().get(0).reader();

        FloatVectorValues vectorValues = leaf.getFloatVectorValues("field1");
        assertEquals(2, vectorValues.dimension());
        assertEquals(2, vectorValues.size());
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        iterator.nextDoc();
        assertEquals(1f, vectorValues.vectorValue(0)[0], 0);
        iterator.nextDoc();
        assertEquals(2f, vectorValues.vectorValue(1)[0], 0);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());

        FloatVectorValues vectorValues2 = leaf.getFloatVectorValues("field2");
        KnnVectorValues.DocIndexIterator it2 = vectorValues2.iterator();
        assertEquals(4, vectorValues2.dimension());
        assertEquals(2, vectorValues2.size());
        it2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue(0)[1], 0);
        it2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue(1)[1], 0);
        assertEquals(NO_MORE_DOCS, it2.nextDoc());

        FloatVectorValues vectorValues3 = leaf.getFloatVectorValues("field3");
        assertEquals(4, vectorValues3.dimension());
        assertEquals(1, vectorValues3.size());
        KnnVectorValues.DocIndexIterator it3 = vectorValues3.iterator();
        it3.nextDoc();
        assertEquals(1f, vectorValues3.vectorValue(0)[0], 0.1);
        assertEquals(NO_MORE_DOCS, it3.nextDoc());
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
      if (dimension % 2 != 0) {
        dimension++;
      }
      float[] scratch = new float[dimension];
      int numValues = 0;
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = randomNormalizedVector(dimension);
          ++numValues;
        }
        if (random().nextBoolean() && values[i] != null) {
          // sometimes use a shared scratch array
          System.arraycopy(values[i], 0, scratch, 0, scratch.length);
          add(iw, fieldName, i, scratch, similarityFunction);
        } else {
          add(iw, fieldName, i, values[i], similarityFunction);
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
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          totalSize += vectorValues.size();
          StoredFields storedFields = ctx.reader().storedFields();
          int docId;
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          while (true) {
            if (!((docId = iterator.nextDoc()) != NO_MORE_DOCS)) break;
            float[] v = vectorValues.vectorValue(iterator.index());
            assertEquals(dimension, v.length);
            String idString = storedFields.document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(docId)) {
              assertArrayEquals(idString + " " + docId, values[id], v, 0);
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
   * Index random vectors as bytes, sometimes skipping documents, sometimes deleting a document,
   * sometimes merging, sometimes sorting the index, and verify that the expected values can be read
   * back consistently.
   */
  public void testRandomBytes() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    if (random().nextBoolean()) {
      iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      byte[] scratch = new byte[dimension];
      int numValues = 0;
      BytesRef[] values = new BytesRef[numDoc];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = new BytesRef(randomVector8(dimension));
          ++numValues;
        }
        if (random().nextBoolean() && values[i] != null) {
          // sometimes use a shared scratch array
          System.arraycopy(values[i].bytes, 0, scratch, 0, dimension);
          add(iw, fieldName, i, scratch, similarityFunction);
        } else {
          BytesRef value = values[i];
          add(iw, fieldName, i, value == null ? null : value.bytes, similarityFunction);
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
          ByteVectorValues vectorValues = ctx.reader().getByteVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          totalSize += vectorValues.size();
          StoredFields storedFields = ctx.reader().storedFields();
          int docId;
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          while (true) {
            if (!((docId = iterator.nextDoc()) != NO_MORE_DOCS)) break;
            byte[] v = vectorValues.vectorValue(iterator.index());
            assertEquals(dimension, v.length);
            String idString = storedFields.document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(docId)) {
              assertEquals(idString, 0, values[id].compareTo(new BytesRef(v)));
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
          value = randomNormalizedVector(dimension);
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
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }

          // check the limit is hit when it's very small
          int k = 5 + random().nextInt(45);
          int visitedLimit = k + random().nextInt(5);
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, liveDocs, visitedLimit);
          assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, results.totalHits.relation());
          assertEquals(visitedLimit, results.totalHits.value());

          // check the limit is not hit when it clearly exceeds the number of vectors
          k = vectorValues.size();
          visitedLimit = k + 30;
          results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, liveDocs, visitedLimit);
          assertEquals(TotalHits.Relation.EQUAL_TO, results.totalHits.relation());
          assertTrue(results.totalHits.value() <= visitedLimit);
        }
      }
    }
  }

  /**
   * Index random vectors, sometimes skipping documents, sometimes updating a document, sometimes
   * merging, sometimes sorting the index, using an HNSW similarity function to also produce a
   * graph, and verify that the expected values can be read back consistently.
   */
  public void testRandomWithUpdatesAndGraph() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      float[][] id2value = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        int id = random().nextInt(numDoc);
        float[] value;
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          value = randomNormalizedVector(dimension);
        } else {
          value = null;
        }
        id2value[id] = value;
        add(iw, fieldName, id, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          StoredFields storedFields = ctx.reader().storedFields();
          int docId;
          int numLiveDocsWithVectors = 0;
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          while (true) {
            if (!((docId = iterator.nextDoc()) != NO_MORE_DOCS)) break;
            float[] v = vectorValues.vectorValue(iterator.index());
            assertEquals(dimension, v.length);
            String idString = storedFields.document(docId).getField("id").stringValue();
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
          int size = ctx.reader().getFloatVectorValues(fieldName).size();
          int k = random().nextInt(size / 10 + 1) + 1;
          if (k > numLiveDocsWithVectors) {
            k = numLiveDocsWithVectors;
          }
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, liveDocs, Integer.MAX_VALUE);
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

  private void add(
      IndexWriter iw, String field, int id, byte[] vector, VectorSimilarityFunction similarity)
      throws IOException {
    add(iw, field, id, random().nextInt(100), vector, similarity);
  }

  private void add(IndexWriter iw, String field, int id, int sortKey, byte[] vector)
      throws IOException {
    add(iw, field, id, sortKey, vector, VectorSimilarityFunction.EUCLIDEAN);
  }

  private void add(
      IndexWriter iw,
      String field,
      int id,
      int sortKey,
      byte[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new KnnByteVectorField(field, vector, similarityFunction));
    }
    doc.add(new NumericDocValuesField("sortkey", sortKey));
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    Term idTerm = new Term("id", idString);
    iw.updateDocument(idTerm, doc);
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
      doc.add(new KnnFloatVectorField(field, vector, similarityFunction));
    }
    doc.add(new NumericDocValuesField("sortkey", sortkey));
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    Term idTerm = new Term("id", idString);
    iw.updateDocument(idTerm, doc);
  }

  public static float[] randomVector(int dim) {
    assert dim > 0;
    float[] v = new float[dim];
    double squareSum = 0.0;
    // keep generating until we don't get a zero-length vector
    while (squareSum == 0.0) {
      squareSum = 0.0;
      for (int i = 0; i < dim; i++) {
        v[i] = random().nextFloat();
        squareSum += v[i] * v[i];
      }
    }
    return v;
  }

  public static float[] randomNormalizedVector(int dim) {
    float[] v = randomVector(dim);
    VectorUtil.l2normalize(v);
    return v;
  }

  public static byte[] randomVector8(int dim) {
    assert dim > 0;
    float[] v = randomNormalizedVector(dim);
    byte[] b = new byte[dim];
    for (int i = 0; i < dim; i++) {
      b[i] = (byte) (v[i] * 127);
    }
    return b;
  }

  public void testCheckIndexIncludesVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "v1", randomNormalizedVector(4), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);

        doc.add(
            new KnnFloatVectorField(
                "v2", randomNormalizedVector(4), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status =
          TestUtil.checkIndex(
              dir, CheckIndex.Level.MIN_LEVEL_FOR_INTEGRITY_CHECKS, true, true, output);
      assertEquals(1, status.segmentInfos.size());
      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
      // total 3 vector values were indexed:
      assertEquals(3, segStatus.vectorValuesStatus.totalVectorValues);
      // ... across 2 fields:
      assertEquals(2, segStatus.vectorValuesStatus.totalKnnVectorFields);

      // Make sure CheckIndex in fact declares that it is testing vectors!
      assertTrue(output.toString(UTF_8).contains("test: vectors..."));
    }
  }

  public void testSimilarityFunctionIdentifiers() {
    // make sure we don't accidentally mess up similarity function identifiers by re-ordering their
    // enumerators
    assertEquals(0, VectorSimilarityFunction.EUCLIDEAN.ordinal());
    assertEquals(1, VectorSimilarityFunction.DOT_PRODUCT.ordinal());
    assertEquals(2, VectorSimilarityFunction.COSINE.ordinal());
    assertEquals(3, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT.ordinal());
    assertEquals(4, VectorSimilarityFunction.values().length);
  }

  public void testVectorEncodingOrdinals() {
    // make sure we don't accidentally mess up vector encoding identifiers by re-ordering their
    // enumerators
    assertEquals(0, VectorEncoding.BYTE.ordinal());
    assertEquals(1, VectorEncoding.FLOAT32.ordinal());
    assertEquals(2, VectorEncoding.values().length);
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
                new KnnFloatVectorField(
                    fieldName, new float[4], VectorSimilarityFunction.EUCLIDEAN));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
          int[] vectorDocs = new int[vectorValues.size() + 1];
          int cur = -1;
          KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
          while (++cur < vectorValues.size() + 1) {
            vectorDocs[cur] = iterator.nextDoc();
            if (cur != 0) {
              assertTrue(vectorDocs[cur] > vectorDocs[cur - 1]);
            }
          }
          vectorValues = r.getFloatVectorValues(fieldName);
          DocIdSetIterator iter = vectorValues.iterator();
          cur = -1;
          for (int i = 0; i < numdocs; i++) {
            // randomly advance to i
            if (random().nextInt(4) == 3) {
              while (vectorDocs[++cur] < i) {}
              assertEquals(vectorDocs[cur], iter.advance(i));
              assertEquals(vectorDocs[cur], iter.docID());
              if (iter.docID() == NO_MORE_DOCS) {
                break;
              }
              // make i equal to docid so that it is greater than docId in the next loop iteration
              i = iter.docID();
            }
          }
        }
      }
    }
  }

  public void testVectorValuesReportCorrectDocs() throws Exception {
    final int numDocs = atLeast(1000);
    int dim = random().nextInt(20) + 1;
    if (dim % 2 != 0) {
      dim++;
    }
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
          switch (vectorEncoding) {
            case BYTE -> {
              byte[] b = randomVector8(dim);
              fieldValuesCheckSum += b[0];
              doc.add(new KnnByteVectorField("knn_vector", b, similarityFunction));
            }
            case FLOAT32 -> {
              float[] v = randomNormalizedVector(dim);
              fieldValuesCheckSum += v[0];
              doc.add(new KnnFloatVectorField("knn_vector", v, similarityFunction));
            }
          }
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
        long sumOrdToDocIds = 0;
        switch (vectorEncoding) {
          case BYTE -> {
            for (LeafReaderContext ctx : r.leaves()) {
              ByteVectorValues byteVectorValues = ctx.reader().getByteVectorValues("knn_vector");
              if (byteVectorValues != null) {
                docCount += byteVectorValues.size();
                StoredFields storedFields = ctx.reader().storedFields();
                KnnVectorValues.DocIndexIterator iter = byteVectorValues.iterator();
                for (iter.nextDoc(); iter.docID() != NO_MORE_DOCS; iter.nextDoc()) {
                  int ord = iter.index();
                  checksum += byteVectorValues.vectorValue(ord)[0];
                  Document doc = storedFields.document(iter.docID(), Set.of("id"));
                  sumDocIds += Integer.parseInt(doc.get("id"));
                }
                for (int ord = 0; ord < byteVectorValues.size(); ord++) {
                  Document doc =
                      storedFields.document(byteVectorValues.ordToDoc(ord), Set.of("id"));
                  sumOrdToDocIds += Integer.parseInt(doc.get("id"));
                }
              }
            }
          }
          case FLOAT32 -> {
            for (LeafReaderContext ctx : r.leaves()) {
              FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues("knn_vector");
              if (vectorValues != null) {
                docCount += vectorValues.size();
                StoredFields storedFields = ctx.reader().storedFields();
                KnnVectorValues.DocIndexIterator iter = vectorValues.iterator();
                for (iter.nextDoc(); iter.docID() != NO_MORE_DOCS; iter.nextDoc()) {
                  int ord = iter.index();
                  checksum += vectorValues.vectorValue(ord)[0];
                  Document doc = storedFields.document(iter.docID(), Set.of("id"));
                  sumDocIds += Integer.parseInt(doc.get("id"));
                }
                for (int ord = 0; ord < vectorValues.size(); ord++) {
                  Document doc = storedFields.document(vectorValues.ordToDoc(ord), Set.of("id"));
                  sumOrdToDocIds += Integer.parseInt(doc.get("id"));
                }
              }
            }
          }
        }
        assertEquals(
            "encoding=" + vectorEncoding,
            fieldValuesCheckSum,
            checksum,
            vectorEncoding == VectorEncoding.BYTE ? numDocs * 0.2 : 1e-5);
        assertEquals(fieldDocCount, docCount);
        assertEquals(fieldSumDocIDs, sumDocIds);
        assertEquals(fieldSumDocIDs, sumOrdToDocIds);
      }
    }
  }

  public void testMismatchedFields() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new KnnFloatVectorField("float", new float[] {1f, 2f}));
    doc.add(new KnnByteVectorField("byte", new byte[] {42}));
    w1.addDocument(doc);

    Directory dir2 = newDirectory();
    IndexWriter w2 =
        new IndexWriter(dir2, newIndexWriterConfig().setMergeScheduler(new SerialMergeScheduler()));
    w2.addDocument(doc);
    w2.commit();

    DirectoryReader reader = DirectoryReader.open(w1);
    w1.close();
    w2.addIndexes(new MismatchedCodecReader((CodecReader) getOnlyLeafReader(reader), random()));
    reader.close();
    w2.forceMerge(1);
    reader = DirectoryReader.open(w2);
    w2.close();

    LeafReader leafReader = getOnlyLeafReader(reader);

    ByteVectorValues byteVectors = leafReader.getByteVectorValues("byte");
    assertNotNull(byteVectors);
    KnnVectorValues.DocIndexIterator iter = byteVectors.iterator();
    assertEquals(0, iter.nextDoc());
    assertArrayEquals(new byte[] {42}, byteVectors.vectorValue(0));
    assertEquals(1, iter.nextDoc());
    assertArrayEquals(new byte[] {42}, byteVectors.vectorValue(1));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());

    FloatVectorValues floatVectors = leafReader.getFloatVectorValues("float");
    assertNotNull(floatVectors);
    iter = floatVectors.iterator();
    assertEquals(0, iter.nextDoc());
    float[] vector = floatVectors.vectorValue(0);
    assertEquals(2, vector.length);
    assertEquals(1f, vector[0], 0f);
    assertEquals(2f, vector[1], 0f);
    assertEquals(1, iter.nextDoc());
    vector = floatVectors.vectorValue(1);
    assertEquals(2, vector.length);
    assertEquals(1f, vector[0], 0f);
    assertEquals(2f, vector[1], 0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());

    IOUtils.close(reader, w2, dir1, dir2);
  }

  /**
   * Test that the query is a viable approximation to exact search. This test is designed to uncover
   * gross failures only, not to represent the true expected recall.
   */
  public void testRecall() throws IOException {
    VectorSimilarityFunction[] functions = {
      VectorSimilarityFunction.EUCLIDEAN,
      VectorSimilarityFunction.COSINE,
      VectorSimilarityFunction.DOT_PRODUCT,
      VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
    };
    for (VectorSimilarityFunction similarity : functions) {
      assertRecall(similarity, 0.5, 1.0);
    }
  }

  protected void assertRecall(VectorSimilarityFunction similarity, double min, double max)
      throws IOException {
    int dim = 16;
    int recalled = 0;
    try (Directory indexStore = getKnownIndexStore("field", dim, similarity);
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      float[] queryEmbedding = new float[dim];
      // indexed 421 lines from LICENSE.txt
      // indexed 157 lines from NOTICE.txt
      int topK = 10;
      int numQueries = 526;
      String[] testQueries = {
        "Apache Lucene",
        "Apache License",
        "TERMS AND CONDITIONS",
        "Copyright 2001",
        "Permission is hereby",
        "Copyright © 2003",
        "The dictionary comes from Morfologik project",
        "The levenshtein automata tables"
      };
      for (String queryString : testQueries) {
        computeLineEmbedding(queryString, queryEmbedding);

        // pass match-all "filter" to force full traversal, bypassing graph
        KnnFloatVectorQuery exactQuery =
            new KnnFloatVectorQuery("field", queryEmbedding, 1000, new MatchAllDocsQuery());
        assertEquals(numQueries, searcher.count(exactQuery)); // Same for exact search

        KnnFloatVectorQuery query = new KnnFloatVectorQuery("field", queryEmbedding, topK);
        assertEquals(10, searcher.count(query)); // Expect some results without timeout
        TopDocs results = searcher.search(query, topK);
        Set<Integer> resultDocs = new HashSet<>();
        int i = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
          if (VERBOSE) {
            System.out.println(
                "result "
                    + i++
                    + ": "
                    + reader.storedFields().document(scoreDoc.doc)
                    + " "
                    + scoreDoc);
          }
          resultDocs.add(scoreDoc.doc);
        }
        TopDocs expected = searcher.search(exactQuery, topK);
        i = 0;
        for (ScoreDoc scoreDoc : expected.scoreDocs) {
          if (VERBOSE) {
            System.out.println(
                "expected "
                    + i++
                    + ": "
                    + reader.storedFields().document(scoreDoc.doc)
                    + " "
                    + scoreDoc);
          }
          if (resultDocs.contains(scoreDoc.doc)) {
            ++recalled;
          }
        }
      }
      int totalResults = testQueries.length * topK;
      assertTrue(
          "Average recall for "
              + similarity
              + " should be at least "
              + (totalResults * min)
              + " / "
              + totalResults
              + ", got "
              + recalled,
          recalled >= (int) (totalResults * min));
      assertTrue(
          "Average recall for "
              + similarity
              + " should be no more than "
              + (totalResults * max)
              + " / "
              + totalResults
              + ", got "
              + recalled,
          recalled <= (int) (totalResults * max));
    }
  }

  /** Creates a new directory and adds documents with the given vectors as kNN vector fields */
  Directory getKnownIndexStore(
      String field, int dimension, VectorSimilarityFunction vectorSimilarityFunction)
      throws IOException {
    Directory indexStore = newDirectory(random());
    IndexWriter writer = new IndexWriter(indexStore, newIndexWriterConfig());
    float[] scratch = new float[dimension];
    Set<String> seen = new HashSet<>(578);
    for (String file : List.of("LICENSE.txt", "NOTICE.txt")) {
      try (InputStream in = BaseKnnVectorsFormatTestCase.class.getResourceAsStream(file);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8))) {
        String line;
        int lineNo = -1;
        while ((line = reader.readLine()) != null) {
          line = line.strip();
          if (line.isEmpty()) {
            continue;
          }
          if (seen.add(line) == false) {
            continue;
          }
          ++lineNo;
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField(
                  field, computeLineEmbedding(line, scratch), vectorSimilarityFunction));
          doc.add(new StoredField("text", line));
          doc.add(new StringField("id", file + "." + lineNo, Field.Store.YES));
          writer.addDocument(doc);
          if (random().nextBoolean()) {
            // Add some documents without a vector
            addDocuments(writer, "id" + lineNo + ".", randomIntBetween(1, 5));
          }
        }
        // System.out.println("indexed " + (lineNo + 1) + " lines from " + file);
      }
    }
    // Add some documents without a vector nor an id
    addDocuments(writer, null, 5);
    writer.close();
    return indexStore;
  }

  private float[] computeLineEmbedding(String line, float[] vector) {
    Arrays.fill(vector, 0);
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      vector[i % vector.length] += c / ((float) (i + 1) / vector.length);
    }
    VectorUtil.l2normalize(vector, false);
    return vector;
  }

  private void addDocuments(IndexWriter writer, String idBase, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      Document doc = new Document();
      doc.add(new StringField("other", "value", Field.Store.NO));
      if (idBase != null) {
        doc.add(new StringField("id", idBase + i, Field.Store.YES));
      }
      writer.addDocument(doc);
    }
  }
}
