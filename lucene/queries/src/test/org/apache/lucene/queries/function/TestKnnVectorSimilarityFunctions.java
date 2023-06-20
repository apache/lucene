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
package org.apache.lucene.queries.function;

import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ByteKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.FloatKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKnnVectorSimilarityFunctions extends LuceneTestCase {
  static Directory dir;
  static Analyzer analyzer;
  static IndexReader reader;
  static IndexSearcher searcher;
  static final List<String> documents = List.of("1", "2");

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwConfig = newIndexWriterConfig(analyzer);
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);

    Document document = new Document();
    document.add(new StringField("id", "1", Field.Store.NO));
    document.add(new SortedDocValuesField("id", new BytesRef("1")));
    document.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
    document.add(new KnnFloatVectorField("knnFloatField2", new float[] {5.2f, 3.2f, 3.1f}));

    // add only to the first document
    document.add(new KnnFloatVectorField("knnFloatField3", new float[] {1.0f, 1.0f, 1.0f}));
    document.add(new KnnByteVectorField("knnByteField3", new byte[] {1, 1, 1}));

    document.add(new KnnByteVectorField("knnByteField1", new byte[] {1, 2, 3}));
    document.add(new KnnByteVectorField("knnByteField2", new byte[] {4, 2, 3}));
    iw.addDocument(document);

    Document document2 = new Document();
    document2.add(new StringField("id", "2", Field.Store.NO));
    document2.add(new SortedDocValuesField("id", new BytesRef("2")));
    document2.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
    document2.add(new KnnFloatVectorField("knnFloatField2", new float[] {5.2f, 3.2f, 3.1f}));

    document2.add(new KnnByteVectorField("knnByteField1", new byte[] {1, 2, 3}));
    document2.add(new KnnByteVectorField("knnByteField2", new byte[] {4, 2, 3}));
    iw.addDocument(document2);

    reader = iw.getReader();
    searcher = newSearcher(reader);
    iw.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    analyzer.close();
    analyzer = null;
  }

  @Test
  public void vectorSimilarity_floatConstantVectors_shouldReturnFloatSimilarity() throws Exception {
    var v1 = new ConstKnnFloatValueSource(new float[] {1, 2, 3});
    var v2 = new ConstKnnFloatValueSource(new float[] {5, 4, 1});
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.04f, 0.04f});
  }

  @Test
  public void vectorSimilarity_byteConstantVectors_shouldReturnFloatSimilarity() throws Exception {
    var v1 = new ConstKnnByteVectorValueSource(new byte[] {1, 2, 3});
    var v2 = new ConstKnnByteVectorValueSource(new byte[] {2, 5, 6});
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.05f, 0.05f});
  }

  @Test
  public void vectorSimilarity_floatFieldVectors_shouldReturnFloatSimilarity() throws Exception {
    var v1 = new FloatKnnVectorFieldSource("knnFloatField1");
    var v2 = new FloatKnnVectorFieldSource("knnFloatField2");
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.049776014f, 0.049776014f});
  }

  @Test
  public void vectorSimilarity_byteFieldVectors_shouldReturnFloatSimilarity() throws Exception {
    var v1 = new ByteKnnVectorFieldSource("knnByteField1");
    var v2 = new ByteKnnVectorFieldSource("knnByteField2");
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.1f, 0.1f});
  }

  @Test
  public void vectorSimilarity_FloatConstAndFloatFieldVectors_shouldReturnFloatSimilarity()
      throws Exception {
    var v1 = new ConstKnnFloatValueSource(new float[] {1, 2, 4});
    var v2 = new FloatKnnVectorFieldSource("knnFloatField1");
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.5f});
  }

  @Test
  public void vectorSimilarity_ByteConstAndByteFieldVectors_shouldReturnFloatSimilarity()
      throws Exception {
    var v1 = new ConstKnnByteVectorValueSource(new byte[] {1, 2, 4});
    var v2 = new ByteKnnVectorFieldSource("knnByteField1");
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.5f});
  }

  @Test
  public void vectorSimilarity_missingFloatVectorField_shouldReturnZero() throws Exception {
    var v1 = new ConstKnnFloatValueSource(new float[] {2.f, 1.f, 1.f});
    var v2 = new FloatKnnVectorFieldSource("knnFloatField3");
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.f});
  }

  @Test
  public void vectorSimilarity_missingByteVectorField_shouldReturnZero() throws Exception {
    var v1 = new ConstKnnByteVectorValueSource(new byte[] {2, 1, 1});
    var v2 = new ByteKnnVectorFieldSource("knnByteField3");
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.f});
  }

  @Test
  public void vectorSimilarity_twoVectorsWithDifferentDimensions_shouldRaiseException() {
    ValueSource v1 = new ConstKnnByteVectorValueSource(new byte[] {1, 2, 3, 4});
    ValueSource v2 = new ByteKnnVectorFieldSource("knnByteField1");
    ByteVectorSimilarityFunction byteDenseVectorSimilarityFunction =
        new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        AssertionError.class,
        () -> searcher.search(new FunctionQuery(byteDenseVectorSimilarityFunction), 10));

    v1 = new ConstKnnFloatValueSource(new float[] {1.f, 2.f});
    v2 = new FloatKnnVectorFieldSource("knnFloatField1");
    FloatVectorSimilarityFunction floatDenseVectorSimilarityFunction =
        new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        AssertionError.class,
        () -> searcher.search(new FunctionQuery(floatDenseVectorSimilarityFunction), 10));
  }

  @Test
  public void vectorSimilarity_byteAndFloatVectors_shouldRaiseException() {
    var v1 = new ConstKnnByteVectorValueSource(new byte[] {1, 2, 3});
    ValueSource v2 = new ByteKnnVectorFieldSource("knnByteField1");
    FloatVectorSimilarityFunction floatDenseVectorSimilarityFunction =
        new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        UnsupportedOperationException.class,
        () -> searcher.search(new FunctionQuery(floatDenseVectorSimilarityFunction), 10));

    v1 = new ConstKnnByteVectorValueSource(new byte[] {1, 2, 3});
    v2 = new FloatKnnVectorFieldSource("knnFloatField1");
    ByteVectorSimilarityFunction byteDenseVectorSimilarityFunction =
        new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        UnsupportedOperationException.class,
        () -> searcher.search(new FunctionQuery(byteDenseVectorSimilarityFunction), 10));
  }

  @Test
  public void vectorSimilarity_wrongFieldType_shouldRaiseException() {
    ValueSource v1 = new ByteKnnVectorFieldSource("knnByteField1");
    ValueSource v2 = new ByteKnnVectorFieldSource("knnFloatField2");
    ByteVectorSimilarityFunction byteDenseVectorSimilarityFunction =
        new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);

    assertThrows(
        IllegalArgumentException.class,
        () -> searcher.search(new FunctionQuery(byteDenseVectorSimilarityFunction), 10));

    v1 = new FloatKnnVectorFieldSource("knnByteField1");
    v2 = new FloatKnnVectorFieldSource("knnFloatField2");
    FloatVectorSimilarityFunction floatVectorSimilarityFunction =
        new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);

    assertThrows(
        IllegalArgumentException.class,
        () -> searcher.search(new FunctionQuery(floatVectorSimilarityFunction), 10));
  }

  private static void assertHits(Query q, float[] scores) throws Exception {
    ScoreDoc[] expected = new ScoreDoc[scores.length];
    int[] expectedDocs = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs =
        searcher.search(
            q, documents.size(), new Sort(new SortField("id", SortField.Type.STRING)), true);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
  }
}
