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
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorValueSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorFieldSource;
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
    for (String docId : documents) {
      Document document = new Document();
      document.add(new StringField("id", docId, Field.Store.NO));
      document.add(new SortedDocValuesField("id", new BytesRef(docId)));
      document.add(new KnnFloatVectorField("knnFloatField1", new float[] {1.f, 2.f, 3.f}));
      document.add(new KnnFloatVectorField("knnFloatField2", new float[] {5.2f, 3.2f, 3.1f}));
      document.add(new KnnByteVectorField("knnByteField1", new byte[] {1, 2, 3}));
      document.add(new KnnByteVectorField("knnByteField2", new byte[] {4, 2, 3}));
      iw.addDocument(document);
    }

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

  public void testFloatVectorSimilarityFunctionConst() throws Exception {
    ValueSource v1 = new FloatVectorValueSource(List.of(1, 2, 3));
    ValueSource v2 = new FloatVectorValueSource(List.of(5, 4, 1));
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.04f, 0.04f});
  }

  public void testByteVectorSimilarityFunctionConst() throws Exception {
    ValueSource v1 = new ByteVectorValueSource(List.of(1, 2, 3));
    ValueSource v2 = new ByteVectorValueSource(List.of(2, 5, 6));
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.05f, 0.05f});
  }

  public void testFloatVectorSimilarityFunctionField() throws Exception {
    ValueSource v1 = new FloatVectorFieldSource("knnFloatField1");
    ValueSource v2 = new FloatVectorFieldSource("knnFloatField2");
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.049776014f, 0.049776014f});
  }

  public void testByteVectorSimilarityFunctionField() throws Exception {
    ValueSource v1 = new ByteVectorFieldSource("knnByteField1");
    ValueSource v2 = new ByteVectorFieldSource("knnByteField2");
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.1f, 0.1f});
  }

  public void testFloatVectorSimilarityFunctionMixed() throws Exception {
    ValueSource v1 = new FloatVectorValueSource(List.of(1, 2, 4));
    ValueSource v2 = new FloatVectorFieldSource("knnFloatField1");
    assertHits(
        new FunctionQuery(
            new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.5f});
  }

  public void testByteVectorSimilarityFunctionMixed() throws Exception {
    ValueSource v1 = new ByteVectorValueSource(List.of(1, 2, 4));
    ValueSource v2 = new ByteVectorFieldSource("knnByteField1");
    assertHits(
        new FunctionQuery(
            new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2)),
        new float[] {0.5f, 0.5f});
  }

  public void testDismatchDimension() {
    ValueSource v1 = new ByteVectorValueSource(List.of(1, 2, 3, 4));
    ValueSource v2 = new ByteVectorFieldSource("knnByteField1");
    ByteVectorSimilarityFunction byteDenseVectorSimilarityFunction =
        new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
            AssertionError.class,
        () -> searcher.search(new FunctionQuery(byteDenseVectorSimilarityFunction), 10));

    v1 = new FloatVectorValueSource(List.of(1, 2));
    v2 = new FloatVectorFieldSource("knnFloatField1");
    FloatVectorSimilarityFunction floatDenseVectorSimilarityFunction =
        new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
            AssertionError.class,
        () -> searcher.search(new FunctionQuery(floatDenseVectorSimilarityFunction), 10));
  }

  public void testMismatchType() {
    ValueSource v1 = new ByteVectorValueSource(List.of(1, 2, 3));
    ValueSource v2 = new ByteVectorFieldSource("knnByteField1");
    FloatVectorSimilarityFunction floatDenseVectorSimilarityFunction =
        new FloatVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        UnsupportedOperationException.class,
        () -> searcher.search(new FunctionQuery(floatDenseVectorSimilarityFunction), 10));

    v1 = new ByteVectorValueSource(List.of(1, 2, 3));
    v2 = new FloatVectorFieldSource("knnByteField1");
    ByteVectorSimilarityFunction byteDenseVectorSimilarityFunction =
        new ByteVectorSimilarityFunction(VectorSimilarityFunction.EUCLIDEAN, v1, v2);
    assertThrows(
        UnsupportedOperationException.class,
        () -> searcher.search(new FunctionQuery(byteDenseVectorSimilarityFunction), 10));
  }

  public static void assertHits(Query q, float[] scores) throws Exception {
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
