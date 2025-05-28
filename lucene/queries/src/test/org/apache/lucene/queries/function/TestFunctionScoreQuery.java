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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LateInteractionValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFunctionScoreQuery extends FunctionTestSetup {

  static IndexReader reader;
  static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
  }

  public void testEqualities() {

    Query q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(1));
    Query q2 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(1));
    Query q3 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));
    Query q4 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));

    QueryUtils.check(q1);
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, q2);
    QueryUtils.checkUnequal(q2, q3);
    QueryUtils.checkEqual(q3, q4);

    Query bq1 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(2));
    QueryUtils.check(bq1);
    Query bq2 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq2);
    Query bq3 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq3);
    QueryUtils.checkUnequal(bq2, bq3);
    Query bq4 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkEqual(bq3, bq4);

    Query qq1 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.check(qq1);
    Query qq2 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.2f);
    QueryUtils.checkUnequal(qq1, qq2);
    Query qq3 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.checkUnequal(qq1, qq3);
    QueryUtils.checkUnequal(qq2, qq3);
    Query qq4 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "zz")),
            0.1f);
    QueryUtils.checkUnequal(qq1, qq4);
    QueryUtils.checkUnequal(qq2, qq4);
    QueryUtils.checkUnequal(qq3, qq4);
    Query qq5 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.checkEqual(qq1, qq5);
  }

  // FunctionQuery equivalent
  public void testSimpleSourceScore() throws Exception {

    FunctionScoreQuery q =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "first")),
            DoubleValuesSource.fromIntField(INT_FIELD));

    QueryUtils.check(random(), q, searcher, rarely());

    int[] expectedDocs = new int[] {4, 7, 9};
    TopDocs docs = searcher.search(q, 4);
    assertEquals(expectedDocs.length, docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(docs.scoreDocs[i].doc, expectedDocs[i]);
    }
  }

  // CustomScoreQuery and BoostedQuery equivalent
  public void testScoreModifyingSource() throws Exception {

    BooleanQuery bq =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD)
            .build();
    TopDocs plain = searcher.search(bq, 1);

    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByValue(bq, DoubleValuesSource.fromIntField("iii"));

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[] {4, 7, 9, 8, 12};
    TopDocs docs = searcher.search(fq, 5);
    assertEquals(plain.totalHits.value(), docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);
    }

    Explanation expl = searcher.explain(fq, 4);
    assertTrue(expl.toString().contains("first"));
    assertTrue(expl.toString().contains("iii"));
  }

  // BoostingQuery equivalent
  public void testCombiningMultipleQueryScores() throws Exception {

    TermQuery q = new TermQuery(new Term(TEXT_FIELD, "text"));
    TopDocs plain = searcher.search(q, 1);

    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByQuery(q, new TermQuery(new Term(TEXT_FIELD, "rechecking")), 100f);

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[] {6, 1, 0, 2, 8};
    TopDocs docs = searcher.search(fq, 20);
    assertEquals(plain.totalHits.value(), docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);
    }

    Explanation expl = searcher.explain(fq, 6);
    assertTrue(expl.toString().contains("rechecking"));
    assertTrue(expl.toString().contains("text"));
  }

  // check boosts with non-distributive score source
  public void testBoostsAreAppliedLast() throws Exception {

    SimpleBindings bindings = new SimpleBindings();
    bindings.add("score", DoubleValuesSource.SCORES);
    Expression expr = JavascriptCompiler.compile("ln(score + 4)");

    Query q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "text")), expr.getDoubleValuesSource(bindings));
    TopDocs plain = searcher.search(q1, 5);

    Query boosted = new BoostQuery(q1, 2);
    TopDocs afterboost = searcher.search(boosted, 5);
    assertEquals(plain.totalHits.value(), afterboost.totalHits.value());
    for (int i = 0; i < 5; i++) {
      assertEquals(plain.scoreDocs[i].doc, afterboost.scoreDocs[i].doc);
      assertEquals(plain.scoreDocs[i].score, afterboost.scoreDocs[i].score / 2, 0.0001);
    }
  }

  public void testTruncateNegativeScores() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", -2));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q =
        new FunctionScoreQuery(new MatchAllDocsQuery(), DoubleValuesSource.fromLongField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(expl.toString(), expl.getDetails()[0].getDescription().contains("truncated score"));
    reader.close();
    dir.close();
  }

  public void testNaN() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", Double.doubleToLongBits(Double.NaN)));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q =
        new FunctionScoreQuery(new MatchAllDocsQuery(), DoubleValuesSource.fromDoubleField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(
        expl.toString(), expl.getDetails()[0].getDescription().contains("NaN is an illegal score"));
    reader.close();
    dir.close();
  }

  // check access to the score source of a functionScoreQuery
  public void testAccessToValueSource() throws Exception {

    FunctionScoreQuery q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(31));
    Query q2 = new FunctionScoreQuery(q1.getWrappedQuery(), q1.getSource());
    QueryUtils.check(q2);
    QueryUtils.checkEqual(q2, q1);

    FunctionScoreQuery q3 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "first")),
            DoubleValuesSource.fromIntField(INT_FIELD));
    Query q4 = new FunctionScoreQuery(q3.getWrappedQuery(), q3.getSource());
    QueryUtils.checkEqual(q3, q4);

    SimpleBindings bindings = new SimpleBindings();
    bindings.add("score", DoubleValuesSource.SCORES);
    Expression expr = JavascriptCompiler.compile("ln(score + 4)");
    FunctionScoreQuery q5 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "text")), expr.getDoubleValuesSource(bindings));
    Query q6 = new FunctionScoreQuery(q5.getWrappedQuery(), q5.getSource());
    QueryUtils.checkEqual(q5, q6);
  }

  public void testScoreMode() throws Exception {
    // Value Source doesn't need scores
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.COMPLETE,
        DoubleValuesSource.fromDoubleField("foo"));
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.COMPLETE_NO_SCORES,
        DoubleValuesSource.fromDoubleField("foo"));
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.TOP_SCORES,
        DoubleValuesSource.fromDoubleField("foo"));

    // Value Source needs scores
    assertInnerScoreMode(ScoreMode.COMPLETE, ScoreMode.COMPLETE, DoubleValuesSource.SCORES);
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES, ScoreMode.COMPLETE_NO_SCORES, DoubleValuesSource.SCORES);
    assertInnerScoreMode(ScoreMode.COMPLETE, ScoreMode.TOP_SCORES, DoubleValuesSource.SCORES);
  }

  private void assertInnerScoreMode(
      ScoreMode expectedScoreMode, ScoreMode inputScoreMode, DoubleValuesSource valueSource)
      throws IOException {
    final AtomicReference<ScoreMode> scoreModeInWeight = new AtomicReference<ScoreMode>();
    Query innerQ =
        new TermQuery(new Term(TEXT_FIELD, "a")) {

          @Override
          public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
              throws IOException {
            scoreModeInWeight.set(scoreMode);
            return super.createWeight(searcher, scoreMode, boost);
          }
        };

    FunctionScoreQuery fq = new FunctionScoreQuery(innerQ, valueSource);
    fq.createWeight(searcher, inputScoreMode, 1f);
    assertEquals(expectedScoreMode, scoreModeInWeight.get());
  }

  /** The FunctionScoreQuery's Scorer score() is going to be called twice for the same doc. */
  public void testScoreCalledTwice() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      IndexWriter indexWriter = new IndexWriter(dir, conf);
      Document doc = new Document();
      doc.add(new TextField("ExampleText", "periodic function", Field.Store.NO));
      doc.add(new TextField("ExampleText", "plot of the original function", Field.Store.NO));
      indexWriter.addDocument(doc);
      indexWriter.commit();
      indexWriter.close();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new TermQuery(new Term("ExampleText", "function"));

        q =
            FunctionScoreQuery.boostByQuery(
                q, new PhraseQuery(1, "ExampleText", "function", "plot"), 2);
        q = FunctionScoreQuery.boostByValue(q, DoubleValuesSource.SCORES);

        assertEquals(1, new IndexSearcher(reader).search(q, 10).totalHits.value());
      }
    }
  }

  // Weight#count is delegated to the inner weight
  public void testQueryMatchesCount() throws Exception {
    TermQuery query = new TermQuery(new Term(TEXT_FIELD, "first"));
    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByValue(query, DoubleValuesSource.fromIntField("iii"));

    final int searchCount = searcher.count(fq);
    final Weight weight = searcher.createWeight(fq, ScoreMode.COMPLETE, 1);
    int weightCount = 0;
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      weightCount += weight.count(leafReaderContext);
    }
    assertEquals(searchCount, weightCount);
  }

  public void testLateInteractionQuery() throws Exception {
    final String LATE_I_FIELD = "li_vector";
    final String KNN_FIELD = "knn_vector";
    List<float[][]> corpus = new ArrayList<>();
    final int numDocs = atLeast(1000);
    final int numSegments = random().nextInt(2, 10);
    final int dim = 128;
    final VectorSimilarityFunction vectorSimilarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length)];
    LateInteractionValuesSource.ScoreFunction scoreFunction =
        LateInteractionValuesSource.ScoreFunction.values()[
            random().nextInt(LateInteractionValuesSource.ScoreFunction.values().length)];

    try (Directory dir = newDirectory()) {
      int id = 0;
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int j = 0; j < numSegments; j++) {
          for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            if (random().nextInt(100) < 30) {
              // skip value for some docs to create sparse field
              doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
            } else {
              float[][] value = createMultiVector(dim);
              corpus.add(value);
              doc.add(new IntField("id", id++, Field.Store.YES));
              doc.add(new LateInteractionField(LATE_I_FIELD, value));
              doc.add(new KnnFloatVectorField(KNN_FIELD, randomVector(dim)));
              doc.add(new IntField("has_li_vector", 1, Field.Store.YES));
            }
            w.addDocument(doc);
            w.flush();
          }
        }
        // add a segment with no vectors
        for (int i = 0; i < 100; i++) {
          Document doc = new Document();
          doc.add(new IntField("has_li_vector", 0, Field.Store.YES));
          w.addDocument(doc);
        }
        w.flush();
      }

      float[][] lateIQueryVector = createMultiVector(dim);
      float[] knnQueryVector = randomVector(dim);
      KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(KNN_FIELD, knnQueryVector, 50);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher s = new IndexSearcher(reader);
        TopDocs knnHits = s.search(knnQuery, 50);
        Set<Integer> knnHitDocs = Arrays.stream(knnHits.scoreDocs).map(k -> k.doc).collect(Collectors.toSet());
        FunctionScoreQuery lateIQuery =
            FunctionScoreQuery.lateInteractionRerankQuery(knnQuery, LATE_I_FIELD, lateIQueryVector, vectorSimilarityFunction);
        TopDocs lateIHits = s.search(lateIQuery, 10);
        StoredFields storedFields = reader.storedFields();
        for (ScoreDoc hit : lateIHits.scoreDocs) {
          assertTrue(knnHitDocs.contains(hit.doc));
          int idValue = Integer.parseInt(storedFields.document(hit.doc).get("id"));
          float[][] docVector = corpus.get(idValue);
          float expected =
              scoreFunction.compare(lateIQueryVector, docVector, vectorSimilarityFunction);
          assertEquals(expected, hit.score, 1e-5);
        }
      }
    }
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    Random random = random();
    for (int i = 0; i < dim; i++) {
      v[i] = random.nextFloat();
    }
    return v;
  }

  private float[][] createMultiVector(int dimension) {
    float[][] value = new float[random().nextInt(3, 12)][];
    for (int i = 0; i < value.length; i++) {
      value[i] = randomVector(dimension);
    }
    return value;
  }
}
