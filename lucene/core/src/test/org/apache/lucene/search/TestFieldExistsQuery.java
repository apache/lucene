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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;

public class TestFieldExistsQuery extends LuceneTestCase {

  public void testDocValuesRewriteWithTermsPresent() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new DoubleDocValuesField("f", 2.0));
      doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
      iw.addDocument(doc);
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    iw.close();

    assertTrue((new FieldExistsQuery("f")).rewrite(reader) instanceof MatchAllDocsQuery);
    reader.close();
    dir.close();
  }

  public void testDocValuesRewriteWithPointValuesPresent() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
      doc.add(new DoubleDocValuesField("dim", 2.0));
      iw.addDocument(doc);
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    iw.close();

    assertTrue(new FieldExistsQuery("dim").rewrite(reader) instanceof MatchAllDocsQuery);
    reader.close();
    dir.close();
  }

  public void testDocValuesNoRewrite() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new DoubleDocValuesField("dim", 2.0));
      doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
      iw.addDocument(doc);
    }
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new DoubleDocValuesField("f", 2.0));
      doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
      iw.addDocument(doc);
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    iw.close();

    assertFalse((new FieldExistsQuery("dim")).rewrite(reader) instanceof MatchAllDocsQuery);
    assertFalse((new FieldExistsQuery("f")).rewrite(reader) instanceof MatchAllDocsQuery);
    reader.close();
    dir.close();
  }

  public void testDocValuesNoRewriteWithDocValues() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("dv1", 1));
      doc.add(new SortedNumericDocValuesField("dv2", 1));
      doc.add(new SortedNumericDocValuesField("dv2", 2));
      iw.addDocument(doc);
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    iw.close();

    assertFalse((new FieldExistsQuery("dv1")).rewrite(reader) instanceof MatchAllDocsQuery);
    assertFalse((new FieldExistsQuery("dv2")).rewrite(reader) instanceof MatchAllDocsQuery);
    assertFalse((new FieldExistsQuery("dv3")).rewrite(reader) instanceof MatchAllDocsQuery);
    reader.close();
    dir.close();
  }

  public void testDocValuesRandom() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new NumericDocValuesField("dv1", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 2));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      assertSameMatches(
          searcher,
          new TermQuery(new Term("has_value", "yes")),
          new FieldExistsQuery("dv1"),
          false);
      assertSameMatches(
          searcher,
          new TermQuery(new Term("has_value", "yes")),
          new FieldExistsQuery("dv2"),
          false);

      reader.close();
      dir.close();
    }
  }

  public void testDocValuesApproximation() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new NumericDocValuesField("dv1", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 2));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      BooleanQuery.Builder ref = new BooleanQuery.Builder();
      ref.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      ref.add(new TermQuery(new Term("has_value", "yes")), Occur.FILTER);

      BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
      bq1.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      bq1.add(new FieldExistsQuery("dv1"), Occur.FILTER);
      assertSameMatches(searcher, ref.build(), bq1.build(), true);

      BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
      bq2.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      bq2.add(new FieldExistsQuery("dv2"), Occur.FILTER);
      assertSameMatches(searcher, ref.build(), bq2.build(), true);

      reader.close();
      dir.close();
    }
  }

  public void testDocValuesScore() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new NumericDocValuesField("dv1", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 1));
          doc.add(new SortedNumericDocValuesField("dv2", 2));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      final float boost = random().nextFloat() * 10;
      final Query ref =
          new BoostQuery(
              new ConstantScoreQuery(new TermQuery(new Term("has_value", "yes"))), boost);

      final Query q1 = new BoostQuery(new FieldExistsQuery("dv1"), boost);
      assertSameMatches(searcher, ref, q1, true);

      final Query q2 = new BoostQuery(new FieldExistsQuery("dv2"), boost);
      assertSameMatches(searcher, ref, q2, true);

      reader.close();
      dir.close();
    }
  }

  public void testDocValuesMissingField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(0, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testDocValuesAllDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("f", 1));
    iw.addDocument(doc);
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testDocValuesFieldExistsButNoDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    // 1st segment has the field, but 2nd one does not
    Document doc = new Document();
    doc.add(new NumericDocValuesField("f", 1));
    iw.addDocument(doc);
    iw.commit();
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testDocValuesQueryMatchesCount() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int randomNumDocs = TestUtil.nextInt(random(), 11, 100);
    int numMatchingDocs = 0;

    for (int i = 0; i < randomNumDocs; i++) {
      Document doc = new Document();
      // We select most documents randomly but keep two documents:
      //  * #0 ensures we will delete at least one document (with long between 0 and 9)
      //  * #10 ensures we will keep at least one document (with long greater than 9)
      if (i == 0 || i == 10 || random().nextBoolean()) {
        doc.add(new LongPoint("long", i));
        doc.add(new NumericDocValuesField("long", i));
        doc.add(new StringField("string", "value", Store.NO));
        doc.add(new SortedDocValuesField("string", new BytesRef("value")));
        numMatchingDocs++;
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    assertSameCount(reader, searcher, "long", numMatchingDocs);
    assertSameCount(reader, searcher, "string", numMatchingDocs);
    assertSameCount(reader, searcher, "doesNotExist", 0);

    // Test that we can't count in O(1) when there are deleted documents
    w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    w.deleteDocuments(LongPoint.newRangeQuery("long", 0L, 9L));
    DirectoryReader reader2 = w.getReader();
    final IndexSearcher searcher2 = new IndexSearcher(reader2);
    final Query testQuery = new FieldExistsQuery("long");
    final Weight weight2 = searcher2.createWeight(testQuery, ScoreMode.COMPLETE, 1);
    assertEquals(weight2.count(reader2.leaves().get(0)), -1);

    IOUtils.close(reader, reader2, w, dir);
  }

  public void testNormsRandom() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      assertSameMatches(
          searcher,
          new TermQuery(new Term("has_value", "yes")),
          new FieldExistsQuery("text1"),
          false);

      reader.close();
      dir.close();
    }
  }

  public void testNormsApproximation() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      BooleanQuery.Builder ref = new BooleanQuery.Builder();
      ref.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      ref.add(new TermQuery(new Term("has_value", "yes")), Occur.FILTER);

      BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
      bq1.add(new TermQuery(new Term("f", "yes")), Occur.MUST);
      bq1.add(new FieldExistsQuery("text1"), Occur.FILTER);
      assertSameMatches(searcher, ref.build(), bq1.build(), true);

      reader.close();
      dir.close();
    }
  }

  public void testNormsScore() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final boolean hasValue = random().nextBoolean();
        if (hasValue) {
          doc.add(new TextField("text1", "value", Store.NO));
          doc.add(new StringField("has_value", "yes", Store.NO));
        }
        doc.add(new StringField("f", random().nextBoolean() ? "yes" : "no", Store.NO));
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term("f", "no")));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      final float boost = random().nextFloat() * 10;
      final Query ref =
          new BoostQuery(
              new ConstantScoreQuery(new TermQuery(new Term("has_value", "yes"))), boost);

      final Query q1 = new BoostQuery(new FieldExistsQuery("text1"), boost);
      assertSameMatches(searcher, ref, q1, true);

      reader.close();
      dir.close();
    }
  }

  public void testNormsMissingField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(0, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testNormsAllDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new TextField("f", "value", Store.NO));
    iw.addDocument(doc);
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testNormsFieldExistsButNoDocsHaveField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    // 1st segment has the field, but 2nd one does not
    Document doc = new Document();
    doc.add(new TextField("f", "value", Store.NO));
    iw.addDocument(doc);
    iw.commit();
    iw.addDocument(new Document());
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();
    assertEquals(1, searcher.count(new FieldExistsQuery("f")));
    reader.close();
    dir.close();
  }

  public void testNormsQueryMatchesCount() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int randomNumDocs = TestUtil.nextInt(random(), 10, 100);

    FieldType noNormsFieldType = new FieldType();
    noNormsFieldType.setOmitNorms(true);
    noNormsFieldType.setIndexOptions(IndexOptions.DOCS);

    Document doc = new Document();
    doc.add(new TextField("text", "always here", Store.NO));
    doc.add(new TextField("text_s", "", Store.NO));
    doc.add(new Field("text_n", "always here", noNormsFieldType));
    w.addDocument(doc);

    for (int i = 1; i < randomNumDocs; i++) {
      doc.clear();
      doc.add(new TextField("text", "some text", Store.NO));
      doc.add(new TextField("text_s", "some text", Store.NO));
      doc.add(new Field("text_n", "some here", noNormsFieldType));
      w.addDocument(doc);
    }
    w.forceMerge(1);

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    assertNormsCountWithShortcut(searcher, "text", randomNumDocs);
    assertNormsCountWithShortcut(searcher, "doesNotExist", 0);
    expectThrows(IllegalStateException.class, () -> searcher.count(new FieldExistsQuery("text_n")));

    // docs that have a text field that analyzes to an empty token
    // stream still have a recorded norm value but don't show up in
    // Reader.getDocCount(field), so we can't use the shortcut for
    // these fields
    assertNormsCountWithoutShortcut(searcher, "text_s", randomNumDocs);

    // We can still shortcut with deleted docs
    w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    w.deleteDocuments(new Term("text", "text")); // deletes all but the first doc
    DirectoryReader reader2 = w.getReader();
    final IndexSearcher searcher2 = new IndexSearcher(reader2);
    assertNormsCountWithShortcut(searcher2, "text", 1);

    IOUtils.close(reader, reader2, w, dir);
  }

  private void assertNormsCountWithoutShortcut(
      IndexSearcher searcher, String field, int expectedCount) throws IOException {
    final Query q = new FieldExistsQuery(field);
    final Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE, 1);
    assertEquals(-1, weight.count(searcher.reader.leaves().get(0)));
    assertEquals(expectedCount, searcher.count(q));
  }

  private void assertNormsCountWithShortcut(
      IndexSearcher searcher, String field, int numMatchingDocs) throws IOException {
    final Query testQuery = new FieldExistsQuery(field);
    assertEquals(numMatchingDocs, searcher.count(testQuery));
    final Weight weight = searcher.createWeight(testQuery, ScoreMode.COMPLETE, 1);
    assertEquals(numMatchingDocs, weight.count(searcher.reader.leaves().get(0)));
  }

  public void testKnnVectorRandom() throws IOException {
    int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      try (Directory dir = newDirectory();
          RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
        int numDocs = atLeast(100);
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          boolean hasValue = random().nextBoolean();
          if (hasValue) {
            doc.add(new KnnFloatVectorField("vector", randomVector(5)));
            doc.add(new StringField("has_value", "yes", Store.NO));
          }
          doc.add(new StringField("field", "value", Store.NO));
          iw.addDocument(doc);
        }
        if (random().nextBoolean()) {
          iw.deleteDocuments(new TermQuery(new Term("f", "no")));
        }
        iw.commit();

        try (IndexReader reader = iw.getReader()) {
          IndexSearcher searcher = newSearcher(reader);

          assertSameMatches(
              searcher,
              new TermQuery(new Term("has_value", "yes")),
              new FieldExistsQuery("vector"),
              false);

          float boost = random().nextFloat() * 10;
          assertSameMatches(
              searcher,
              new BoostQuery(
                  new ConstantScoreQuery(new TermQuery(new Term("has_value", "yes"))), boost),
              new BoostQuery(new FieldExistsQuery("vector"), boost),
              true);
        }
      }
    }
  }

  public void testKnnVectorMissingField() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      iw.addDocument(new Document());
      iw.commit();
      try (IndexReader reader = iw.getReader()) {
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(0, searcher.count(new FieldExistsQuery("f")));
      }
    }
  }

  public void testKnnVectorAllDocsHaveField() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 100; ++i) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("vector", randomVector(5)));
        iw.addDocument(doc);
      }
      iw.commit();

      try (IndexReader reader = iw.getReader()) {
        IndexSearcher searcher = newSearcher(reader);
        Query query = new FieldExistsQuery("vector");
        assertTrue(searcher.rewrite(query) instanceof MatchAllDocsQuery);
        assertEquals(100, searcher.count(query));
      }
    }
  }

  public void testKnnVectorConjunction() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      int numDocs = atLeast(100);
      int numVectors = 0;

      boolean allDocsHaveVector = random().nextBoolean();
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        if (allDocsHaveVector || random().nextBoolean()) {
          doc.add(new KnnFloatVectorField("vector", randomVector(5)));
          numVectors++;
        }
        doc.add(new StringField("field", "value" + (i % 2), Store.NO));
        iw.addDocument(doc);
      }
      try (IndexReader reader = iw.getReader()) {
        IndexSearcher searcher = newSearcher(reader);
        Occur occur = random().nextBoolean() ? Occur.MUST : Occur.FILTER;
        BooleanQuery booleanQuery =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field", "value1")), occur)
                .add(new FieldExistsQuery("vector"), Occur.FILTER)
                .build();

        int count = searcher.count(booleanQuery);
        assertTrue(count <= numVectors);
        if (allDocsHaveVector) {
          assertEquals(numDocs / 2, count);
        }
      }
    }
  }

  public void testKnnVectorFieldExistsButNoDocsHaveField() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      // 1st segment has the field, but 2nd one does not
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("vector", randomVector(3)));
      iw.addDocument(doc);
      iw.commit();
      iw.addDocument(new Document());
      iw.commit();
      try (IndexReader reader = iw.getReader()) {
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(1, searcher.count(new FieldExistsQuery("vector")));
      }
    }
  }

  public void testOldIndices() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setIndexCreatedVersionMajor(8)
              .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        Document d1 = new Document();
        d1.add(new StringField("my_field", "text", Store.YES));
        d1.add(new BinaryDocValuesField("my_field", new BytesRef("first")));
        writer.addDocument(d1);
        writer.flush();

        Document d2 = new Document();
        d2.add(new StringField("my_field", "text", Store.YES));
        writer.addDocument(d2);
        writer.flush();

        try (IndexReader reader = DirectoryReader.open(writer)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertEquals(1, searcher.count(new FieldExistsQuery("my_field")));
        }
      }
    }
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat();
    }
    VectorUtil.l2normalize(v);
    return v;
  }

  public void testDeleteAllPointDocs() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {

      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(new LongPoint("long", 17));
      doc.add(new NumericDocValuesField("long", 17));
      iw.addDocument(doc);
      // add another document before the flush, otherwise the segment only has the document that
      // we are going to delete and the merge simply ignores the segment without carrying over its
      // field infos
      iw.addDocument(new Document());
      // make sure there are two segments or force merge will be a no-op
      iw.flush();
      iw.addDocument(new Document());
      iw.commit();

      iw.deleteDocuments(new Term("id", "0"));
      iw.forceMerge(1);

      try (IndexReader reader = iw.getReader()) {
        assertTrue(reader.leaves().size() == 1 && reader.hasDeletions() == false);
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(0, searcher.count(new FieldExistsQuery("long")));
      }
    }
  }

  public void testDeleteAllTermDocs() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {

      Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      doc.add(new StringField("str", "foo", Store.NO));
      doc.add(new SortedDocValuesField("str", new BytesRef("foo")));
      iw.addDocument(doc);
      // add another document before the flush, otherwise the segment only has the document that
      // we are going to delete and the merge simply ignores the segment without carrying over its
      // field infos
      iw.addDocument(new Document());
      // make sure there are two segments or force merge will be a no-op
      iw.flush();
      iw.addDocument(new Document());
      iw.commit();

      iw.deleteDocuments(new Term("id", "0"));
      iw.forceMerge(1);

      try (IndexReader reader = iw.getReader()) {
        assertTrue(reader.leaves().size() == 1 && reader.hasDeletions() == false);
        IndexSearcher searcher = newSearcher(reader);
        assertEquals(0, searcher.count(new FieldExistsQuery("str")));
      }
    }
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores)
      throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits.value, td2.totalHits.value);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  private void assertSameCount(
      IndexReader reader, IndexSearcher searcher, String field, int numMatchingDocs)
      throws IOException {
    final Query testQuery = new FieldExistsQuery(field);
    assertEquals(searcher.count(testQuery), numMatchingDocs);
    final Weight weight = searcher.createWeight(testQuery, ScoreMode.COMPLETE, 1);
    assertEquals(weight.count(reader.leaves().get(0)), numMatchingDocs);
  }
}
