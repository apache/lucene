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
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.automaton.Operations;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiTermConstantScore extends TestBaseRangeFilter {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 1e-6f;

  public static final Set<MultiTermQuery.RewriteMethod> CONSTANT_SCORE_REWRITES =
      Set.of(MultiTermQuery.CONSTANT_SCORE_REWRITE, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);

  static Directory small;
  static IndexReader reader;

  public static void assertEquals(String m, int e, int a) {
    Assert.assertEquals(m, e, a);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    String[] data =
        new String[] {
          "A 1 2 3 4 5 6",
          "Z       4 5 6",
          null,
          "B   2   4 5 6",
          "Y     3   5 6",
          null,
          "C     3     6",
          "X       4 5 6"
        };

    small = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            small,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                .setMergePolicy(newLogMergePolicy()));

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    for (int i = 0; i < data.length; i++) {
      Document doc = new Document();
      doc.add(
          newField("id", String.valueOf(i), customType)); // Field.Keyword("id",String.valueOf(i)));
      doc.add(newField("all", "all", customType)); // Field.Keyword("all","all"));
      if (null != data[i]) {
        doc.add(newTextField("data", data[i], Field.Store.YES)); // Field.Text("data",data[i]));
      }
      writer.addDocument(doc);
    }

    reader = writer.getReader();
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    small.close();
    reader = null;
    small = null;
  }

  /**
   * macro for readability
   *
   * @deprecated please use {@link #cspq(Term, MultiTermQuery.RewriteMethod)} instead
   */
  @Deprecated
  public static Query csrq(String f, String l, String h, boolean il, boolean ih) {
    TermRangeQuery query =
        TermRangeQuery.newStringRange(f, l, h, il, ih, MultiTermQuery.CONSTANT_SCORE_REWRITE);
    if (VERBOSE) {
      System.out.println("TEST: query=" + query);
    }
    return query;
  }

  /** macro for readability */
  public static Query csrq(
      String f, String l, String h, boolean il, boolean ih, MultiTermQuery.RewriteMethod method) {
    TermRangeQuery query = TermRangeQuery.newStringRange(f, l, h, il, ih, method);
    if (VERBOSE) {
      System.out.println("TEST: query=" + query + " method=" + method);
    }
    return query;
  }

  /**
   * macro for readability
   *
   * @deprecated please use {@link #cspq(Term, MultiTermQuery.RewriteMethod)} instead
   */
  @Deprecated
  public static Query cspq(Term prefix) {
    return new PrefixQuery(prefix, MultiTermQuery.CONSTANT_SCORE_REWRITE);
  }

  /** macro for readability */
  public static Query cspq(Term prefix, MultiTermQuery.RewriteMethod method) {
    return new PrefixQuery(prefix, method);
  }

  /**
   * macro for readability
   *
   * @deprecated please use {@link #cswcq(Term, MultiTermQuery.RewriteMethod)} instead
   */
  @Deprecated
  public static Query cswcq(Term wild) {
    return new WildcardQuery(
        wild, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.CONSTANT_SCORE_REWRITE);
  }

  /** macro for readability */
  public static Query cswcq(Term wild, MultiTermQuery.RewriteMethod method) {
    return new WildcardQuery(wild, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, method);
  }

  @Test
  public void testBasics() throws IOException {
    for (MultiTermQuery.RewriteMethod rw : CONSTANT_SCORE_REWRITES) {
      QueryUtils.check(csrq("data", "1", "6", T, T, rw));
      QueryUtils.check(csrq("data", "A", "Z", T, T, rw));
      QueryUtils.checkUnequal(csrq("data", "1", "6", T, T, rw), csrq("data", "A", "Z", T, T, rw));

      QueryUtils.check(cspq(new Term("data", "p*u?"), rw));
      QueryUtils.checkUnequal(
          cspq(new Term("data", "pre*"), rw), cspq(new Term("data", "pres*"), rw));

      QueryUtils.check(cswcq(new Term("data", "p"), rw));
      QueryUtils.checkUnequal(
          cswcq(new Term("data", "pre*n?t"), rw), cswcq(new Term("data", "pr*t?j"), rw));
    }
  }

  @Test
  public void testEqualScores() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    ScoreDoc[] result;

    // some hits match more terms then others, score should be the same

    result =
        search.search(
                csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE), 1000)
            .scoreDocs;
    int numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    float score = result[0].score;
    for (int i = 1; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }

    result =
        search.search(
                csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE), 1000)
            .scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }

    result =
        search.search(csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), 1000)
            .scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }
  }

  @Test // Test for LUCENE-5245: Empty MTQ rewrites should have a consistent norm, so always need to
  // return a CSQ!
  public void testEqualScoresWhenNoHits() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    ScoreDoc[] result;

    TermQuery dummyTerm = new TermQuery(new Term("data", "1"));

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(
        csrq("data", "#", "#", T, T, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE),
        BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    int numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    float score = result[0].score;
    for (int i = 1; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }

    bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(
        csrq("data", "#", "#", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE),
        BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }

    bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(
        csrq("data", "#", "#", T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE),
        BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals(
          "score for " + i + " was not the same", score, result[i].score, SCORE_COMP_THRESH);
    }
  }

  @Test
  public void testBooleanOrderUnAffected() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    for (MultiTermQuery.RewriteMethod rw : CONSTANT_SCORE_REWRITES) {

      // first do a regular TermRangeQuery which uses term expansion so
      // docs with more terms in range get higher scores

      Query rq = TermRangeQuery.newStringRange("data", "1", "4", T, T, rw);

      ScoreDoc[] expected = search.search(rq, 1000).scoreDocs;
      int numHits = expected.length;

      // now do a boolean where which also contains a
      // ConstantScoreRangeQuery and make sure the order is the same

      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.add(rq, BooleanClause.Occur.MUST); // T, F);
      q.add(csrq("data", "1", "6", T, T, rw), BooleanClause.Occur.MUST); // T, F);

      ScoreDoc[] actual = search.search(q.build(), 1000).scoreDocs;

      assertEquals("wrong number of hits", numHits, actual.length);
      for (int i = 0; i < numHits; i++) {
        assertEquals("mismatch in docid for hit#" + i, expected[i].doc, actual[i].doc);
      }
    }
  }

  @Test
  public void testRangeQueryId() throws IOException {
    // NOTE: uses index build in *super* setUp

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    if (VERBOSE) {
      System.out.println("TEST: reader=" + reader);
    }

    int medId = ((maxId - minId) / 2);

    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);

    int numDocs = reader.numDocs();

    assertEquals("num of docs", numDocs, 1 + maxId - minId);

    ScoreDoc[] result;

    for (MultiTermQuery.RewriteMethod rw : CONSTANT_SCORE_REWRITES) {

      // test id, bounded on both ends

      result = search.search(csrq("id", minIP, maxIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("find all", numDocs, result.length);

      result = search.search(csrq("id", minIP, maxIP, T, F, rw), numDocs).scoreDocs;
      assertEquals("all but last", numDocs - 1, result.length);

      result = search.search(csrq("id", minIP, maxIP, F, T, rw), numDocs).scoreDocs;
      assertEquals("all but first", numDocs - 1, result.length);

      result = search.search(csrq("id", minIP, maxIP, F, F, rw), numDocs).scoreDocs;
      assertEquals("all but ends", numDocs - 2, result.length);

      result = search.search(csrq("id", medIP, maxIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("med and up", 1 + maxId - medId, result.length);

      result = search.search(csrq("id", minIP, medIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("up to med", 1 + medId - minId, result.length);

      // unbounded id

      result = search.search(csrq("id", minIP, null, T, F, rw), numDocs).scoreDocs;
      assertEquals("min and up", numDocs, result.length);

      result = search.search(csrq("id", null, maxIP, F, T, rw), numDocs).scoreDocs;
      assertEquals("max and down", numDocs, result.length);

      result = search.search(csrq("id", minIP, null, F, F, rw), numDocs).scoreDocs;
      assertEquals("not min, but up", numDocs - 1, result.length);

      result = search.search(csrq("id", null, maxIP, F, F, rw), numDocs).scoreDocs;
      assertEquals("not max, but down", numDocs - 1, result.length);

      result = search.search(csrq("id", medIP, maxIP, T, F, rw), numDocs).scoreDocs;
      assertEquals("med and up, not max", maxId - medId, result.length);

      result = search.search(csrq("id", minIP, medIP, F, T, rw), numDocs).scoreDocs;
      assertEquals("not min, up to med", medId - minId, result.length);

      // very small sets

      result = search.search(csrq("id", minIP, minIP, F, F, rw), numDocs).scoreDocs;
      assertEquals("min,min,F,F", 0, result.length);

      result = search.search(csrq("id", medIP, medIP, F, F, rw), numDocs).scoreDocs;
      assertEquals("med,med,F,F", 0, result.length);

      result = search.search(csrq("id", maxIP, maxIP, F, F, rw), numDocs).scoreDocs;
      assertEquals("max,max,F,F", 0, result.length);

      result = search.search(csrq("id", minIP, minIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("min,min,T,T", 1, result.length);

      result = search.search(csrq("id", null, minIP, F, T, rw), numDocs).scoreDocs;
      assertEquals("nul,min,F,T", 1, result.length);

      result = search.search(csrq("id", maxIP, maxIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("max,max,T,T", 1, result.length);

      result = search.search(csrq("id", maxIP, null, T, F, rw), numDocs).scoreDocs;
      assertEquals("max,nul,T,T", 1, result.length);

      result = search.search(csrq("id", medIP, medIP, T, T, rw), numDocs).scoreDocs;
      assertEquals("med,med,T,T", 1, result.length);
    }
  }

  @Test
  public void testRangeQueryRand() throws IOException {
    // NOTE: uses index build in *super* setUp

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    String minRP = pad(signedIndexDir.minR);
    String maxRP = pad(signedIndexDir.maxR);

    int numDocs = reader.numDocs();

    assertEquals("num of docs", numDocs, 1 + maxId - minId);

    ScoreDoc[] result;

    for (MultiTermQuery.RewriteMethod rw : CONSTANT_SCORE_REWRITES) {

      // test extremes, bounded on both ends

      result = search.search(csrq("rand", minRP, maxRP, T, T, rw), numDocs).scoreDocs;
      assertEquals("find all", numDocs, result.length);

      result = search.search(csrq("rand", minRP, maxRP, T, F, rw), numDocs).scoreDocs;
      assertEquals("all but biggest", numDocs - 1, result.length);

      result = search.search(csrq("rand", minRP, maxRP, F, T, rw), numDocs).scoreDocs;
      assertEquals("all but smallest", numDocs - 1, result.length);

      result = search.search(csrq("rand", minRP, maxRP, F, F, rw), numDocs).scoreDocs;
      assertEquals("all but extremes", numDocs - 2, result.length);

      // unbounded

      result = search.search(csrq("rand", minRP, null, T, F, rw), numDocs).scoreDocs;
      assertEquals("smallest and up", numDocs, result.length);

      result = search.search(csrq("rand", null, maxRP, F, T, rw), numDocs).scoreDocs;
      assertEquals("biggest and down", numDocs, result.length);

      result = search.search(csrq("rand", minRP, null, F, F, rw), numDocs).scoreDocs;
      assertEquals("not smallest, but up", numDocs - 1, result.length);

      result = search.search(csrq("rand", null, maxRP, F, F, rw), numDocs).scoreDocs;
      assertEquals("not biggest, but down", numDocs - 1, result.length);

      // very small sets

      result = search.search(csrq("rand", minRP, minRP, F, F, rw), numDocs).scoreDocs;
      assertEquals("min,min,F,F", 0, result.length);
      result = search.search(csrq("rand", maxRP, maxRP, F, F, rw), numDocs).scoreDocs;
      assertEquals("max,max,F,F", 0, result.length);

      result = search.search(csrq("rand", minRP, minRP, T, T, rw), numDocs).scoreDocs;
      assertEquals("min,min,T,T", 1, result.length);
      result = search.search(csrq("rand", null, minRP, F, T, rw), numDocs).scoreDocs;
      assertEquals("nul,min,F,T", 1, result.length);

      result = search.search(csrq("rand", maxRP, maxRP, T, T, rw), numDocs).scoreDocs;
      assertEquals("max,max,T,T", 1, result.length);
      result = search.search(csrq("rand", maxRP, null, T, F, rw), numDocs).scoreDocs;
      assertEquals("max,nul,T,T", 1, result.length);
    }
  }
}
