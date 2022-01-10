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

import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.IndriDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndriAndQuery extends LuceneTestCase {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 0.0000f;

  public Similarity sim = new IndriDirichletSimilarity();
  public Directory index;
  public IndexReader r;
  public IndexSearcher s;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    index = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            index,
            newIndexWriterConfig(
                    new MockAnalyzer(
                        random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET))
                .setSimilarity(sim)
                .setMergePolicy(newLogMergePolicy()));
    {
      Document d1 = new Document();
      d1.add(newField("id", "d1", TextField.TYPE_STORED));
      d1.add(newTextField("body", "President Washington", Field.Store.YES));
      writer.addDocument(d1);
    }

    {
      Document d2 = new Document();
      d2.add(newField("id", "d2", TextField.TYPE_STORED));
      d2.add(newTextField("body", "President Lincoln", Field.Store.YES));
      writer.addDocument(d2);
    }

    {
      Document d3 = new Document();
      d3.add(newField("id", "d3", TextField.TYPE_STORED));
      d3.add(newTextField("body", "George Washington", Field.Store.YES));
      writer.addDocument(d3);
    }

    {
      Document d4 = new Document();
      d4.add(newField("id", "d4", TextField.TYPE_STORED));
      d4.add(newTextField("body", "President Jefferson", Field.Store.YES));
      writer.addDocument(d4);
    }

    writer.forceMerge(1);
    r = getOnlyLeafReader(writer.getReader());
    writer.close();
    s = new IndexSearcher(r);
    s.setSimilarity(sim);
  }

  @Override
  public void tearDown() throws Exception {
    r.close();
    index.close();
    super.tearDown();
  }

  public void testSimpleQuery1() throws Exception {

    BooleanClause clause1 = new BooleanClause(tq("body", "george"), Occur.SHOULD);
    BooleanClause clause2 = new BooleanClause(tq("body", "washington"), Occur.SHOULD);

    IndriAndQuery q = new IndriAndQuery(Arrays.asList(clause1, clause2));

    ScoreDoc[] h = s.search(q, 1000).scoreDocs;

    try {
      assertEquals("2 docs should match " + q.toString(), 2, h.length);
    } catch (Error e) {
      printHits("testSimpleEqualScores1", h, s);
      throw e;
    }
  }

  public void testSimpleQuery2() throws Exception {

    BooleanClause clause1 = new BooleanClause(tq("body", "president"), Occur.SHOULD);
    BooleanClause clause2 = new BooleanClause(tq("body", "washington"), Occur.SHOULD);

    IndriAndQuery q = new IndriAndQuery(Arrays.asList(clause1, clause2));

    ScoreDoc[] h = s.search(q, 1000).scoreDocs;

    try {
      assertEquals("all docs should match " + q.toString(), 4, h.length);

      float score0 = h[0].score;
      float score1 = h[1].score;
      float score2 = h[2].score;
      float score3 = h[3].score;

      String doc0 = s.doc(h[0].doc).get("id");
      String doc1 = s.doc(h[1].doc).get("id");
      String doc2 = s.doc(h[2].doc).get("id");
      String doc3 = s.doc(h[3].doc).get("id");

      assertEquals("doc0 should be d1: ", "d1", doc0);
      assertEquals("doc1 should be d3: ", "d3", doc1);
      assertEquals("doc2 should be d2: ", "d2", doc2);
      assertEquals("doc3 should be d4: ", "d4", doc3);

      assertTrue(
          "d3 does not have a better score then d1: " + score0 + " >? " + score1, score0 > score1);
      assertTrue(
          "d4 does not have a better score then d3: " + score1 + " >? " + score3, score1 > score3);
      assertTrue(
          "d2 does not have a better score then d3: " + score1 + " >? " + score2, score1 > score2);
    } catch (Error e) {
      printHits("testSimpleQuery2", h, s);
      throw e;
    }
  }

  public void testBoostQuery() throws Exception {

    BooleanClause clause1 =
        new BooleanClause(new BoostQuery(tq("body", "president"), 3), Occur.SHOULD);
    BooleanClause clause2 =
        new BooleanClause(new BoostQuery(tq("body", "washington"), 1), Occur.SHOULD);

    IndriAndQuery q = new IndriAndQuery(Arrays.asList(clause1, clause2));

    ScoreDoc[] h = s.search(q, 1000).scoreDocs;

    try {
      assertEquals("all docs should match " + q.toString(), 4, h.length);

      String doc0 = s.doc(h[0].doc).get("id");
      String doc1 = s.doc(h[1].doc).get("id");
      String doc2 = s.doc(h[2].doc).get("id");
      String doc3 = s.doc(h[3].doc).get("id");

      assertEquals("doc0 should be d1: ", "d1", doc0);
      assertEquals("doc1 should be d3: ", "d2", doc1);
      assertEquals("doc2 should be d2: ", "d4", doc2);
      assertEquals("doc3 should be d4: ", "d3", doc3);

    } catch (Error e) {
      printHits("testBoostQuery", h, s);
      throw e;
    }
  }

  /** macro */
  protected Query tq(String f, String t) {
    return new TermQuery(new Term(f, t));
  }

  /** macro */
  protected Query tq(String f, String t, float b) {
    Query q = tq(f, t);
    return new BoostQuery(q, b);
  }

  protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher) throws Exception {

    System.err.println("------- " + test + " -------");

    for (int i = 0; i < h.length; i++) {
      Document d = searcher.doc(h[i].doc);
      float score = h[i].score;
      System.err.println("#" + i + ": " + score + " - " + d.get("body"));
    }
  }
}
