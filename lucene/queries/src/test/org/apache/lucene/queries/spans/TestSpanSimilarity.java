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

package org.apache.lucene.queries.spans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.AxiomaticF1EXP;
import org.apache.lucene.search.similarities.AxiomaticF1LOG;
import org.apache.lucene.search.similarities.AxiomaticF2EXP;
import org.apache.lucene.search.similarities.AxiomaticF2LOG;
import org.apache.lucene.search.similarities.AxiomaticF3EXP;
import org.apache.lucene.search.similarities.AxiomaticF3LOG;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Independence;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.IndependenceSaturated;
import org.apache.lucene.search.similarities.IndependenceStandardized;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSpanSimilarity extends LuceneTestCase {

  private List<Similarity> sims;

  /** The DFR basic models to test. */
  private static final BasicModel[] BASIC_MODELS = {
    new BasicModelG(), new BasicModelIF(), new BasicModelIn(), new BasicModelIne()
  };

  /** The DFR aftereffects to test. */
  private static final AfterEffect[] AFTER_EFFECTS = {new AfterEffectB(), new AfterEffectL()};

  private static final Normalization[] NORMALIZATIONS = {
    new NormalizationH1(),
    new NormalizationH2(),
    new NormalizationH3(),
    new NormalizationZ(),
    new Normalization.NoNormalization()
  };

  /** The distributions for IB. */
  private static final Distribution[] DISTRIBUTIONS = {new DistributionLL(), new DistributionSPL()};

  /** Lambdas for IB. */
  private static final Lambda[] LAMBDAS = {new LambdaDF(), new LambdaTTF()};

  /** Independence measures for DFI */
  private static final Independence[] INDEPENDENCE_MEASURES = {
    new IndependenceStandardized(), new IndependenceSaturated(), new IndependenceChiSquared()
  };

  @Override
  public void setUp() throws Exception {
    super.setUp();
    sims = new ArrayList<>();
    sims.add(new ClassicSimilarity());
    sims.add(new BM25Similarity());
    sims.add(new BooleanSimilarity());
    sims.add(new AxiomaticF1EXP());
    sims.add(new AxiomaticF1LOG());
    sims.add(new AxiomaticF2EXP());
    sims.add(new AxiomaticF2LOG());
    sims.add(new AxiomaticF3EXP(0.25f, 3));
    sims.add(new AxiomaticF3LOG(0.25f, 3));
    // TODO: not great that we dup this all with TestSimilarityBase
    for (BasicModel basicModel : BASIC_MODELS) {
      for (AfterEffect afterEffect : AFTER_EFFECTS) {
        for (Normalization normalization : NORMALIZATIONS) {
          sims.add(new DFRSimilarity(basicModel, afterEffect, normalization));
        }
      }
    }
    for (Distribution distribution : DISTRIBUTIONS) {
      for (Lambda lambda : LAMBDAS) {
        for (Normalization normalization : NORMALIZATIONS) {
          sims.add(new IBSimilarity(distribution, lambda, normalization));
        }
      }
    }
    sims.add(new LMDirichletSimilarity());
    sims.add(new LMJelinekMercerSimilarity(0.1f));
    sims.add(new LMJelinekMercerSimilarity(0.7f));
    for (Independence independence : INDEPENDENCE_MEASURES) {
      sims.add(new DFISimilarity(independence));
    }
  }

  /**
   * Verify that SpanOrQuery scores each document using only the IDF of the term that matched, not
   * the combined IDF of all clauses. With two docs (foo:bar, foo:baz) and a spanOr([foo:bar,
   * foo:baz]) query, each doc should score the same as the equivalent bool-should query (i.e.
   * single-term IDF, not summed IDF).
   */
  public void testSpanOrScoresWithPerClauseIdf() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    Document doc1 = new Document();
    doc1.add(newField("foo", "bar", ft));
    iw.addDocument(doc1);
    Document doc2 = new Document();
    doc2.add(newField("foo", "baz", ft));
    iw.addDocument(doc2);
    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher is = newSearcher(ir);
    is.setSimilarity(new BM25Similarity());

    SpanTermQuery spanBar = new SpanTermQuery(new Term("foo", "bar"));
    SpanTermQuery spanBaz = new SpanTermQuery(new Term("foo", "baz"));

    // Score each term individually to get the expected single-term score
    float scoreBar = is.search(spanBar, 10).scoreDocs[0].score;
    float scoreBaz = is.search(spanBaz, 10).scoreDocs[0].score;

    // SpanOrQuery should score each doc the same as its individual SpanTermQuery
    TopDocs spanOrResults = is.search(new SpanOrQuery(spanBar, spanBaz), 10);
    assertEquals(2, spanOrResults.totalHits.value());

    Map<Integer, Float> spanOrScores = new HashMap<>();
    for (var sd : spanOrResults.scoreDocs) {
      spanOrScores.put(sd.doc, sd.score);
    }

    int docBar = is.search(spanBar, 10).scoreDocs[0].doc;
    int docBaz = is.search(spanBaz, 10).scoreDocs[0].doc;

    assertEquals(
        "foo:bar doc score should equal single SpanTermQuery score, not combined-IDF score",
        scoreBar,
        spanOrScores.get(docBar),
        1e-5f);
    assertEquals(
        "foo:baz doc score should equal single SpanTermQuery score, not combined-IDF score",
        scoreBaz,
        spanOrScores.get(docBaz),
        1e-5f);

    ir.close();
    dir.close();
  }

  /** make sure all sims work with spanOR(termX, termY) where termY does not exist */
  public void testCrazySpans() throws Exception {
    // historically this was a problem, but sim's no longer have to score terms that dont exist
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    doc.add(newField("foo", "bar", ft));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);

    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      SpanTermQuery s1 = new SpanTermQuery(new Term("foo", "bar"));
      SpanTermQuery s2 = new SpanTermQuery(new Term("foo", "baz"));
      Query query = new SpanOrQuery(s1, s2);
      TopDocs td = is.search(query, 10);
      assertEquals(1, td.totalHits.value());
      float score = td.scoreDocs[0].score;
      assertFalse("negative score for " + sim, score < 0.0f);
      assertFalse("inf score for " + sim, Float.isInfinite(score));
      assertFalse("nan score for " + sim, Float.isNaN(score));
    }
    ir.close();
    dir.close();
  }
}
