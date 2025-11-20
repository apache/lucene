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

import static org.apache.lucene.search.BooleanClause.Occur;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestConstantScoreScorer extends LuceneTestCase {
  private static final String FIELD = "f";
  private static final String[] VALUES =
      new String[] {"foo", "bar", "foo bar", "bar foo", "foo not bar", "bar foo bar", "azerty"};

  private static final Query TERM_QUERY =
      new BooleanQuery.Builder()
          .add(new TermQuery(new Term(FIELD, "foo")), Occur.MUST)
          .add(new TermQuery(new Term(FIELD, "bar")), Occur.MUST)
          .build();
  private static final Query PHRASE_QUERY = new PhraseQuery(FIELD, "foo", "bar");

  public void testMatching_ScoreMode_COMPLETE() throws Exception {
    testMatching(ScoreMode.COMPLETE);
  }

  public void testMatching_ScoreMode_COMPLETE_NO_SCORES() throws Exception {
    testMatching(ScoreMode.COMPLETE_NO_SCORES);
  }

  private void testMatching(ScoreMode scoreMode) throws Exception {

    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      int doc;
      ConstantScoreScorer scorer = index.constantScoreScorer(TERM_QUERY, 1f, scoreMode);

      // "foo bar" match
      doc = scorer.iterator().nextDoc();
      assertEquals(2, doc);
      assertEquals(1f, scorer.score(), 0);

      // should not reset iterator
      scorer.setMinCompetitiveScore(2f);
      assertEquals(doc, scorer.docID());
      assertEquals(doc, scorer.iterator().docID());
      assertEquals(1f, scorer.score(), 0);

      // "bar foo" match
      doc = scorer.iterator().nextDoc();
      assertEquals(3, doc);
      assertEquals(1f, scorer.score(), 0);

      // "foo not bar" match
      doc = scorer.iterator().nextDoc();
      assertEquals(4, doc);
      assertEquals(1f, scorer.score(), 0);

      // "foo bar foo" match
      doc = scorer.iterator().nextDoc();
      assertEquals(5, doc);
      assertEquals(1f, scorer.score(), 0);

      doc = scorer.iterator().nextDoc();
      assertEquals(NO_MORE_DOCS, doc);
    }
  }

  public void testMatching_ScoreMode_TOP_SCORES() throws Exception {
    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      int doc;
      ConstantScoreScorer scorer = index.constantScoreScorer(TERM_QUERY, 1f, ScoreMode.TOP_SCORES);

      // "foo bar" match
      doc = scorer.iterator().nextDoc();
      assertEquals(2, doc);
      assertEquals(1f, scorer.score(), 0);

      scorer.setMinCompetitiveScore(2f);
      assertEquals(doc, scorer.docID());
      assertEquals(doc, scorer.iterator().docID());
      assertEquals(1f, scorer.score(), 0);

      doc = scorer.iterator().nextDoc();
      assertEquals(NO_MORE_DOCS, doc);
    }
  }

  public void testTwoPhaseMatching_ScoreMode_COMPLETE() throws Exception {
    testTwoPhaseMatching(ScoreMode.COMPLETE);
  }

  public void testTwoPhaseMatching_ScoreMode_COMPLETE_NO_SCORES() throws Exception {
    testTwoPhaseMatching(ScoreMode.COMPLETE_NO_SCORES);
  }

  private void testTwoPhaseMatching(ScoreMode scoreMode) throws Exception {
    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      int doc;
      ConstantScoreScorer scorer = index.constantScoreScorer(PHRASE_QUERY, 1f, scoreMode);

      // "foo bar" match
      doc = scorer.iterator().nextDoc();
      assertEquals(2, doc);
      assertEquals(1f, scorer.score(), 0);

      // should not reset iterator
      scorer.setMinCompetitiveScore(2f);
      assertEquals(doc, scorer.docID());
      assertEquals(doc, scorer.iterator().docID());
      assertEquals(1f, scorer.score(), 0);

      // "foo not bar" will match the approximation but not the two phase iterator

      // "foo bar foo" match
      doc = scorer.iterator().nextDoc();
      assertEquals(5, doc);
      assertEquals(1f, scorer.score(), 0);

      doc = scorer.iterator().nextDoc();
      assertEquals(NO_MORE_DOCS, doc);
    }
  }

  public void testTwoPhaseMatching_ScoreMode_TOP_SCORES() throws Exception {
    try (TestConstantScoreScorerIndex index = new TestConstantScoreScorerIndex()) {
      int doc;
      ConstantScoreScorer scorer =
          index.constantScoreScorer(PHRASE_QUERY, 1f, ScoreMode.TOP_SCORES);

      // "foo bar" match
      doc = scorer.iterator().nextDoc();
      assertEquals(2, doc);
      assertEquals(1f, scorer.score(), 0);

      scorer.setMinCompetitiveScore(2f);
      assertEquals(doc, scorer.docID());
      assertEquals(doc, scorer.iterator().docID());
      assertEquals(1f, scorer.score(), 0);

      doc = scorer.iterator().nextDoc();
      assertEquals(NO_MORE_DOCS, doc);
    }
  }

  static class TestConstantScoreScorerIndex implements AutoCloseable {
    private final Directory directory;
    private final RandomIndexWriter writer;
    private final IndexReader reader;

    TestConstantScoreScorerIndex() throws IOException {
      directory = newDirectory();

      writer =
          new RandomIndexWriter(
              random(), directory, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
      writer
          .w
          .getConfig()
          .getCodec()
          .compoundFormat()
          .setShouldUseCompoundFile(random().nextBoolean());
      for (String VALUE : VALUES) {
        Document doc = new Document();
        doc.add(newTextField(FIELD, VALUE, Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);

      reader = writer.getReader();
      writer.close();
    }

    ConstantScoreScorer constantScoreScorer(Query query, float score, ScoreMode scoreMode)
        throws IOException {
      IndexSearcher searcher = newSearcher(reader);
      Weight weight = searcher.createWeight(new ConstantScoreQuery(query), scoreMode, 1);
      List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

      assertEquals(1, leaves.size());

      LeafReaderContext context = leaves.get(0);
      Scorer scorer = weight.scorer(context);

      if (scorer.twoPhaseIterator() == null) {
        return new ConstantScoreScorer(score, scoreMode, scorer.iterator());
      } else {
        return new ConstantScoreScorer(score, scoreMode, scorer.twoPhaseIterator());
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
      directory.close();
    }
  }

  public void testEarlyTermination() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());
    Directory dir = newDirectory();
    IndexWriter iw =
        new IndexWriter(
            dir,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(2)
                .setMergePolicy(newLogMergePolicy()));
    final int numDocs = 50;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Field f = newTextField("key", i % 2 == 0 ? "foo bar" : "baz", Field.Store.YES);
      doc.add(f);
      iw.addDocument(doc);
    }
    IndexReader ir = DirectoryReader.open(iw);

    // Don't use threads so that we can assert on the number of visited hits
    IndexSearcher is = newSearcher(ir, true, true, false);

    TopScoreDocCollectorManager c = new TopScoreDocCollectorManager(10, 10);
    TopDocs topDocs = is.search(new ConstantScoreQuery(new TermQuery(new Term("key", "foo"))), c);
    assertEquals(11, topDocs.totalHits.value());
    assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation());

    c = new TopScoreDocCollectorManager(10, 10);
    Query query =
        new BooleanQuery.Builder()
            .add(new ConstantScoreQuery(new TermQuery(new Term("key", "foo"))), Occur.SHOULD)
            .add(new ConstantScoreQuery(new TermQuery(new Term("key", "bar"))), Occur.FILTER)
            .build();
    topDocs = is.search(query, c);
    assertEquals(11, topDocs.totalHits.value());
    assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation());

    iw.close();
    ir.close();
    dir.close();
  }
}
