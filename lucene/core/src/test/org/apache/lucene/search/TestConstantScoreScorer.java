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
import java.util.ArrayList;
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
import org.apache.lucene.tests.search.AssertingScorer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

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

  /**
   * An empty {@link DocAndFloatFeatureBuffer} tells callers that the iterator has no doc left
   * before {@code upTo}, so batches whose docs are all deleted must not be reported as such.
   */
  public void testNextDocsAndScoresSkipsFullyDeletedBatches() throws IOException {
    int maxDoc = 10_000;
    int firstLiveDoc = 9_000;
    Bits liveDocs =
        new Bits() {
          @Override
          public boolean get(int index) {
            return index >= firstLiveDoc;
          }

          @Override
          public int length() {
            return maxDoc;
          }
        };

    for (ScoreMode scoreMode : new ScoreMode[] {ScoreMode.COMPLETE, ScoreMode.TOP_SCORES}) {
      for (boolean disjunction : new boolean[] {false, true}) {
        DocIdSetIterator disi;
        if (disjunction) {
          disi =
              DisjunctionDISIApproximation.of(
                  List.of(
                      new DisiWrapper(
                          new ConstantScoreScorer(
                              1f, ScoreMode.COMPLETE_NO_SCORES, DocIdSetIterator.range(0, 5_000)),
                          false),
                      new DisiWrapper(
                          new ConstantScoreScorer(
                              1f,
                              ScoreMode.COMPLETE_NO_SCORES,
                              DocIdSetIterator.range(5_000, maxDoc)),
                          false)),
                  maxDoc);
        } else {
          disi = DocIdSetIterator.all(maxDoc);
        }

        ConstantScoreScorer scorer = new ConstantScoreScorer(2f, scoreMode, disi);
        assertEquals(0, scorer.iterator().nextDoc());

        DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
        List<Integer> collected = new ArrayList<>();
        for (scorer.nextDocsAndScores(maxDoc, liveDocs, buffer);
            buffer.size > 0;
            scorer.nextDocsAndScores(maxDoc, liveDocs, buffer)) {
          for (int i = 0; i < buffer.size; ++i) {
            collected.add(buffer.docs[i]);
            assertEquals(2f, buffer.features[i], 0f);
          }
        }

        List<Integer> expected = new ArrayList<>();
        for (int doc = firstLiveDoc; doc < maxDoc; ++doc) {
          expected.add(doc);
        }
        assertEquals(expected, collected);
      }
    }
  }

  /**
   * Randomized companion to the test above. Reaching a fully deleted batch takes a doc ID range
   * that spans more than one batch together with deletions that cluster, so uniformly random
   * deletions over the small indexes that most randomized tests build never get there. Scorers are
   * wrapped in {@link AssertingScorer} so that the buffer contract is checked as well.
   */
  public void testNextDocsAndScoresRandomClusteredDeletions() throws IOException {
    int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      int maxDoc = TestUtil.nextInt(random(), 12_000, 30_000);
      float density = 1f / (1 << random().nextInt(3));

      FixedBitSet matches = new FixedBitSet(maxDoc);
      for (int doc = 0; doc < maxDoc; ++doc) {
        if (random().nextFloat() < density) {
          matches.set(doc);
        }
      }
      if (matches.cardinality() == 0) {
        continue;
      }

      // A run of deleted docs wide enough to swallow a whole batch whatever its alignment, plus
      // scattered deletions.
      FixedBitSet liveDocs = new FixedBitSet(maxDoc);
      liveDocs.set(0, maxDoc);
      int runLength = TestUtil.nextInt(random(), 2 * 4096, Math.min(3 * 4096, maxDoc));
      int runStart = TestUtil.nextInt(random(), 0, maxDoc - runLength);
      liveDocs.clear(runStart, runStart + runLength);
      for (int i = 0, scattered = random().nextInt(100); i < scattered; ++i) {
        liveDocs.clear(random().nextInt(maxDoc));
      }

      List<Integer> expected = new ArrayList<>();
      for (int doc = 0; doc < maxDoc; ++doc) {
        if (matches.get(doc) && liveDocs.get(doc)) {
          expected.add(doc);
        }
      }

      DocIdSetIterator disi;
      if (random().nextBoolean()) {
        int numSubs = TestUtil.nextInt(random(), 2, 4);
        FixedBitSet[] subSets = new FixedBitSet[numSubs];
        for (int i = 0; i < numSubs; ++i) {
          subSets[i] = new FixedBitSet(maxDoc);
        }
        for (int doc = 0; doc < maxDoc; ++doc) {
          if (matches.get(doc)) {
            subSets[random().nextInt(numSubs)].set(doc);
          }
        }
        List<DisiWrapper> subs = new ArrayList<>();
        for (FixedBitSet subSet : subSets) {
          subs.add(
              new DisiWrapper(
                  new ConstantScoreScorer(
                      1f,
                      ScoreMode.COMPLETE_NO_SCORES,
                      new BitSetIterator(subSet, subSet.cardinality())),
                  false));
        }
        disi = DisjunctionDISIApproximation.of(subs, maxDoc);
      } else {
        disi = new BitSetIterator(matches, matches.cardinality());
      }

      ScoreMode scoreMode = random().nextBoolean() ? ScoreMode.COMPLETE : ScoreMode.TOP_SCORES;
      Scorer scorer =
          AssertingScorer.wrap(new ConstantScoreScorer(2f, scoreMode, disi), true, false);
      scorer.iterator().nextDoc();

      DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
      List<Integer> collected = new ArrayList<>();
      for (scorer.nextDocsAndScores(maxDoc, liveDocs, buffer);
          buffer.size > 0;
          scorer.nextDocsAndScores(maxDoc, liveDocs, buffer)) {
        for (int i = 0; i < buffer.size; ++i) {
          collected.add(buffer.docs[i]);
          assertEquals(2f, buffer.features[i], 0f);
        }
      }

      assertEquals(expected, collected);
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
