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
import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestNegationTwoPhaseIterator extends LuceneTestCase {

  // Returns a TwoPhaseIterator whose approximation is approxSet and whose matches() checks
  // confirmedSet. confirmedSet must be a subset of approxSet.
  private static TwoPhaseIterator twoPhaseFromBitSets(FixedBitSet approxSet, FixedBitSet confirmed) {
    BitSetIterator approxIt = new BitSetIterator(approxSet, approxSet.approximateCardinality());
    return new TwoPhaseIterator(approxIt) {
      @Override
      public boolean matches() {
        return confirmed.get(approximation().docID());
      }

      @Override
      public float matchCost() {
        return 1f;
      }
    };
  }

  private static BulkScorer scorerWithNegation(
      FixedBitSet lead, NegationTwoPhaseIterator negation, int maxDoc) throws IOException {
    ConstantScoreScorer negScorer =
        new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, negation);
    return DenseConjunctionBulkScorer.of(
        Arrays.asList(
            new ConstantScoreScorer(
                0f,
                ScoreMode.COMPLETE_NO_SCORES,
                new BitSetIterator(lead, lead.approximateCardinality())),
            negScorer),
        maxDoc,
        0f);
  }

  private static FixedBitSet collectHits(BulkScorer scorer, int maxDoc) throws IOException {
    FixedBitSet hits = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            hits.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    return hits;
  }

  // Correctness: NegationTwoPhaseIterator wrapping a plain DISI produces allDocs AND NOT excluded.
  public void testRandomDirectIterator() throws IOException {
    final int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc =
          TestUtil.nextInt(random(), DenseConjunctionBulkScorer.WINDOW_SIZE, 200_000);
      FixedBitSet allDocs = new FixedBitSet(maxDoc);
      allDocs.set(0, maxDoc);
      FixedBitSet excluded = new FixedBitSet(maxDoc);
      int numExcluded = random().nextInt(maxDoc / 2 + 1);
      for (int i = 0; i < numExcluded; i++) {
        excluded.set(random().nextInt(maxDoc));
      }

      FixedBitSet expected = allDocs.clone();
      expected.andNot(excluded);

      NegationTwoPhaseIterator negation =
          new NegationTwoPhaseIterator(
              new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
      FixedBitSet actual = collectHits(scorerWithNegation(allDocs, negation, maxDoc), maxDoc);

      assertEquals("iter=" + iter, expected, actual);
    }
  }

  // Correctness: NegationTwoPhaseIterator wrapping a TwoPhaseIterator with false positives in the
  // approximation produces allDocs AND NOT confirmedExcluded (false positives are not excluded).
  public void testRandomTwoPhaseExcluded() throws IOException {
    final int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc =
          TestUtil.nextInt(random(), DenseConjunctionBulkScorer.WINDOW_SIZE, 200_000);
      FixedBitSet allDocs = new FixedBitSet(maxDoc);
      allDocs.set(0, maxDoc);

      // Approximation is a superset: matches ~50% of docs.
      FixedBitSet approxExcluded = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; i++) {
        if (random().nextBoolean()) approxExcluded.set(i);
      }
      // Confirmed excluded is a subset of the approximation.
      FixedBitSet confirmedExcluded = approxExcluded.clone();
      int numFalsePositives = random().nextInt(confirmedExcluded.cardinality() + 1);
      for (int i = confirmedExcluded.nextSetBit(0);
          i != DocIdSetIterator.NO_MORE_DOCS && numFalsePositives > 0;
          i = confirmedExcluded.nextSetBit(i + 1)) {
        if (random().nextBoolean()) {
          confirmedExcluded.clear(i);
          numFalsePositives--;
        }
      }

      FixedBitSet expected = allDocs.clone();
      expected.andNot(confirmedExcluded);

      NegationTwoPhaseIterator negation =
          new NegationTwoPhaseIterator(
              twoPhaseFromBitSets(approxExcluded, confirmedExcluded), maxDoc);
      FixedBitSet actual = collectHits(scorerWithNegation(allDocs, negation, maxDoc), maxDoc);

      assertEquals("iter=" + iter, expected, actual);
    }
  }

  // End-to-end: FILTER + MUST_NOT with term queries on a real index.
  public void testEndToEndSearchCorrectness() throws Exception {
    int numDocs = atLeast(5000);
    numDocs = Math.max(numDocs, DenseConjunctionBulkScorer.WINDOW_SIZE);
    int excludedEvery = TestUtil.nextInt(random(), 2, 10);

    try (var dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("present", "yes", Field.Store.NO));
          if (i % excludedEvery == 0) {
            doc.add(new StringField("exclude", "yes", Field.Store.NO));
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        Query lead = new TermQuery(new Term("present", "yes"));
        Query excl = new TermQuery(new Term("exclude", "yes"));

        Query mustNotQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(excl, Occur.MUST_NOT)
                .build();

        int mustNotCount = searcher.count(mustNotQuery);
        int expectedCount = 0;
        for (int i = 0; i < numDocs; i++) {
          if (i % excludedEvery != 0) expectedCount++;
        }
        assertEquals(expectedCount, mustNotCount);
      }
    }
  }

  // End-to-end: FILTER + numeric range MUST_NOT produces the same count as the complement filter.
  public void testNumericRangeMustNot() throws Exception {
    int numDocs = atLeast(DenseConjunctionBulkScorer.WINDOW_SIZE);
    long domain = 1000L;

    try (var dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(NumericDocValuesField.indexedField("val", i % domain));
          doc.add(new StringField("present", "yes", Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        long min = domain / 4;
        long max = domain * 3 / 4;
        Query rangeQuery = SortedNumericDocValuesField.newSlowRangeQuery("val", min, max);
        Query lead = new TermQuery(new Term("present", "yes"));

        Query mustNotQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(rangeQuery, Occur.MUST_NOT)
                .build();

        Query filterQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(
                    SortedNumericDocValuesField.newSlowRangeQuery(
                        "val", Long.MIN_VALUE, min - 1),
                    Occur.SHOULD)
                .add(
                    SortedNumericDocValuesField.newSlowRangeQuery(
                        "val", max + 1, Long.MAX_VALUE),
                    Occur.SHOULD)
                .setMinimumNumberShouldMatch(1)
                .build();

        assertEquals(searcher.count(filterQuery), searcher.count(mustNotQuery));
      }
    }
  }

  // applyMask() should remove excluded docs from the candidate bitset.
  public void testApplyMask() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      excluded.set(i);
    }

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
    negation.approximation().advance(0);

    FixedBitSet candidates = new FixedBitSet(windowSize);
    candidates.set(0, windowSize);
    negation.applyMask(windowSize, candidates, 0);

    FixedBitSet expected = new FixedBitSet(windowSize);
    for (int i = 1; i < windowSize; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, candidates);
  }

  // intoBitSet() should set all non-excluded docs in the window (lead bits come from negation).
  public void testIntoBitSet() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      excluded.set(i);
    }

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
    negation.approximation().advance(0);

    // Start with an empty bitset; intoBitSet should populate it.
    FixedBitSet result = new FixedBitSet(windowSize);
    negation.intoBitSet(windowSize, result, 0);

    FixedBitSet expected = new FixedBitSet(windowSize);
    for (int i = 1; i < windowSize; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);
  }

  // docIDRunEnd() should expose NO-blocks (spans with no excluded docs) without needing a prior
  // matches() call — it advances exclApprox itself when it lags behind the current doc.
  public void testDocIDRunEndNOBlock() throws IOException {
    int maxDoc = 10_000;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    excluded.set(5000);

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(new BitSetIterator(excluded, 1), maxDoc);

    negation.approximation().advance(0);
    // docIDRunEnd() should advance exclApprox from its initial position to the first excluded doc.
    assertEquals(5000, negation.docIDRunEnd());
  }

  // docIDRunEnd() returns doc+1 when the excluded clause is at the current doc.
  public void testDocIDRunEndAtCurrentDoc() throws IOException {
    int maxDoc = 10_000;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    excluded.set(0);
    excluded.set(5000);

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);

    negation.approximation().advance(0);
    // matches() positions exclApprox at doc 0 (excluded), so docIDRunEnd returns 1.
    assertFalse(negation.matches());
    assertEquals(1, negation.docIDRunEnd());
  }

  // Edge case: no docs are excluded — all lead docs should be returned.
  public void testNoDocsExcluded() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    FixedBitSet allDocs = new FixedBitSet(maxDoc);
    allDocs.set(0, maxDoc);
    FixedBitSet excluded = new FixedBitSet(maxDoc); // empty

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, 0), maxDoc);
    FixedBitSet actual = collectHits(scorerWithNegation(allDocs, negation, maxDoc), maxDoc);

    assertEquals(allDocs, actual);
  }

  // Edge case: all docs are excluded — result should be empty.
  public void testAllDocsExcluded() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    FixedBitSet allDocs = new FixedBitSet(maxDoc);
    allDocs.set(0, maxDoc);
    FixedBitSet excluded = allDocs.clone();

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
    FixedBitSet actual = collectHits(scorerWithNegation(allDocs, negation, maxDoc), maxDoc);

    assertEquals(new FixedBitSet(maxDoc), actual);
  }

  // When scoreMode needs scores the optimization should not fire; verify the query still returns
  // correct results (it will use ReqExclBulkScorer instead).
  public void testScoringQueryFallsBackToReqExcl() throws Exception {
    int numDocs = atLeast(DenseConjunctionBulkScorer.WINDOW_SIZE);

    try (var dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("present", "yes", Field.Store.NO));
          if (i % 3 == 0) {
            doc.add(new StringField("exclude", "yes", Field.Store.NO));
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        // MUST clause (not FILTER) forces scoring, which bypasses NegationTwoPhaseIterator.
        Query mustNotQuery =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("present", "yes")), Occur.MUST)
                .add(new TermQuery(new Term("exclude", "yes")), Occur.MUST_NOT)
                .build();

        // Count via scoring path must equal count via filter path.
        Query filterQuery =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("present", "yes")), Occur.FILTER)
                .add(new TermQuery(new Term("exclude", "yes")), Occur.MUST_NOT)
                .build();

        assertEquals(searcher.count(filterQuery), searcher.count(mustNotQuery));
      }
    }
  }
}
