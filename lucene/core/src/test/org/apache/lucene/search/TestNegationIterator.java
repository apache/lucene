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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestNegationIterator extends LuceneTestCase {

  // Verify that NegationTwoPhaseIterator via DenseConjunctionBulkScorer produces the same results
  // as a brute-force complement over a random excluded set.
  public void testRandomDirectIterator() throws IOException {
    final int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc =
          TestUtil.nextInt(random(), DenseConjunctionBulkScorer.WINDOW_SIZE, 200_000);
      // Dense "lead" clause that matches all docs.
      FixedBitSet allDocs = new FixedBitSet(maxDoc);
      allDocs.set(0, maxDoc);
      // Random excluded set.
      FixedBitSet excluded = new FixedBitSet(maxDoc);
      int numExcluded = random().nextInt(maxDoc / 2 + 1);
      for (int i = 0; i < numExcluded; i++) {
        excluded.set(random().nextInt(maxDoc));
      }

      // Expected: allDocs AND NOT excluded.
      FixedBitSet expected = allDocs.clone();
      expected.andNot(excluded);

      NegationTwoPhaseIterator negation =
          new NegationTwoPhaseIterator(
              new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
      ConstantScoreScorer negScorer =
          new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, negation);

      BulkScorer scorer =
          DenseConjunctionBulkScorer.of(
              Arrays.asList(
                  new ConstantScoreScorer(
                      0f,
                      ScoreMode.COMPLETE_NO_SCORES,
                      new BitSetIterator(allDocs, allDocs.approximateCardinality())),
                  negScorer),
              maxDoc,
              0f);

      FixedBitSet actual = new FixedBitSet(maxDoc);
      scorer.score(
          new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
              actual.set(doc);
            }
          },
          null,
          0,
          DocIdSetIterator.NO_MORE_DOCS);

      assertEquals("iter=" + iter, expected, actual);
    }
  }

  // Verify correctness end-to-end: FILTER + MUST_NOT query on a real index.
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

  /** docIDRunEnd() should return the next excluded doc when the excl approximation is ahead. */
  public void testDocIDRunEndNOBlock() throws IOException {
    int maxDoc = 10_000;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    excluded.set(5000);

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, 1), maxDoc);

    // Advance the approximation (all) to doc 0.
    negation.approximation().advance(0);
    // exclApprox starts at -1, so docIDRunEnd falls back to doc+1 conservatively.
    // After the first applyMask call the excl is positioned and NO-block detection works.
    // Here we manually advance exclApprox to verify the NO-block logic.
    // Use matches() to position exclApprox at doc 0 (not excluded, returns true).
    assertTrue(negation.matches()); // doc 0 is not excluded
    // exclApprox is now at 5000 (first excluded), approximation at 0.
    assertEquals(5000, negation.docIDRunEnd());
  }

  /** applyMask() should remove excluded docs from the bitset. */
  public void testApplyMask() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE;
    // Exclude even-numbered docs.
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      excluded.set(i);
    }

    NegationTwoPhaseIterator negation =
        new NegationTwoPhaseIterator(
            new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
    negation.approximation().advance(0);

    // Candidate set: all docs in [0, windowSize).
    FixedBitSet candidates = new FixedBitSet(windowSize);
    candidates.set(0, windowSize);

    negation.applyMask(windowSize, candidates, 0);

    // Only odd docs should remain.
    FixedBitSet expected = new FixedBitSet(windowSize);
    for (int i = 1; i < windowSize; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, candidates);
  }

  /** FILTER + MUST_NOT on a numeric range should produce correct results. */
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
}
