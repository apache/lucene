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
import java.util.Collections;
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
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

public class TestNegationIterator extends LuceneTestCase {

  // Verify that NegationIterator via DenseConjunctionBulkScorer produces the same results
  // as a brute-force complement over a random excluded set.
  public void testRandomDirectIterator() throws IOException {
    final int iters = atLeast(20);
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc = TestUtil.nextInt(random(), DenseConjunctionBulkScorer.WINDOW_SIZE, 200_000);
      // Build a dense "lead" clause that matches all docs.
      FixedBitSet allDocs = new FixedBitSet(maxDoc);
      allDocs.set(0, maxDoc);
      // Build a random excluded set.
      FixedBitSet excluded = new FixedBitSet(maxDoc);
      int numExcluded = random().nextInt(maxDoc / 2 + 1);
      for (int i = 0; i < numExcluded; i++) {
        excluded.set(random().nextInt(maxDoc));
      }

      // Expected result: allDocs AND NOT excluded.
      FixedBitSet expected = allDocs.clone();
      expected.andNot(excluded);

      // Build NegationIterator wrapping the excluded set.
      NegationIterator negIter =
          new NegationIterator(new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
      ConstantScoreScorer negScorer =
          new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, negIter);

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

  // Verify correctness end-to-end: FILTER + MUST_NOT query on a real index, comparing against
  // the same query expressed as two FILTER clauses (equivalent when the lead is all-matching).
  public void testEndToEndSearchCorrectness() throws Exception {
    int numDocs = atLeast(5000);
    // Ensure DenseConjunctionBulkScorer is chosen (needs dense lead).
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

        // Query using MUST_NOT (our NegationIterator path).
        Query mustNotQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(excl, Occur.MUST_NOT)
                .build();

        // Reference: a query that explicitly matches docs where "exclude" is absent.
        // We verify the count matches.
        int mustNotCount = searcher.count(mustNotQuery);
        int expectedCount = 0;
        for (int i = 0; i < numDocs; i++) {
          if (i % excludedEvery != 0) expectedCount++;
        }
        assertEquals(expectedCount, mustNotCount);
      }
    }
  }

  /** Verify docIDRunEnd() returns the next excluded doc position (NO block optimization). */
  public void testDocIDRunEndNOBlock() throws IOException {
    int maxDoc = 10_000;
    // Excluded set: only doc 5000.
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    excluded.set(5000);

    NegationIterator negIter =
        new NegationIterator(new BitSetIterator(excluded, 1), maxDoc);

    // Advance to doc 0 (not excluded).
    assertEquals(0, negIter.advance(0));
    // docIDRunEnd should return 5000 (start of excluded run).
    assertEquals(5000, negIter.docIDRunEnd());
  }

  /** Verify intoBitSet correctly sets non-excluded docs in the window. */
  public void testIntoBitSet() throws IOException {
    int maxDoc = DenseConjunctionBulkScorer.WINDOW_SIZE * 2;
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE;
    // Exclude even-numbered docs.
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      excluded.set(i);
    }

    NegationIterator negIter =
        new NegationIterator(new BitSetIterator(excluded, excluded.approximateCardinality()), maxDoc);
    assertEquals(1, negIter.advance(0)); // advance to first non-excluded (doc 1)

    FixedBitSet result = new FixedBitSet(windowSize);
    negIter.intoBitSet(windowSize, result, 0);

    // Only odd docs [1, 3, 5, ...] should be set.
    FixedBitSet expected = new FixedBitSet(windowSize);
    for (int i = 1; i < windowSize; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);
  }

  /** Verify that a MUST_NOT + FILTER query on a dense numeric range produces correct results. */
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

        // MUST_NOT: exclude docs where val is in [min, max].
        Query mustNotQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(rangeQuery, Occur.MUST_NOT)
                .build();

        // Reference: docs where val is NOT in [min, max].
        Query filterQuery =
            new BooleanQuery.Builder()
                .add(lead, Occur.FILTER)
                .add(SortedNumericDocValuesField.newSlowRangeQuery("val", Long.MIN_VALUE, min - 1), Occur.SHOULD)
                .add(SortedNumericDocValuesField.newSlowRangeQuery("val", max + 1, Long.MAX_VALUE), Occur.SHOULD)
                .setMinimumNumberShouldMatch(1)
                .build();

        int mustNotCount = searcher.count(mustNotQuery);
        int filterCount = searcher.count(filterQuery);
        assertEquals(filterCount, mustNotCount);
      }
    }
  }
}
