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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestDisjunctionDISIApproximation extends LuceneTestCase {

  public void testDocIDRunEnd() throws IOException {
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 30_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(20_000, 50_000);
    DocIdSetIterator clause3 = DocIdSetIterator.range(60_000, 60_001);
    Scorer scorer1 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause1);
    Scorer scorer2 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause2);
    Scorer scorer3 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause3);
    DocIdSetIterator iterator =
        new DisjunctionDISIApproximation(
            Arrays.asList(
                new DisiWrapper(scorer1, false),
                new DisiWrapper(scorer2, false),
                new DisiWrapper(scorer3, false)),
            // High enough to keep all clauses in the lead heap.
            100_000);
    assertEquals(10_000, iterator.nextDoc());
    assertEquals(30_000, iterator.docIDRunEnd());
    assertEquals(25_000, iterator.advance(25_000));
    assertEquals(50_000, iterator.docIDRunEnd());
    assertEquals(60_000, iterator.advance(50_000));
    assertEquals(60_001, iterator.docIDRunEnd());
  }

  public void testDocIDRunEndOnlyUsesLeadIteratorsWhenBothClausesMatchAndOtherHasLongerRun()
      throws IOException {
    DocIdSetIterator heapClause = DocIdSetIterator.range(100, 110);
    DocIdSetIterator otherIteratorClause = DocIdSetIterator.range(100, 200);
    Scorer heapScorer = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, heapClause);
    Scorer otherIteratorScorer =
        new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, otherIteratorClause);
    DocIdSetIterator iterator =
        new DisjunctionDISIApproximation(
            Arrays.asList(
                new DisiWrapper(heapScorer, false), new DisiWrapper(otherIteratorScorer, false)),
            // Low enough to put the shorter, lower-cost clause in the lead heap and the longer,
            // higher-cost clause in otherIterators.
            10);
    // Both clauses match doc 100, but the longer otherIterator run is ignored.
    assertEquals(100, iterator.nextDoc());
    assertEquals(110, iterator.docIDRunEnd());
  }

  public void testIntoArray() throws IOException {
    DisjunctionDISIApproximation iterator =
        disjunction(100_000, DocIdSetIterator.range(10, 20), DocIdSetIterator.range(15, 30));
    assertEquals(10, iterator.nextDoc());

    // The bulk window is bounded by the length of the target array, so that no match is dropped.
    int[] docs = new int[8];
    assertEquals(8, iterator.intoArray(DocIdSetIterator.NO_MORE_DOCS, docs));
    assertArrayEquals(new int[] {10, 11, 12, 13, 14, 15, 16, 17}, docs);
    assertEquals(18, iterator.docID());

    docs = new int[64];
    assertEquals(12, iterator.intoArray(DocIdSetIterator.NO_MORE_DOCS, docs));
    for (int i = 0; i < 12; ++i) {
      assertEquals(18 + i, docs[i]);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    assertEquals(0, iterator.intoArray(DocIdSetIterator.NO_MORE_DOCS, docs));
  }

  public void testIntoArrayReturnsSameDocsAsNextDoc() throws IOException {
    for (int iter = 0; iter < 100; ++iter) {
      int maxDoc = TestUtil.nextInt(random(), 1, 20_000);
      int numClauses = TestUtil.nextInt(random(), 2, 5);
      int[][] ranges = new int[numClauses][];
      for (int i = 0; i < numClauses; ++i) {
        int min = random().nextInt(maxDoc);
        ranges[i] = new int[] {min, TestUtil.nextInt(random(), min + 1, maxDoc + 1)};
      }
      long leadCost = random().nextBoolean() ? 1 : Long.MAX_VALUE;

      List<Integer> expected = new ArrayList<>();
      DocIdSetIterator reference = disjunction(leadCost, ranges(ranges));
      for (int doc = reference.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = reference.nextDoc()) {
        expected.add(doc);
      }

      // #intoArray loads doc IDs by windows rather than one at a time, so it may return fewer doc
      // IDs per call than the default implementation. It must still return the exact same doc IDs
      // overall, and never return an empty array while doc IDs below upTo remain.
      List<Integer> actual = new ArrayList<>();
      DocIdSetIterator iterator = disjunction(leadCost, ranges(ranges));
      iterator.nextDoc();
      while (iterator.docID() != DocIdSetIterator.NO_MORE_DOCS) {
        int[] docs = new int[TestUtil.nextInt(random(), 1, 4096)];
        int upTo = iterator.docID() + random().nextInt(5_000);
        int size = iterator.intoArray(upTo, docs);
        if (size == 0) {
          assertTrue(iterator.docID() >= upTo);
          continue;
        }
        assertTrue(size <= docs.length);
        for (int i = 0; i < size; ++i) {
          assertTrue(docs[i] < upTo);
          actual.add(docs[i]);
        }
        assertTrue(iterator.docID() > docs[size - 1]);
      }
      assertEquals(expected, actual);
    }
  }

  private static DocIdSetIterator[] ranges(int[][] ranges) {
    DocIdSetIterator[] iterators = new DocIdSetIterator[ranges.length];
    for (int i = 0; i < ranges.length; ++i) {
      iterators[i] = DocIdSetIterator.range(ranges[i][0], ranges[i][1]);
    }
    return iterators;
  }

  private static DisjunctionDISIApproximation disjunction(
      long leadCost, DocIdSetIterator... clauses) {
    List<DisiWrapper> wrappers = new ArrayList<>();
    for (DocIdSetIterator clause : clauses) {
      wrappers.add(
          new DisiWrapper(
              new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause), false));
    }
    return new DisjunctionDISIApproximation(wrappers, leadCost);
  }

  public void testDocIDRunEndDoesNotScanOtherIterators() throws IOException {
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 30_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(20_000, 50_000);
    DocIdSetIterator clause3 = DocIdSetIterator.range(60_000, 60_001);
    Scorer scorer1 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause1);
    Scorer scorer2 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause2);
    Scorer scorer3 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause3);
    DocIdSetIterator iterator =
        new DisjunctionDISIApproximation(
            Arrays.asList(
                new DisiWrapper(scorer1, false),
                new DisiWrapper(scorer2, false),
                new DisiWrapper(scorer3, false)),
            1);
    assertEquals(10_000, iterator.nextDoc());
    assertEquals(10_001, iterator.docIDRunEnd());
    assertEquals(25_000, iterator.advance(25_000));
    assertEquals(25_001, iterator.docIDRunEnd());
    assertEquals(60_000, iterator.advance(50_000));
    assertEquals(60_001, iterator.docIDRunEnd());
  }
}
