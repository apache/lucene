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
import java.util.List;
import org.apache.lucene.tests.search.AssertingBulkScorer;
import org.apache.lucene.tests.search.RandomApproximationQuery.RandomTwoPhaseView;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestDenseConjunctionBulkScorer extends LuceneTestCase {

  public void testSameMatches() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause1.set(i);
      clause2.set(i);
      clause3.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    assertEquals(clause1, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(clause1.cardinality(), collector.count);
  }

  public void testApplyAcceptDocs() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    clause1.set(0, maxDoc);
    clause2.set(0, maxDoc);
    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      acceptDocs.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    assertEquals(acceptDocs, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(acceptDocs.cardinality(), collector.count);
  }

  public void testEmptyIntersection() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc - 1; i += 2) {
      clause1.set(i);
      clause2.set(i + 1);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    assertTrue(result.scanIsEmpty());

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, collector.count);
  }

  public void testClustered() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    clause1.set(10_000, 90_000);
    clause2.set(0, 80_000);
    clause3.set(20_000, 100_000);
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    expected.set(20_000, 80_000);
    assertArrayEquals(expected.getBits(), result.getBits());
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testSparseAfter2ndClause() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    // 13 and 17 are primes, so their only intersection is on multiples of both 13 and 17
    // Likewise, 19 is prime, so the only intersection of the conjunction is on multiples of 13, 17
    // and 19
    for (int i = 0; i < maxDoc; i += 13) {
      clause1.set(i);
    }
    for (int i = 0; i < maxDoc; i += 17) {
      clause2.set(i);
    }
    for (int i = 0; i < maxDoc; i += 19) {
      clause3.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 13 * 17 * 19) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testMatchAllNoLiveDocs() throws IOException {
    int maxDoc = 100_000;
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(DocIdSetIterator.all(maxDoc)),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    result.flip(0, maxDoc);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, result.nextSetBit(0));

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(DocIdSetIterator.all(maxDoc)),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(maxDoc, collector.count);
  }

  public void testMatchAllWithLiveDocs() throws IOException {
    int maxDoc = 100_000;
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(DocIdSetIterator.all(maxDoc)),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    acceptDocs.set(10_000, 20_000);
    for (int i = 30_000; i < maxDoc; i += 3) {
      acceptDocs.set(i);
    }
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    assertEquals(acceptDocs, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(DocIdSetIterator.all(maxDoc)),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(acceptDocs.cardinality(), collector.count);
  }

  public void testOneClauseNoLiveDocs() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause1.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause1, clause1.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    assertEquals(clause1, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause1, clause1.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(clause1.cardinality(), collector.count);
  }

  public void testOneClauseWithLiveDocs() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause1.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause1, clause1.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    acceptDocs.set(10_000, 20_000);
    for (int i = 30_000; i < maxDoc; i += 3) {
      acceptDocs.set(i);
    }
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    expected.or(acceptDocs);
    expected.and(clause1);
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause1, clause1.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testStopOnMinCompetitiveScore() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause1.set(i);
    }
    for (int i = 0; i < maxDoc; i += 5) {
      clause2.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Collections.emptyList(),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {

          private Scorable scorable;

          @Override
          public void setScorer(Scorable scorer) throws IOException {
            this.scorable = scorer;
          }

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
            if (doc == 50_000) {
              scorable.setMinCompetitiveScore(Float.MIN_VALUE);
            }
            // It should never go above the doc when setMinCompetitiveScore was called, plus the
            // window size
            assertTrue(doc < 50_000 + DenseConjunctionBulkScorer.WINDOW_SIZE);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
  }

  private static class CountingLeafCollector implements LeafCollector {

    int count;

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
      count++;
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      count += stream.count();
    }
  }

  public void testRangeIntersection() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(30_000, 80_000);
    List<DocIdSetIterator> clauses = Arrays.asList(clause1, clause2);
    Collections.shuffle(clauses, random());

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    // Matches are collected as a single DocIdStream
    scorer.score(
        new LeafCollector() {

          private boolean called = false;
          private int expected = 30_000;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            fail();
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            assertFalse(called);
            called = true;
            stream.forEach(
                doc -> {
                  assertEquals(expected++, doc);
                });
          }

          @Override
          public void finish() throws IOException {
            assertEquals(60_001, expected);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    clause1 = DocIdSetIterator.range(10_000, 60_000);
    clause2 = DocIdSetIterator.range(30_000, 80_000);
    clauses = Arrays.asList(clause1, clause2);
    Collections.shuffle(clauses, random());
    scorer = new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(30_000, collector.count);
  }

  public void testRangeIntersectionWithLiveDocs() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(30_000, 80_000);
    List<DocIdSetIterator> clauses = Arrays.asList(clause1, clause2);
    Collections.shuffle(clauses, random());

    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      acceptDocs.set(i);
    }

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 30_000; i < 60_000; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    clause1 = DocIdSetIterator.range(10_000, 60_000);
    clause2 = DocIdSetIterator.range(30_000, 80_000);
    clauses = Arrays.asList(clause1, clause2);
    Collections.shuffle(clauses, random());
    scorer = new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testMixedRangeIntersection() throws IOException {
    int maxDoc = 80_000;
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause2.set(i);
    }
    DocIdSetIterator rangeIterator =
        new FilterDocIdSetIterator(DocIdSetIterator.range(10_000, 50_000)) {
          @Override
          public int docIDRunEnd() throws IOException {
            return in.docIDRunEnd();
          }

          @Override
          public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            fail(
                "intoBitSet should not be called if scoring windows get aligned with #docIDRunEnd");
          }
        };
    List<DocIdSetIterator> clauses =
        Arrays.asList(rangeIterator, new BitSetIterator(clause2, 40_000));
    Collections.shuffle(clauses, random());
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 10_000; i < 50_000; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    clauses =
        Arrays.asList(DocIdSetIterator.range(10_000, 50_000), new BitSetIterator(clause2, 40_000));
    Collections.shuffle(clauses, random());
    scorer = new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testMixedRangeIntersectionWithLiveDocs() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3) {
      clause2.set(i);
    }
    List<DocIdSetIterator> clauses =
        Arrays.asList(DocIdSetIterator.range(10_000, 60_000), new BitSetIterator(clause2, 50_000));
    Collections.shuffle(clauses, random());

    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      acceptDocs.set(i);
    }

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 10_002; i < 60_000; i += 6) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    clauses =
        Arrays.asList(DocIdSetIterator.range(10_000, 60_000), new BitSetIterator(clause2, 50_000));
    Collections.shuffle(clauses, random());
    scorer = new DenseConjunctionBulkScorer(clauses, Collections.emptyList(), maxDoc, 0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testTwoPhaseIterators() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3) {
      clause1.set(i);
    }
    for (int i = 0; i < maxDoc; i += 5) {
      clause2.set(i);
    }
    for (int i = 0; i < maxDoc; i += 7) {
      clause3.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause3, clause3.approximateCardinality()))),
            maxDoc,
            0f);
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3 * 5 * 7) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause3, clause3.approximateCardinality()))),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testTwoPhaseIteratorsWithLiveDocs() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3) {
      clause1.set(i);
    }
    for (int i = 0; i < maxDoc; i += 5) {
      clause2.set(i);
    }
    FixedBitSet acceptDocs = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      acceptDocs.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.approximateCardinality()))),
            maxDoc,
            0f);
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        acceptDocs,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2 * 3 * 5) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.approximateCardinality()))),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, acceptDocs, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testMixedTwoPhaseIterators() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3) {
      clause1.set(i);
    }
    for (int i = 0; i < maxDoc; i += 5) {
      clause2.set(i);
    }
    for (int i = 0; i < maxDoc; i += 7) {
      clause3.set(i);
    }
    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause3, clause3.approximateCardinality()))),
            maxDoc,
            0f);
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 3 * 5 * 7) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(
                new BitSetIterator(clause2, clause2.approximateCardinality())),
            Arrays.asList(
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause1, clause1.approximateCardinality())),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause3, clause3.approximateCardinality()))),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testTwoPhaseRangeIntersection() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(30_000, 80_000);
    DocIdSetIterator clause3 = DocIdSetIterator.range(20_000, 70_000);
    List<TwoPhaseIterator> clauses =
        Arrays.asList(
            new RandomTwoPhaseView(random(), clause1),
            new RandomTwoPhaseView(random(), clause2),
            new RandomTwoPhaseView(random(), clause3));
    Collections.shuffle(clauses, random());

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(Collections.emptyList(), clauses, maxDoc, 0f);
    // Matches are collected as a single DocIdStream
    scorer.score(
        new LeafCollector() {

          private boolean called = false;
          private int expected = 30_000;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            assertEquals(expected++, doc);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            assertFalse(called);
            called = true;
            stream.forEach(
                doc -> {
                  assertEquals(expected++, doc);
                });
          }

          @Override
          public void finish() throws IOException {
            assertTrue(called);
            assertEquals(60_001, expected);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    clause1 = DocIdSetIterator.range(10_000, 60_000);
    clause2 = DocIdSetIterator.range(30_000, 80_000);
    clause3 = DocIdSetIterator.range(20_000, 70_000);
    clauses =
        Arrays.asList(
            new RandomTwoPhaseView(random(), clause1),
            new RandomTwoPhaseView(random(), clause2),
            new RandomTwoPhaseView(random(), clause3));
    Collections.shuffle(clauses, random());

    scorer = new DenseConjunctionBulkScorer(Collections.emptyList(), clauses, maxDoc, 0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(30_000, collector.count);
  }

  public void testMixedTwoPhaseRangeIntersection() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(30_000, 80_000);
    DocIdSetIterator clause3 = DocIdSetIterator.range(20_000, 70_000);

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(clause1),
            Arrays.asList(
                new RandomTwoPhaseView(random(), clause2),
                new RandomTwoPhaseView(random(), clause3)),
            maxDoc,
            0f);
    // Matches are collected as a single DocIdStream
    scorer.score(
        new LeafCollector() {

          private boolean called = false;
          private int expected = 30_000;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            assertEquals(expected++, doc);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            assertFalse(called);
            called = true;
            stream.forEach(
                doc -> {
                  assertEquals(expected++, doc);
                });
          }

          @Override
          public void finish() throws IOException {
            assertTrue(called);
            assertEquals(60_001, expected);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    clause1 = DocIdSetIterator.range(10_000, 60_000);
    clause2 = DocIdSetIterator.range(30_000, 80_000);
    clause3 = DocIdSetIterator.range(20_000, 70_000);

    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(clause1),
            Arrays.asList(
                new RandomTwoPhaseView(random(), clause2),
                new RandomTwoPhaseView(random(), clause3)),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(30_000, collector.count);
  }

  public void testMixedRangeIntersectionTwoPhase1() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause2.set(i);
    }

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(new BitSetIterator(clause2, clause2.cardinality())),
            Collections.singletonList(new RandomTwoPhaseView(random(), clause1)),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 10_000; i < 60_000; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    clause1 = DocIdSetIterator.range(10_000, 60_000);
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.singletonList(new BitSetIterator(clause2, clause2.cardinality())),
            Collections.singletonList(new RandomTwoPhaseView(random(), clause1)),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }

  public void testMixedRangeIntersectionTwoPhase2() throws IOException {
    int maxDoc = 100_000;
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 60_000);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc; i += 2) {
      clause2.set(i);
    }

    BulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(random(), clause1),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.cardinality()))),
            maxDoc,
            0f);
    // AssertingBulkScorer randomly splits the scored range into smaller ranges
    scorer = AssertingBulkScorer.wrap(random(), scorer, maxDoc);
    FixedBitSet result = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            result.set(doc);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int i = 10_000; i < 60_000; i += 2) {
      expected.set(i);
    }
    assertEquals(expected, result);

    // Now exercise DocIdStream.count()
    clause1 = DocIdSetIterator.range(10_000, 60_000);
    scorer =
        new DenseConjunctionBulkScorer(
            Collections.emptyList(),
            Arrays.asList(
                new RandomTwoPhaseView(random(), clause1),
                new RandomTwoPhaseView(
                    random(), new BitSetIterator(clause2, clause2.cardinality()))),
            maxDoc,
            0f);
    CountingLeafCollector collector = new CountingLeafCollector();
    scorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(expected.cardinality(), collector.count);
  }
}
