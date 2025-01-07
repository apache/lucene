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
    DenseConjunctionBulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())));
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
    DenseConjunctionBulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())));
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
  }

  public void testEmptyIntersection() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    for (int i = 0; i < maxDoc - 1; i += 2) {
      clause1.set(i);
      clause2.set(i + 1);
    }
    DenseConjunctionBulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality())));
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
  }

  public void testClustered() throws IOException {
    int maxDoc = 100_000;
    FixedBitSet clause1 = new FixedBitSet(maxDoc);
    FixedBitSet clause2 = new FixedBitSet(maxDoc);
    FixedBitSet clause3 = new FixedBitSet(maxDoc);
    clause1.set(10_000, 90_000);
    clause2.set(0, 80_000);
    clause3.set(20_000, 100_000);
    DenseConjunctionBulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())));
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
    DenseConjunctionBulkScorer scorer =
        new DenseConjunctionBulkScorer(
            Arrays.asList(
                new BitSetIterator(clause1, clause1.approximateCardinality()),
                new BitSetIterator(clause2, clause2.approximateCardinality()),
                new BitSetIterator(clause3, clause3.approximateCardinality())));
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
  }
}
