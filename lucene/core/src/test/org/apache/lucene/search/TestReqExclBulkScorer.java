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
import org.apache.lucene.tests.search.RandomApproximationQuery.RandomTwoPhaseView;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

public class TestReqExclBulkScorer extends LuceneTestCase {

  public void testRandom() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      doTestRandom(false);
    }
  }

  public void testRandomTwoPhase() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      doTestRandom(true);
    }
  }

  public void doTestRandom(boolean twoPhase) throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 1, 1000);
    DocIdSetBuilder reqBuilder = new DocIdSetBuilder(maxDoc);
    DocIdSetBuilder exclBuilder = new DocIdSetBuilder(maxDoc);
    final int numIncludedDocs = TestUtil.nextInt(random(), 1, maxDoc);
    final int numExcludedDocs = TestUtil.nextInt(random(), 1, maxDoc);
    DocIdSetBuilder.BulkAdder reqAdder = reqBuilder.grow(numIncludedDocs);
    for (int i = 0; i < numIncludedDocs; ++i) {
      reqAdder.add(random().nextInt(maxDoc));
    }
    DocIdSetBuilder.BulkAdder exclAdder = exclBuilder.grow(numExcludedDocs);
    for (int i = 0; i < numExcludedDocs; ++i) {
      exclAdder.add(random().nextInt(maxDoc));
    }

    final DocIdSet req = reqBuilder.build();
    final DocIdSet excl = exclBuilder.build();

    final BulkScorer reqBulkScorer =
        new BulkScorer() {
          final DocIdSetIterator iterator = req.iterator();

          @Override
          public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
              throws IOException {
            int doc = iterator.docID();
            if (iterator.docID() < min) {
              doc = iterator.advance(min);
            }
            while (doc < max) {
              if (acceptDocs == null || acceptDocs.get(doc)) {
                collector.collect(doc);
              }
              doc = iterator.nextDoc();
            }
            return doc;
          }

          @Override
          public long cost() {
            return iterator.cost();
          }
        };

    ReqExclBulkScorer reqExcl;
    if (twoPhase) {
      reqExcl =
          new ReqExclBulkScorer(reqBulkScorer, new RandomTwoPhaseView(random(), excl.iterator()));
    } else {
      reqExcl = new ReqExclBulkScorer(reqBulkScorer, excl.iterator());
    }
    final FixedBitSet actualMatches = new FixedBitSet(maxDoc);
    final FixedBitSet acceptedDocs = new FixedBitSet(maxDoc);
    for (int doc = 0; doc < maxDoc; doc++) {
      if (random().nextBoolean()) {
        acceptedDocs.set(doc);
      }
    }
    Bits acceptDocs = random().nextBoolean() ? null : acceptedDocs.asReadOnlyBits();
    if (random().nextBoolean()) {
      reqExcl.score(
          new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {}

            @Override
            public void collect(int doc) throws IOException {
              actualMatches.set(doc);
            }
          },
          acceptDocs,
          0,
          DocIdSetIterator.NO_MORE_DOCS);
    } else {
      int next = 0;
      while (next < maxDoc) {
        final int min = next;
        final int max = min + random().nextInt(10);
        next =
            reqExcl.score(
                new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) throws IOException {}

                  @Override
                  public void collect(int doc) throws IOException {
                    actualMatches.set(doc);
                  }
                },
                acceptDocs,
                min,
                max);
        assertTrue(next >= max);
      }
    }

    final FixedBitSet expectedMatches = new FixedBitSet(maxDoc);
    expectedMatches.or(req.iterator());
    FixedBitSet excludedSet = new FixedBitSet(maxDoc);
    excludedSet.or(excl.iterator());
    expectedMatches.andNot(excludedSet);
    if (acceptDocs != null) {
      expectedMatches.and(acceptedDocs);
    }

    assertArrayEquals(expectedMatches.getBits(), actualMatches.getBits());
  }

  public void testDenseScore() throws IOException {
    int maxDoc = 10_000;
    FixedBitSet required = new FixedBitSet(maxDoc);
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    FixedBitSet accepted = null;
    FixedBitSet expected = new FixedBitSet(maxDoc);

    boolean hasDelete = random().nextBoolean();
    if (hasDelete) {
      accepted = new FixedBitSet(maxDoc);
    }

    for (int doc = 0; doc < maxDoc; doc++) {
      if (doc % 3 == 1) {
        required.set(doc);
      }
      if ((doc & 1) == 0) {
        excluded.set(doc);
      }

      if (required.get(doc) && excluded.get(doc) == false) {
        expected.set(doc);
      }

      if (hasDelete) {
        if (doc % 5 != 0) {
          accepted.set(doc);
        } else {
          expected.clear(doc);
        }
      }
    }
    Bits acceptDocs = hasDelete ? accepted.asReadOnlyBits() : null;

    BitSetIterator exclusion = new BitSetIterator(excluded, excluded.cardinality());
    ReqExclBulkScorer scorer =
        new ReqExclBulkScorer(
            new ConstantScoreBulkScorer(
                0f,
                ScoreMode.COMPLETE_NO_SCORES,
                new BitSetIterator(required, required.cardinality())),
            exclusion);
    FixedBitSet actual = new FixedBitSet(maxDoc);
    assertEquals(
        DocIdSetIterator.NO_MORE_DOCS,
        scorer.score(
            new LeafCollector() {
              @Override
              public void setScorer(Scorable scorer) {}

              @Override
              public void collect(int doc) {
                actual.set(doc);
              }
            },
            acceptDocs,
            0,
            maxDoc));

    assertArrayEquals(expected.getBits(), actual.getBits());
  }

  public void testSparseScore() throws IOException {
    int maxDoc = 10_000;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    FixedBitSet accepted = null;
    FixedBitSet expected = new FixedBitSet(maxDoc);
    expected.set(0, maxDoc);

    excluded.set(42);
    excluded.set(600);

    expected.clear(42);
    expected.clear(600);

    boolean hasDelete = random().nextBoolean();
    if (hasDelete) {
      accepted = new FixedBitSet(maxDoc);
      accepted.set(0, maxDoc);
      accepted.clear(42);
      accepted.clear(601);
      expected.clear(42);
      expected.clear(600);
    }

    BitSetIterator exclusion = new BitSetIterator(excluded, excluded.cardinality());
    ReqExclBulkScorer scorer =
        new ReqExclBulkScorer(
            new ConstantScoreBulkScorer(
                0f, ScoreMode.COMPLETE_NO_SCORES, DocIdSetIterator.all(maxDoc)),
            exclusion);
    FixedBitSet collected = new FixedBitSet(maxDoc);
    assertEquals(
        DocIdSetIterator.NO_MORE_DOCS,
        scorer.score(
            new LeafCollector() {
              @Override
              public void setScorer(Scorable scorer) {}

              @Override
              public void collect(int doc) {
                collected.set(doc);
              }
            },
            null,
            0,
            maxDoc));

    assertArrayEquals(expected.getBits(), collected.getBits());
  }

  public void testWindowBitsApplyMask() {
    ReqExclBulkScorer.WindowBits windowBits = new ReqExclBulkScorer.WindowBits();
    windowBits.reset(100, 108, 1_000);
    windowBits.windowMask.set(1);
    windowBits.windowMask.set(3);
    windowBits.windowMask.set(6);

    // intersection
    FixedBitSet target = new FixedBitSet(12);
    target.set(0, 12);
    target.clear(5);
    windowBits.applyMask(target, 98);
    FixedBitSet expected = new FixedBitSet(12);
    // 100+1-98=3
    // 100+6-98=8
    expected.set(3);
    expected.set(8);
    assertArrayEquals(expected.getBits(), target.getBits());

    // no intersection
    target = new FixedBitSet(4);
    target.set(0, target.length());
    windowBits.applyMask(target, 96);
    assertEquals(0, target.cardinality());

    // no intersection
    target = new FixedBitSet(4);
    target.set(0, target.length());
    windowBits.applyMask(target, 108);
    assertEquals(0, target.cardinality());
  }
}
