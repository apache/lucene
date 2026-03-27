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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;

public class TestDocValuesRangeIterator extends BaseDocValuesSkipperTests {

  public void testSingleLevel() throws IOException {
    doTestBasics(false);
  }

  public void testMultipleLevels() throws IOException {
    doTestBasics(true);
  }

  private void doTestBasics(boolean doLevels) throws IOException {
    long queryMin = 10;
    long queryMax = 20;

    // Test with both gaps and no-gaps in the ranges:
    NumericDocValues values = docValues(queryMin, queryMax);
    NumericDocValues values2 = docValues(queryMin, queryMax);

    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    TwoPhaseIterator twoPhase = twoPhaseIterator(values, queryMin, queryMax, twoPhaseCalled);
    AtomicBoolean twoPhaseCalled2 = new AtomicBoolean();
    TwoPhaseIterator twoPhase2 = twoPhaseIterator(values2, queryMin, queryMax, twoPhaseCalled2);

    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, doLevels);
    DocValuesSkipper skipper2 = docValuesSkipper(queryMin, queryMax, doLevels);

    DocValuesRangeIterator rangeIterator =
        new DocValuesRangeIterator(twoPhase, skipper, queryMin, queryMax, false);
    DocValuesRangeIterator rangeIteratorWithGaps =
        new DocValuesRangeIterator(twoPhase2, skipper2, queryMin, queryMax, true);
    DocValuesRangeIterator.Approximation rangeApproximation =
        (DocValuesRangeIterator.Approximation) rangeIterator.approximation();
    DocValuesRangeIterator.Approximation rangeApproximationWithGaps =
        (DocValuesRangeIterator.Approximation) rangeIteratorWithGaps.approximation();

    assertEquals(100, rangeApproximation.advance(100));
    assertEquals(100, rangeApproximationWithGaps.advance(100));
    assertEquals(DocValuesRangeIterator.Match.YES, rangeApproximation.match);
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximationWithGaps.match);
    assertEquals(255, rangeApproximation.upTo);
    assertEquals(256, rangeIterator.docIDRunEnd());
    if (doLevels) {
      assertEquals(127, rangeApproximationWithGaps.upTo);
    } else {
      assertEquals(255, rangeApproximationWithGaps.upTo);
    }
    assertEquals(rangeApproximationWithGaps.docID(), rangeIteratorWithGaps.docIDRunEnd());
    assertTrue(rangeIterator.matches());
    assertTrue(rangeIteratorWithGaps.matches());
    assertTrue(values.docID() < rangeApproximation.docID()); // we did not advance doc values
    assertEquals(
        values2.docID(), rangeApproximationWithGaps.docID()); // we _did_ advance doc values
    assertFalse(twoPhaseCalled.get());
    assertTrue(twoPhaseCalled2.get());
    twoPhaseCalled2.set(false);

    assertEquals(768, rangeApproximation.advance(300));
    assertEquals(768, rangeApproximationWithGaps.advance(300));
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximation.match);
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximationWithGaps.match);
    assertEquals(rangeApproximation.docID(), rangeIterator.docIDRunEnd());
    assertEquals(rangeApproximationWithGaps.docID(), rangeIteratorWithGaps.docIDRunEnd());
    if (doLevels) {
      assertEquals(831, rangeApproximation.upTo);
      assertEquals(831, rangeApproximationWithGaps.upTo);
    } else {
      assertEquals(1023, rangeApproximation.upTo);
      assertEquals(1023, rangeApproximationWithGaps.upTo);
    }
    for (int i = 0; i < 10; ++i) {
      assertEquals(values.docID(), rangeApproximation.docID());
      assertEquals(values2.docID(), rangeApproximationWithGaps.docID());
      assertEquals(twoPhase.matches(), rangeIterator.matches());
      assertEquals(twoPhase2.matches(), rangeIteratorWithGaps.matches());
      assertTrue(twoPhaseCalled.get());
      assertTrue(twoPhaseCalled2.get());
      twoPhaseCalled.set(false);
      twoPhaseCalled2.set(false);
      rangeApproximation.nextDoc();
      rangeApproximationWithGaps.nextDoc();
    }

    assertEquals(1100, rangeApproximation.advance(1099));
    assertEquals(1100, rangeApproximationWithGaps.advance(1099));
    assertEquals(DocValuesRangeIterator.Match.IF_DOC_HAS_VALUE, rangeApproximation.match);
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximationWithGaps.match);
    assertEquals(1024 + 256 - 1, rangeApproximation.upTo);
    assertEquals(rangeApproximation.docID(), rangeIterator.docIDRunEnd());
    assertEquals(rangeApproximationWithGaps.docID(), rangeIteratorWithGaps.docIDRunEnd());
    if (doLevels) {
      assertEquals(1024 + 128 - 1, rangeApproximationWithGaps.upTo);
    } else {
      assertEquals(1024 + 256 - 1, rangeApproximationWithGaps.upTo);
    }
    assertEquals(values.docID(), rangeApproximation.docID());
    assertEquals(values2.docID(), rangeApproximationWithGaps.docID());
    assertTrue(rangeIterator.matches());
    assertTrue(rangeIteratorWithGaps.matches());
    assertFalse(twoPhaseCalled.get());
    assertTrue(twoPhaseCalled2.get());
    twoPhaseCalled2.set(false);

    assertEquals(1024 + 768, rangeApproximation.advance(1024 + 300));
    assertEquals(1024 + 768, rangeApproximationWithGaps.advance(1024 + 300));
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximation.match);
    assertEquals(DocValuesRangeIterator.Match.MAYBE, rangeApproximationWithGaps.match);
    assertEquals(rangeApproximation.docID(), rangeIterator.docIDRunEnd());
    assertEquals(rangeApproximationWithGaps.docID(), rangeIteratorWithGaps.docIDRunEnd());
    if (doLevels) {
      assertEquals(1024 + 831, rangeApproximation.upTo);
      assertEquals(1024 + 831, rangeApproximationWithGaps.upTo);
    } else {
      assertEquals(2047, rangeApproximation.upTo);
      assertEquals(2047, rangeApproximationWithGaps.upTo);
    }
    for (int i = 0; i < 10; ++i) {
      assertEquals(values.docID(), rangeApproximation.docID());
      assertEquals(values2.docID(), rangeApproximationWithGaps.docID());
      assertEquals(twoPhase.matches(), rangeIterator.matches());
      assertEquals(twoPhase2.matches(), rangeIteratorWithGaps.matches());
      assertTrue(twoPhaseCalled.get());
      assertTrue(twoPhaseCalled2.get());
      twoPhaseCalled.set(false);
      twoPhaseCalled2.set(false);
      rangeApproximation.nextDoc();
      rangeApproximationWithGaps.nextDoc();
    }

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, rangeApproximation.advance(2048));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, rangeApproximationWithGaps.advance(2048));
  }

  private static TwoPhaseIterator twoPhaseIterator(
      NumericDocValues values, long queryMin, long queryMax, AtomicBoolean twoPhaseCalled) {
    return new TwoPhaseIterator(values) {

      @Override
      public boolean matches() throws IOException {
        twoPhaseCalled.set(true);
        long v = values.longValue();
        return v >= queryMin && v <= queryMax;
      }

      @Override
      public float matchCost() {
        return 2f; // 2 comparisons
      }
    };
  }
}
