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
import java.util.List;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * Tests {@link DocValuesRangeIterator#forRange(SortedNumericDocValues, DocValuesSkipper, long,
 * long)} with multi-valued documents. Parallel to {@link TestDocValuesRangeIterator} which tests
 * single-valued {@link org.apache.lucene.index.NumericDocValues}.
 *
 * <p>Each document has two values in sorted order. The three cases in the mixed region exercise
 * distinct multi-value behaviors:
 *
 * <ul>
 *   <li>Case 0: values bracket the query range (below and above) — no match
 *   <li>Case 1: first value is in range — immediate match
 *   <li>Case 2: only the second value is in range — match via iteration
 * </ul>
 */
public class TestSortedNumericDocValuesRangeIterator extends BaseDocValuesSkipperTests {

  private static final long QUERY_MIN = 10;
  private static final long QUERY_MAX = 20;

  /**
   * Returns the two sorted values for a given doc. Block-level min/max values are consistent with
   * the skipper from {@link BaseDocValuesSkipperTests#docValuesSkipper}.
   */
  private static long[] docValuePair(int doc, long queryMin, long queryMax) {
    int d = doc % 1024;
    long mid = (queryMin + queryMax) >> 1;
    if (d < 128) {
      return new long[] {queryMin, queryMax};
    } else if (d < 256) {
      return new long[] {queryMax + 1, queryMax + 1};
    } else if (d < 512) {
      return new long[] {queryMin - 1, queryMin - 1};
    } else {
      return switch ((d / 2) % 3) {
        case 0 -> new long[] {queryMin - 1, queryMax + 1};
        case 1 -> new long[] {queryMin, queryMax + 1};
        case 2 -> new long[] {queryMin - 1, mid};
        default -> throw new AssertionError();
      };
    }
  }

  private static boolean valueInRange(int doc) {
    long[] vals = docValuePair(doc, QUERY_MIN, QUERY_MAX);
    for (long v : vals) {
      if (v >= QUERY_MIN && v <= QUERY_MAX) {
        return true;
      }
    }
    return false;
  }

  private static boolean docHasValue(int doc) {
    return doc < 1024 || (doc < 2048 && (doc & 1) == 0);
  }

  private static List<Integer> expectedMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && valueInRange(doc)) {
        matches.add(doc);
      }
    }
    return matches;
  }

  private static SortedNumericDocValues sortedNumericDocValues(long queryMin, long queryMax) {
    return new SortedNumericDocValues() {
      int doc = -1;
      int valueIdx = 0;

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        valueIdx = 0;
        if (target < 1024) {
          return doc = target;
        } else if (target < 2048) {
          doc = target + (target & 1);
          return doc < 2048 ? doc : (doc = DocIdSetIterator.NO_MORE_DOCS);
        } else {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
      }

      @Override
      public int docValueCount() {
        return 2;
      }

      @Override
      public long nextValue() {
        long[] vals = docValuePair(doc, queryMin, queryMax);
        return vals[valueIdx++];
      }

      @Override
      public long cost() {
        return 42;
      }
    };
  }

  private static List<Integer> collectMatches(DocValuesRangeIterator iter) throws IOException {
    List<Integer> matches = new ArrayList<>();
    DocIdSetIterator approx = iter.approximation();
    while (approx.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      if (iter.matches()) {
        matches.add(approx.docID());
      }
    }
    return matches;
  }

  private DocValuesRangeIterator createIterator(boolean multiLevel) {
    SortedNumericDocValues values = sortedNumericDocValues(QUERY_MIN, QUERY_MAX);
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, multiLevel);
    return DocValuesRangeIterator.forRange(values, skipper, QUERY_MIN, QUERY_MAX);
  }

  // --- Correctness: all matching docs are found and no false positives ---

  public void testCorrectResultsSingleLevel() throws IOException {
    assertEquals(expectedMatches(), collectMatches(createIterator(false)));
  }

  public void testCorrectResultsMultipleLevels() throws IOException {
    assertEquals(expectedMatches(), collectMatches(createIterator(true)));
  }

  // --- Multi-value specific behavior ---

  public void testMatchOnFirstValueInRange() throws IOException {
    // Doc 512: (512/2)%3 = 1 → values [queryMin, queryMax+1]
    // First value is in range → immediate match.
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(512);
    assertTrue(iter.matches());
  }

  public void testMatchOnSecondValue() throws IOException {
    // Doc 514: (514/2)%3 = 2 → values [queryMin-1, mid]
    // First value below range (skipped), second value in range → match.
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(514);
    assertTrue(iter.matches());
  }

  public void testNoMatchWhenValuesBracketRange() throws IOException {
    // Doc 516: (516/2)%3 = 0 → values [queryMin-1, queryMax+1]
    // Values span below and above the range, but none falls within it.
    // The sorted-value optimization correctly rejects: first value < min
    // is skipped, second value >= min but > max → no match.
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(516);
    assertFalse(iter.matches());
  }

  // --- Approximation correctly skips non-matching blocks ---

  public void testApproximationSkipsAboveRangeBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(512, approx.advance(128));
  }

  public void testApproximationSkipsBelowRangeBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(512, approx.advance(300));
  }

  public void testApproximationSkipsFromEndOfMatchingBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(100, approx.advance(100));
    assertEquals(512, approx.advance(128));
  }

  public void testApproximationIteratesWithinMatchingBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(10, approx.advance(10));
    assertEquals(11, approx.nextDoc());
    assertEquals(12, approx.nextDoc());
    assertEquals(50, approx.advance(50));
  }

  public void testApproximationSkipsSparseNonMatchingBlocks() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(1536, approx.advance(1200));
  }

  public void testApproximationExhausted() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, approx.advance(2048));
  }

  // --- Match classification ---

  public void testYesMatchInDenseBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertTrue(iter.matches());
  }

  public void testYesMatchDoesNotAdvanceDocValues() throws IOException {
    SortedNumericDocValues values = sortedNumericDocValues(QUERY_MIN, QUERY_MAX);
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, QUERY_MIN, QUERY_MAX);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(100);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertTrue(iter.matches());
    assertTrue(values.docID() < approx.docID());
  }

  public void testMaybeMatchClassification() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
  }

  public void testMaybeMatchChecksMultipleValues() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // d=512: case 1 → [queryMin, queryMax+1] → match on first value
    approx.advance(512);
    assertTrue(iter.matches());

    // d=514: case 2 → [queryMin-1, mid] → match on second value
    approx.advance(514);
    assertTrue(iter.matches());

    // d=515: case 2 → same pair → match on second value
    approx.advance(515);
    assertTrue(iter.matches());

    // d=516: case 0 → [queryMin-1, queryMax+1] → no match (brackets range)
    approx.advance(516);
    assertFalse(iter.matches());
  }

  public void testYesIfPresentInSparseBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    // Even doc has a value → match
    assertTrue(iter.matches());

    approx.advance(1025);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    // Odd doc has no value → no match
    assertFalse(iter.matches());
  }

  // --- docIdRunEnd ---

  public void testDocIdRunEndYesBlockMultipleLevels() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(128, iter.docIDRunEnd());
  }

  public void testDocIdRunEndExtendsFromMiddleOfBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(64);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(100);
    assertEquals(128, iter.docIDRunEnd());
  }

  public void testDocIdRunEndConsistentAcrossYesBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    for (int doc = 0; doc < 128; doc++) {
      approx.advance(doc);
      assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
      assertEquals(
          "docIdRunEnd at doc " + doc + " should extend to level 1 boundary",
          128,
          iter.docIDRunEnd());
    }
  }

  public void testDocIdRunEndMaybeBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertEquals(513, iter.docIDRunEnd());

    approx.advance(800);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertEquals(801, iter.docIDRunEnd());
  }

  public void testDocIdRunEndYesIfPresentBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertEquals(1025, iter.docIDRunEnd());
  }
}
