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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Tests {@link DocValuesRangeIterator#forOrdinalRange} for both single-valued ({@link
 * SortedDocValues}) and multi-valued ({@link SortedSetDocValues}) ordinals. Parallel to {@link
 * TestDocValuesRangeIterator} and {@link TestSortedNumericDocValuesRangeIterator}.
 */
public class TestDocValuesOrdinalRangeIterator extends BaseDocValuesSkipperTests {

  private static final long QUERY_MIN = 10;
  private static final long QUERY_MAX = 20;

  // ---- Shared helpers ----

  private static boolean docHasValue(int doc) {
    return doc < 1024 || (doc < 2048 && (doc & 1) == 0);
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

  // ==========================================================================
  // SortedDocValues (single-valued ordinals)
  // ==========================================================================

  private static int singleOrdinal(int doc, long queryMin, long queryMax) {
    int d = doc % 1024;
    if (d < 128) {
      return (int) ((queryMin + queryMax) >> 1);
    } else if (d < 256) {
      return (int) (queryMax + 1);
    } else if (d < 512) {
      return (int) (queryMin - 1);
    } else {
      return switch ((d / 2) % 3) {
        case 0 -> (int) (queryMin - 1);
        case 1 -> (int) (queryMax + 1);
        case 2 -> (int) ((queryMin + queryMax) >> 1);
        default -> throw new AssertionError();
      };
    }
  }

  private static boolean singleOrdInRange(int doc) {
    int ord = singleOrdinal(doc, QUERY_MIN, QUERY_MAX);
    return ord >= QUERY_MIN && ord <= QUERY_MAX;
  }

  private static List<Integer> expectedSingleOrdMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && singleOrdInRange(doc)) {
        matches.add(doc);
      }
    }
    return matches;
  }

  private static SortedDocValues sortedDocValues(long queryMin, long queryMax) {
    return new SortedDocValues() {
      int doc = -1;

      @Override
      public int ordValue() {
        return singleOrdinal(doc, queryMin, queryMax);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getValueCount() {
        return (int) (queryMax + 10);
      }

      @Override
      public boolean advanceExact(int target) {
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
      public int advance(int target) {
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
      public long cost() {
        return 42;
      }
    };
  }

  private DocValuesRangeIterator createSortedDocValuesIterator(boolean multiLevel) {
    SortedDocValues values = sortedDocValues(QUERY_MIN, QUERY_MAX);
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, multiLevel);
    return DocValuesRangeIterator.forOrdinalRange(values, skipper, QUERY_MIN, QUERY_MAX);
  }

  // --- SortedDocValues: Correctness ---

  public void testSortedDocValuesCorrectResultsSingleLevel() throws IOException {
    assertEquals(expectedSingleOrdMatches(), collectMatches(createSortedDocValuesIterator(false)));
  }

  public void testSortedDocValuesCorrectResultsMultipleLevels() throws IOException {
    assertEquals(expectedSingleOrdMatches(), collectMatches(createSortedDocValuesIterator(true)));
  }

  // --- SortedDocValues: Approximation skipping ---

  public void testSortedDocValuesApproximationSkips() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    assertEquals(512, approx.advance(128));
    assertEquals(1536, approx.advance(1200));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, approx.advance(2048));
  }

  // --- SortedDocValues: Match classification ---

  public void testSortedDocValuesYesMatch() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertTrue(iter.matches());
  }

  public void testSortedDocValuesMaybeMatch() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());

    // d=512: (512/2)%3 = 1 → ord above range
    assertFalse(iter.matches());

    // d=514: (514/2)%3 = 2 → ord in range
    approx.advance(514);
    assertTrue(iter.matches());

    // d=516: (516/2)%3 = 0 → ord below range
    approx.advance(516);
    assertFalse(iter.matches());
  }

  public void testSortedDocValuesYesIfPresent() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertTrue(iter.matches());

    approx.advance(1025);
    assertFalse(iter.matches());
  }

  // --- SortedDocValues: docIdRunEnd ---

  public void testSortedDocValuesDocIdRunEnd() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(64);
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(512);
    assertEquals(513, iter.docIDRunEnd());

    approx.advance(1024);
    assertEquals(1025, iter.docIDRunEnd());
  }

  // ==========================================================================
  // SortedSetDocValues (multi-valued ordinals)
  // ==========================================================================

  /**
   * Returns the two sorted ordinals for a given doc. Same block-level min/max as the single-valued
   * case, but with multi-value behaviors:
   *
   * <ul>
   *   <li>Case 0 (mixed region): ords bracket the range — no match
   *   <li>Case 1 (mixed region): first ord in range — immediate match
   *   <li>Case 2 (mixed region): only second ord in range — match via iteration
   * </ul>
   */
  private static long[] ordinalPair(int doc, long queryMin, long queryMax) {
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

  private static boolean multiOrdInRange(int doc) {
    long[] ords = ordinalPair(doc, QUERY_MIN, QUERY_MAX);
    for (long ord : ords) {
      if (ord >= QUERY_MIN && ord <= QUERY_MAX) {
        return true;
      }
    }
    return false;
  }

  private static List<Integer> expectedMultiOrdMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && multiOrdInRange(doc)) {
        matches.add(doc);
      }
    }
    return matches;
  }

  private static SortedSetDocValues sortedSetDocValues(long queryMin, long queryMax) {
    return new SortedSetDocValues() {
      int doc = -1;
      int ordIdx = 0;

      @Override
      public long nextOrd() {
        long[] ords = ordinalPair(doc, queryMin, queryMax);
        return ords[ordIdx++];
      }

      @Override
      public int docValueCount() {
        return 2;
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getValueCount() {
        return queryMax + 10;
      }

      @Override
      public boolean advanceExact(int target) {
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
      public int advance(int target) {
        ordIdx = 0;
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
      public long cost() {
        return 42;
      }
    };
  }

  private DocValuesRangeIterator createSortedSetDocValuesIterator(boolean multiLevel) {
    SortedSetDocValues values = sortedSetDocValues(QUERY_MIN, QUERY_MAX);
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, multiLevel);
    return DocValuesRangeIterator.forOrdinalRange(values, skipper, QUERY_MIN, QUERY_MAX);
  }

  // --- SortedSetDocValues: Correctness ---

  public void testSortedSetDocValuesCorrectResultsSingleLevel() throws IOException {
    assertEquals(
        expectedMultiOrdMatches(), collectMatches(createSortedSetDocValuesIterator(false)));
  }

  public void testSortedSetDocValuesCorrectResultsMultipleLevels() throws IOException {
    assertEquals(expectedMultiOrdMatches(), collectMatches(createSortedSetDocValuesIterator(true)));
  }

  // --- SortedSetDocValues: Multi-value specific behavior ---

  public void testSortedSetMatchOnFirstOrdinal() throws IOException {
    // Doc 512: (512/2)%3 = 1 → ords [queryMin, queryMax+1]
    // First ordinal is in range → immediate match.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(512);
    assertTrue(iter.matches());
  }

  public void testSortedSetMatchOnSecondOrdinal() throws IOException {
    // Doc 514: (514/2)%3 = 2 → ords [queryMin-1, mid]
    // First ordinal below range, second ordinal in range → match.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(514);
    assertTrue(iter.matches());
  }

  public void testSortedSetNoMatchWhenOrdinalsBracketRange() throws IOException {
    // Doc 516: (516/2)%3 = 0 → ords [queryMin-1, queryMax+1]
    // Ordinals span below and above range, but none falls within it.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(516);
    assertFalse(iter.matches());
  }

  // --- SortedSetDocValues: Approximation skipping ---

  public void testSortedSetApproximationSkipsAboveRange() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(512, approx.advance(128));
  }

  public void testSortedSetApproximationSkipsBelowRange() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(512, approx.advance(300));
  }

  public void testSortedSetApproximationSkipsSparseNonMatching() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(1536, approx.advance(1200));
  }

  public void testSortedSetApproximationExhausted() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, approx.advance(2048));
  }

  // --- SortedSetDocValues: Match classification ---

  public void testSortedSetYesMatch() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertTrue(iter.matches());
  }

  public void testSortedSetYesMatchDoesNotAdvanceDocValues() throws IOException {
    SortedSetDocValues values = sortedSetDocValues(QUERY_MIN, QUERY_MAX);
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forOrdinalRange(values, skipper, QUERY_MIN, QUERY_MAX);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(100);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertTrue(iter.matches());
    assertTrue(values.docID() < approx.docID());
  }

  public void testSortedSetMaybeMatchChecksMultipleOrdinals() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // d=512: case 1 → [queryMin, queryMax+1] → match on first ordinal
    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertTrue(iter.matches());

    // d=514: case 2 → [queryMin-1, mid] → match on second ordinal
    approx.advance(514);
    assertTrue(iter.matches());

    // d=516: case 0 → [queryMin-1, queryMax+1] → no match (brackets range)
    approx.advance(516);
    assertFalse(iter.matches());
  }

  public void testSortedSetYesIfPresent() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertTrue(iter.matches());

    approx.advance(1025);
    assertFalse(iter.matches());
  }

  // --- SortedSetDocValues: docIdRunEnd ---

  public void testSortedSetDocIdRunEnd() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(64);
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertEquals(513, iter.docIDRunEnd());

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertEquals(1025, iter.docIDRunEnd());
  }
}
