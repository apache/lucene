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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

/**
 * Tests {@code DocValuesRangeIterator#forOrdinalSet} for both single-valued ({@link
 * SortedDocValues}) and multi-valued ({@link SortedSetDocValues}) ordinals.
 *
 * <p>Unlike the ordinal range tests, the ordinal set has a gap: ordinal 15 (the midpoint of [10,
 * 20]) is excluded. This means docs with ordinal 15 are within the bounding range but NOT in the
 * matching set, exercising the predicate-always-checked behavior of {@code alwaysCheckPredicate}.
 */
public class TestDocValuesOrdinalSetIterator extends BaseDocValuesSkipperTests {

  private static final long QUERY_MIN = 10;
  private static final long QUERY_MAX = 20;
  private static final long GAP_ORD = 15;

  /** Matching ordinals: all of [10, 20] except 15. */
  private static final long[] MATCHING_ORDS = {10, 11, 12, 13, 14, 16, 17, 18, 19, 20};

  /**
   * Fake TermsEnum that iterates over a fixed array of ordinals. Only {@code next()} and {@code
   * ord()} are needed by {@code DocValuesRangeIterator#forOrdinalSet()}.
   */
  private static TermsEnum fakeTermsEnum(long[] ordinals) {
    return new TermsEnum() {
      int idx = -1;

      @Override
      public BytesRef next() {
        idx++;
        if (idx >= ordinals.length) {
          return null;
        }
        return new BytesRef("term" + ordinals[idx]);
      }

      @Override
      public long ord() {
        return ordinals[idx];
      }

      @Override
      public AttributeSource attributes() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean seekExact(BytesRef text) {
        throw new UnsupportedOperationException();
      }

      @Override
      public IOBooleanSupplier prepareSeekExact(BytesRef text) {
        throw new UnsupportedOperationException();
      }

      @Override
      public SeekStatus seekCeil(BytesRef text) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(long ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(BytesRef term, TermState state) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BytesRef term() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long totalTermFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ImpactsEnum impacts(int flags) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TermState termState() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static boolean ordInSet(long ord) {
    return ord >= QUERY_MIN && ord <= QUERY_MAX && ord != GAP_ORD;
  }

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
  // SortedDocValues (single-valued ordinals with a set)
  // ==========================================================================

  /**
   * Single ordinal per doc. Uses ordinal 14 (in the set) for the in-range region, and ordinal 15
   * (in the gap) for the mixed region's case 2.
   */
  private static int singleOrdinal(int doc) {
    int d = doc % 1024;
    if (d < 128) {
      return 14;
    } else if (d < 256) {
      return (int) (QUERY_MAX + 1);
    } else if (d < 512) {
      return (int) (QUERY_MIN - 1);
    } else {
      return switch ((d / 2) % 3) {
        case 0 -> (int) (QUERY_MIN - 1);
        case 1 -> (int) (QUERY_MAX + 1);
        case 2 -> (int) GAP_ORD;
        default -> throw new AssertionError();
      };
    }
  }

  private static List<Integer> expectedSingleOrdMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && ordInSet(singleOrdinal(doc))) {
        matches.add(doc);
      }
    }
    return matches;
  }

  private static SortedDocValues sortedDocValues() {
    return new SortedDocValues() {
      int doc = -1;

      @Override
      public int ordValue() {
        return singleOrdinal(doc);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getValueCount() {
        return (int) (QUERY_MAX + 10);
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

  private DocValuesRangeIterator createSortedDocValuesIterator(boolean multiLevel)
      throws IOException {
    SortedDocValues values = sortedDocValues();
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, multiLevel);
    return DocValuesRangeIterator.forOrdinalSet(values, skipper, fakeTermsEnum(MATCHING_ORDS));
  }

  // --- SortedDocValues: Correctness ---

  public void testSortedDocValuesCorrectResultsSingleLevel() throws IOException {
    assertEquals(expectedSingleOrdMatches(), collectMatches(createSortedDocValuesIterator(false)));
  }

  public void testSortedDocValuesCorrectResultsMultipleLevels() throws IOException {
    assertEquals(expectedSingleOrdMatches(), collectMatches(createSortedDocValuesIterator(true)));
  }

  // --- SortedDocValues: Gap ordinal is rejected ---

  public void testSortedDocValuesGapOrdinalRejected() throws IOException {
    // Doc 514: (514/2)%3 = 2 → ordinal 15 (in [10,20] but NOT in set)
    // With forOrdinalRange this would match; with forOrdinalSet it must not.
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(514);
    assertFalse(iter.matches());
  }

  public void testSortedDocValuesInSetOrdinalAccepted() throws IOException {
    // Doc 0: ordinal 14 (in set) → match
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(0);
    assertTrue(iter.matches());
  }

  // --- SortedDocValues: YES blocks still check predicate ---

  public void testSortedDocValuesYesBlockChecksPredicate() throws IOException {
    SortedDocValues values = sortedDocValues();
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forOrdinalSet(values, skipper, fakeTermsEnum(MATCHING_ORDS));
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Block 0-63 is classified YES, but predicate is still checked
    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    // ordinal 14 is in the set → match
    assertTrue(iter.matches());
    // doc values iterator was advanced (predicate was checked)
    assertEquals(0, values.docID());
  }

  // --- SortedDocValues: Approximation skipping ---

  public void testSortedDocValuesApproximationSkips() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    assertEquals(512, approx.advance(128));
    assertEquals(1536, approx.advance(1200));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, approx.advance(2048));
  }

  // --- SortedDocValues: docIdRunEnd is conservative with ordinal sets ---

  public void testSortedDocValuesDocIdRunEndAlwaysDocPlusOne() throws IOException {
    DocValuesRangeIterator iter = createSortedDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Even in a YES block, docIdRunEnd returns doc+1 because ordinal sets
    // may have gaps that the block-level check cannot detect.
    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(1, iter.docIDRunEnd());

    approx.advance(64);
    assertEquals(65, iter.docIDRunEnd());

    // MAYBE block: same behavior as forOrdinalRange
    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertEquals(513, iter.docIDRunEnd());

    // YES_IF_PRESENT block: also doc+1
    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertEquals(1025, iter.docIDRunEnd());
  }

  // ==========================================================================
  // SortedSetDocValues (multi-valued ordinals with a set)
  // ==========================================================================

  /**
   * Two sorted ordinals per doc. In the mixed region:
   *
   * <ul>
   *   <li>Case 0: [9, 21] — both outside set → no match
   *   <li>Case 1: [10, 21] — first ordinal 10 is in set → match
   *   <li>Case 2: [9, 15] — second ordinal 15 is in gap → no match (unlike forOrdinalRange!)
   * </ul>
   */
  private static long[] ordinalPair(int doc) {
    int d = doc % 1024;
    if (d < 128) {
      return new long[] {QUERY_MIN, QUERY_MAX};
    } else if (d < 256) {
      return new long[] {QUERY_MAX + 1, QUERY_MAX + 1};
    } else if (d < 512) {
      return new long[] {QUERY_MIN - 1, QUERY_MIN - 1};
    } else {
      return switch ((d / 2) % 3) {
        case 0 -> new long[] {QUERY_MIN - 1, QUERY_MAX + 1};
        case 1 -> new long[] {QUERY_MIN, QUERY_MAX + 1};
        case 2 -> new long[] {QUERY_MIN - 1, GAP_ORD};
        default -> throw new AssertionError();
      };
    }
  }

  private static boolean multiOrdInSet(int doc) {
    long[] ords = ordinalPair(doc);
    for (long ord : ords) {
      if (ordInSet(ord)) {
        return true;
      }
    }
    return false;
  }

  private static List<Integer> expectedMultiOrdMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && multiOrdInSet(doc)) {
        matches.add(doc);
      }
    }
    return matches;
  }

  private static SortedSetDocValues sortedSetDocValues() {
    return new SortedSetDocValues() {
      int doc = -1;
      int ordIdx = 0;

      @Override
      public long nextOrd() {
        long[] ords = ordinalPair(doc);
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
        return QUERY_MAX + 10;
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

  private DocValuesRangeIterator createSortedSetDocValuesIterator(boolean multiLevel)
      throws IOException {
    SortedSetDocValues values = sortedSetDocValues();
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, multiLevel);
    return DocValuesRangeIterator.forOrdinalSet(values, skipper, fakeTermsEnum(MATCHING_ORDS));
  }

  // --- SortedSetDocValues: Correctness ---

  public void testSortedSetDocValuesCorrectResultsSingleLevel() throws IOException {
    assertEquals(
        expectedMultiOrdMatches(), collectMatches(createSortedSetDocValuesIterator(false)));
  }

  public void testSortedSetDocValuesCorrectResultsMultipleLevels() throws IOException {
    assertEquals(expectedMultiOrdMatches(), collectMatches(createSortedSetDocValuesIterator(true)));
  }

  // --- SortedSetDocValues: Gap ordinal behavior ---

  public void testSortedSetGapOrdinalRejected() throws IOException {
    // Doc 514: case 2 → ords [9, 15]. Ordinal 15 is in [10,20] but NOT in set.
    // With forOrdinalRange this would match; with forOrdinalSet it must not.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(514);
    assertFalse(iter.matches());
  }

  public void testSortedSetMatchOnFirstOrdinalInSet() throws IOException {
    // Doc 512: case 1 → ords [10, 21]. Ordinal 10 is in set → match.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(512);
    assertTrue(iter.matches());
  }

  public void testSortedSetNoMatchWhenOrdinalsBracketRange() throws IOException {
    // Doc 516: case 0 → ords [9, 21]. Neither ordinal is in the set.
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();
    approx.advance(516);
    assertFalse(iter.matches());
  }

  // --- SortedSetDocValues: YES blocks still check predicate ---

  public void testSortedSetYesBlockChecksPredicate() throws IOException {
    SortedSetDocValues values = sortedSetDocValues();
    DocValuesSkipper skipper = docValuesSkipper(QUERY_MIN, QUERY_MAX, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forOrdinalSet(values, skipper, fakeTermsEnum(MATCHING_ORDS));
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    // Ords [10, 20]: 10 is in set → match, but only because predicate was checked
    assertTrue(iter.matches());
    // doc values iterator was advanced (predicate was checked)
    assertEquals(0, values.docID());
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

  // --- SortedSetDocValues: docIdRunEnd ---

  public void testSortedSetDocIdRunEndAlwaysDocPlusOne() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(1, iter.docIDRunEnd());

    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
    assertEquals(513, iter.docIDRunEnd());

    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertEquals(1025, iter.docIDRunEnd());
  }

  // --- SortedSetDocValues: YES_IF_PRESENT ---

  public void testSortedSetYesIfPresent() throws IOException {
    DocValuesRangeIterator iter = createSortedSetDocValuesIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Sparse region: even doc has value, odd doc does not
    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    // Ords [10, 20], 10 in set → match
    assertTrue(iter.matches());

    approx.advance(1025);
    // Odd doc has no value → no match
    assertFalse(iter.matches());
  }
}
