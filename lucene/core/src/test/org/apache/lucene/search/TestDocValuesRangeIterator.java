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
import org.apache.lucene.index.NumericDocValues;

public class TestDocValuesRangeIterator extends BaseDocValuesSkipperTests {

  private static final long QUERY_MIN = 10;
  private static final long QUERY_MAX = 20;

  private static boolean valueInRange(int doc) {
    int d = doc % 1024;
    long value;
    if (d < 128) {
      value = (QUERY_MIN + QUERY_MAX) >> 1;
    } else if (d < 256) {
      value = QUERY_MAX + 1;
    } else if (d < 512) {
      value = QUERY_MIN - 1;
    } else {
      value =
          switch ((d / 2) % 3) {
            case 0 -> QUERY_MIN - 1;
            case 1 -> QUERY_MAX + 1;
            case 2 -> (QUERY_MIN + QUERY_MAX) >> 1;
            default -> throw new AssertionError();
          };
    }
    return value >= QUERY_MIN && value <= QUERY_MAX;
  }

  private static boolean docHasValue(int doc) {
    return doc < 1024 || (doc < 2048 && (doc & 1) == 0);
  }

  /**
   * Compute expected matching docs. With the corrected aggregating skipper, YES and YES_IF_PRESENT
   * blocks only contain docs whose values are genuinely within the query range, so the expected
   * matches are simply all docs that have a value in range.
   */
  private static List<Integer> expectedMatches() {
    List<Integer> matches = new ArrayList<>();
    for (int doc = 0; doc < 2048; doc++) {
      if (docHasValue(doc) && valueInRange(doc)) {
        matches.add(doc);
      }
    }
    return matches;
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
    NumericDocValues values = docValues(QUERY_MIN, QUERY_MAX);
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

  // --- Approximation correctly skips non-matching blocks ---

  public void testApproximationSkipsAboveRangeBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Docs 128-511 are in blocks above or below the query range.
    // The first MAYBE block starts at 512 (where mixed values begin).
    assertEquals(512, approx.advance(128));
  }

  public void testApproximationSkipsBelowRangeBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Docs 256-511 are below range; skip to the first MAYBE block at 512.
    assertEquals(512, approx.advance(300));
  }

  public void testApproximationSkipsFromEndOfMatchingBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    assertEquals(100, approx.advance(100));
    // Advancing past the last YES block boundary at 127 should skip to 512
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

    // In the sparse region (1024-2047), docs 1152-1535 are in non-matching
    // blocks (above-range then below-range), so they should be skipped.
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
    NumericDocValues values = docValues(QUERY_MIN, QUERY_MAX);
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

    // Docs 512+ are in blocks with mixed values -> MAYBE
    approx.advance(512);
    assertEquals(SkipBlockRangeIterator.Match.MAYBE, approx.getMatch());
  }

  public void testMaybeMatchChecksValues() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    approx.advance(512);

    // d=512: (512/2)%3 = 1 -> value above range
    assertFalse(iter.matches());

    // d=514: (514/2)%3 = 2 -> value in range
    approx.advance(514);
    assertTrue(iter.matches());

    // d=515: (515/2)%3 = 2 -> value in range
    approx.advance(515);
    assertTrue(iter.matches());

    // d=516: (516/2)%3 = 0 -> value below range
    approx.advance(516);
    assertFalse(iter.matches());
  }

  public void testYesIfPresentInSparseBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // Sparse block with values in range: all values match but not all docs
    // have a value, so the block is classified YES_IF_PRESENT.
    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    // Even doc has a value -> match
    assertTrue(iter.matches());

    approx.advance(1025);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    // Odd doc has no value -> no match
    assertFalse(iter.matches());
  }

  // --- docIdRunEnd ---

  public void testDocIdRunEndYesBlockMultipleLevels() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // 3 levels: level 0 blocks are 64 docs wide.
    // At doc 0: level 0 block is 0-63 (YES). Level 1 block 0-127 also has
    // all values in range, so docIdRunEnd extends to 128. But level 2 block
    // 0-255 includes docs 128-255 with values above range, so the
    // containment check fails and the run stops at the level 1 boundary.
    approx.advance(0);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(128, iter.docIDRunEnd());
  }

  public void testDocIdRunEndExtendsFromMiddleOfBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // At doc 64: level 0 block is 64-127, level 1 is 0-127 (all in range).
    // Level 2 is 0-255 which includes above-range values -> stop at level 1.
    approx.advance(64);
    assertEquals(SkipBlockRangeIterator.Match.YES, approx.getMatch());
    assertEquals(128, iter.docIDRunEnd());

    approx.advance(100);
    assertEquals(128, iter.docIDRunEnd());
  }

  public void testDocIdRunEndConsistentAcrossYesBlock() throws IOException {
    DocValuesRangeIterator iter = createIterator(true);
    SkipBlockRangeIterator approx = (SkipBlockRangeIterator) iter.approximation();

    // With 3 levels, docs 0-127 are in YES blocks (two level-0 blocks:
    // 0-63 and 64-127). Both extend to the level 1 boundary at 128, but
    // not further (level 2 includes above-range docs 128-255).
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

    // MAYBE blocks cannot guarantee all docs match, so docIdRunEnd = doc + 1
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

    // YES_IF_PRESENT blocks have gaps, so docIdRunEnd = doc + 1
    approx.advance(1024);
    assertEquals(SkipBlockRangeIterator.Match.YES_IF_PRESENT, approx.getMatch());
    assertEquals(1025, iter.docIDRunEnd());
  }
}
