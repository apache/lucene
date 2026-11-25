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
package org.apache.lucene.index;

import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/** Tests the strided per-term layout of {@link FreqProxPostingsArray} in isolation. */
public class TestFreqProxPostingsArray extends LuceneTestCase {

  public void testDocs() {
    doTest(false, false, false, ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES);
  }

  public void testDocsAndFreqs() {
    doTest(true, false, false, ParallelPostingsArray.BYTES_PER_POSTING + 3 * Integer.BYTES);
  }

  public void testDocsAndFreqsAndPositions() {
    doTest(true, true, false, ParallelPostingsArray.BYTES_PER_POSTING + 4 * Integer.BYTES);
  }

  public void testDocsAndFreqsAndPositionsAndOffsets() {
    doTest(true, true, true, ParallelPostingsArray.BYTES_PER_POSTING + 5 * Integer.BYTES);
  }

  public void testInvalidLayouts() {
    assumeTrue("requires assertions", TEST_ASSERTS_ENABLED);
    // offsets imply positions, which imply freqs
    expectThrows(AssertionError.class, () -> new FreqProxPostingsArray(4, false, true, false));
    expectThrows(AssertionError.class, () -> new FreqProxPostingsArray(4, false, false, true));
    expectThrows(AssertionError.class, () -> new FreqProxPostingsArray(4, true, false, true));
  }

  public void testAccessorsCheckLayout() {
    assumeTrue("requires assertions", TEST_ASSERTS_ENABLED);
    FreqProxPostingsArray docs = new FreqProxPostingsArray(4, false, false, false);
    expectThrows(AssertionError.class, () -> docs.termFreq(0));
    expectThrows(AssertionError.class, () -> docs.setTermFreq(0, 1));
    expectThrows(AssertionError.class, () -> docs.lastPosition(0));
    expectThrows(AssertionError.class, () -> docs.setLastPosition(0, 1));
    expectThrows(AssertionError.class, () -> docs.lastStartOffset(0));
    expectThrows(AssertionError.class, () -> docs.setLastStartOffset(0, 1));

    FreqProxPostingsArray freqs = new FreqProxPostingsArray(4, true, false, false);
    freqs.setTermFreq(0, 1);
    expectThrows(AssertionError.class, () -> freqs.lastPosition(0));
    expectThrows(AssertionError.class, () -> freqs.lastStartOffset(0));

    FreqProxPostingsArray positions = new FreqProxPostingsArray(4, true, true, false);
    positions.setLastPosition(0, 1);
    expectThrows(AssertionError.class, () -> positions.lastStartOffset(0));
  }

  private void doTest(boolean freqs, boolean prox, boolean offsets, int expectedBytesPerPosting) {
    final int size = TestUtil.nextInt(random(), 1, 64);
    FreqProxPostingsArray array = new FreqProxPostingsArray(size, freqs, prox, offsets);
    assertEquals(size, array.size);
    assertEquals(freqs, array.hasFreqs());
    assertEquals(prox, array.hasPositions());
    assertEquals(offsets, array.hasOffsets());
    assertEquals(expectedBytesPerPosting, array.bytesPerPosting());

    // every slot starts out zeroed, newTerm relies on this for lastStartOffset
    for (int termID = 0; termID < size; termID++) {
      assertZero(array, termID);
    }

    // writing one term must not touch its neighbours
    final int termID = random().nextInt(size);
    fill(array, termID);
    for (int other = 0; other < size; other++) {
      if (other != termID) {
        assertZero(array, other);
      }
    }
    verify(array, termID);

    for (int t = 0; t < size; t++) {
      fill(array, t);
    }
    for (int t = 0; t < size; t++) {
      verify(array, t);
    }

    // newInstance keeps the layout
    FreqProxPostingsArray fresh = (FreqProxPostingsArray) array.newInstance(size + 3);
    assertEquals(size + 3, fresh.size);
    assertEquals(freqs, fresh.hasFreqs());
    assertEquals(prox, fresh.hasPositions());
    assertEquals(offsets, fresh.hasOffsets());
    assertEquals(expectedBytesPerPosting, fresh.bytesPerPosting());

    // grow copies every slot of every term and zeroes the new ones
    FreqProxPostingsArray grown = (FreqProxPostingsArray) array.grow();
    assertTrue(grown.size > size);
    assertEquals(expectedBytesPerPosting, grown.bytesPerPosting());
    for (int t = 0; t < size; t++) {
      verify(grown, t);
    }
    for (int t = size; t < grown.size; t++) {
      assertZero(grown, t);
    }

    // a partial copy leaves the tail of the destination untouched
    FreqProxPostingsArray partial = (FreqProxPostingsArray) array.newInstance(size);
    final int numToCopy = random().nextInt(size + 1);
    array.copyTo(partial, numToCopy);
    for (int t = 0; t < numToCopy; t++) {
      verify(partial, t);
    }
    for (int t = numToCopy; t < size; t++) {
      assertZero(partial, t);
    }
  }

  /** Writes a term-specific value into every slot of the given term, base arrays included. */
  private static void fill(FreqProxPostingsArray array, int termID) {
    array.textStarts[termID] = textStart(termID);
    array.addressOffset[termID] = addressOffset(termID);
    array.byteStarts[termID] = byteStart(termID);
    array.setLastDocID(termID, lastDocID(termID));
    array.setLastDocCode(termID, lastDocCode(termID));
    if (array.hasFreqs()) {
      array.setTermFreq(termID, termFreq(termID));
    }
    if (array.hasPositions()) {
      array.setLastPosition(termID, lastPosition(termID));
    }
    if (array.hasOffsets()) {
      array.setLastStartOffset(termID, lastStartOffset(termID));
    }
  }

  private static void verify(FreqProxPostingsArray array, int termID) {
    assertEquals(textStart(termID), array.textStarts[termID]);
    assertEquals(addressOffset(termID), array.addressOffset[termID]);
    assertEquals(byteStart(termID), array.byteStarts[termID]);
    assertEquals(lastDocID(termID), array.lastDocID(termID));
    assertEquals(lastDocCode(termID), array.lastDocCode(termID));
    if (array.hasFreqs()) {
      assertEquals(termFreq(termID), array.termFreq(termID));
    }
    if (array.hasPositions()) {
      assertEquals(lastPosition(termID), array.lastPosition(termID));
    }
    if (array.hasOffsets()) {
      assertEquals(lastStartOffset(termID), array.lastStartOffset(termID));
    }
  }

  private static void assertZero(FreqProxPostingsArray array, int termID) {
    assertEquals(0, array.textStarts[termID]);
    assertEquals(0, array.addressOffset[termID]);
    assertEquals(0, array.byteStarts[termID]);
    assertEquals(0, array.lastDocID(termID));
    assertEquals(0, array.lastDocCode(termID));
    if (array.hasFreqs()) {
      assertEquals(0, array.termFreq(termID));
    }
    if (array.hasPositions()) {
      assertEquals(0, array.lastPosition(termID));
    }
    if (array.hasOffsets()) {
      assertEquals(0, array.lastStartOffset(termID));
    }
  }

  // Distinct, slot-specific values so that a value landing in the wrong slot or the wrong term is
  // detected.
  private static int textStart(int termID) {
    return 1_000_000 + termID;
  }

  private static int addressOffset(int termID) {
    return 2_000_000 + termID;
  }

  private static int byteStart(int termID) {
    return 3_000_000 + termID;
  }

  private static int lastDocID(int termID) {
    return 10_000 + termID;
  }

  private static int lastDocCode(int termID) {
    return 20_000 + termID;
  }

  private static int termFreq(int termID) {
    return 30_000 + termID;
  }

  private static int lastPosition(int termID) {
    return 40_000 + termID;
  }

  private static int lastStartOffset(int termID) {
    return 50_000 + termID;
  }
}
