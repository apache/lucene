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
package org.apache.lucene.util;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestBitSetIterator extends LuceneTestCase {

  public void testDocIDRunEndContiguous() {
    final int totalDocs = random().nextInt(1_000, 10_000);
    final int minDoc = random().nextInt(totalDocs - 2);
    final int maxDoc = random().nextInt(minDoc + 1, totalDocs);
    final BitSet fbs =
        random().nextBoolean() ? new FixedBitSet(totalDocs) : new SparseFixedBitSet(totalDocs);
    for (int d = minDoc; d < maxDoc; d++) {
      fbs.set(d);
    }
    final BitSetIterator it = new BitSetIterator(fbs, fbs.cardinality());
    assertEquals(minDoc, it.advance(0));
    assertEquals(maxDoc, it.docIDRunEnd());
    assertEquals(NO_MORE_DOCS, it.advance(maxDoc));
  }

  public void testDocIDRunEndFullFixedBitSet() {
    final int n = 500;
    final FixedBitSet fbs = new FixedBitSet(n);
    fbs.set(0, n);
    final BitSetIterator it = new BitSetIterator(fbs, n);
    assertEquals(0, it.nextDoc());
    assertEquals(n, it.docIDRunEnd());
    assertEquals(42, it.advance(42));
    assertEquals(n, it.docIDRunEnd());
  }

  public void testDocIDRunEndFullSparseBitSet() {
    final int n = 500;
    final SparseFixedBitSet bs = new SparseFixedBitSet(n);
    for (int i = 0; i < n; i++) {
      bs.set(i);
    }
    final BitSetIterator it = new BitSetIterator(bs, n);
    assertEquals(0, it.nextDoc());
    assertEquals(n, it.docIDRunEnd());
    assertEquals(42, it.advance(42));
    assertEquals(n, it.docIDRunEnd());
  }

  public void testDocIDRunEndAdvanceFixedBitSet() {
    final FixedBitSet fbs = new FixedBitSet(2_000);
    for (int d = 100; d < 800; d++) {
      fbs.set(d);
    }
    final BitSetIterator it = new BitSetIterator(fbs, fbs.cardinality());
    assertEquals(100, it.advance(50));
    assertEquals(expectedDocIDRunEnd(fbs, 100), it.docIDRunEnd());
    assertEquals(400, it.advance(400));
    assertEquals(expectedDocIDRunEnd(fbs, 400), it.docIDRunEnd());
    assertEquals(799, it.advance(799));
    assertEquals(expectedDocIDRunEnd(fbs, 799), it.docIDRunEnd());
  }

  public void testDocIDRunEndSparseFixedBitSet() {
    final int n = 30_000;
    final SparseFixedBitSet sbs = new SparseFixedBitSet(n);
    for (int d = 12_000; d < 12_100; d++) {
      sbs.set(d);
    }
    for (int d = 20_000; d < 20_003; d++) {
      sbs.set(d);
    }
    final BitSetIterator it = new BitSetIterator(sbs, sbs.cardinality());
    assertDocIDRunEndMatchesIterator(it, sbs);
  }

  public void testRandomDocIDRunEndFixedBitSet() {
    final Random r = random();
    final int iters = TEST_NIGHTLY ? 200 : 40;
    for (int iter = 0; iter < iters; iter++) {
      final int n = TestUtil.nextInt(r, 1, 40_000);
      final FixedBitSet fbs = new FixedBitSet(n);
      final int numSets = TestUtil.nextInt(r, 0, Math.min(n, 8_000));
      for (int s = 0; s < numSets; s++) {
        fbs.set(r.nextInt(n));
      }
      if (fbs.cardinality() == 0) {
        assertEquals(NO_MORE_DOCS, new BitSetIterator(fbs, 0).nextDoc());
        continue;
      }
      assertDocIDRunEndMatchesIterator(new BitSetIterator(fbs, fbs.cardinality()), fbs);
    }
  }

  public void testRandomDocIDRunEndSparseFixedBitSet() {
    final Random r = random();
    final int iters = TEST_NIGHTLY ? 200 : 40;
    for (int iter = 0; iter < iters; iter++) {
      final int n = TestUtil.nextInt(r, 1, 40_000);
      final SparseFixedBitSet sbs = new SparseFixedBitSet(n);
      final int numSets = TestUtil.nextInt(r, 0, Math.min(n, 8_000));
      for (int s = 0; s < numSets; s++) {
        sbs.set(r.nextInt(n));
      }
      if (sbs.cardinality() == 0) {
        assertEquals(NO_MORE_DOCS, new BitSetIterator(sbs, 0).nextDoc());
        continue;
      }
      assertDocIDRunEndMatchesIterator(new BitSetIterator(sbs, sbs.cardinality()), sbs);
    }
  }

  private static int expectedDocIDRunEnd(BitSet bits, int doc) {
    int end = doc + 1;
    final int len = bits.length();
    while (end < len && bits.get(end)) {
      end++;
    }
    return end;
  }

  private static void assertDocIDRunEndMatchesIterator(BitSetIterator it, BitSet bits) {
    for (int d = it.nextDoc(); d != NO_MORE_DOCS; d = it.nextDoc()) {
      assertTrue(bits.get(d));
      assertEquals(expectedDocIDRunEnd(bits, d), it.docIDRunEnd());
    }
  }
}
