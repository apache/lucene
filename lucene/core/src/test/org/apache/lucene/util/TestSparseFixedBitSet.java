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

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.BaseBitSetTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestSparseFixedBitSet extends BaseBitSetTestCase<SparseFixedBitSet> {

  @Override
  public SparseFixedBitSet copyOf(BitSet bs, int length) throws IOException {
    final SparseFixedBitSet set = new SparseFixedBitSet(length);
    for (int doc = bs.nextSetBit(0);
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = doc + 1 >= length ? DocIdSetIterator.NO_MORE_DOCS : bs.nextSetBit(doc + 1)) {
      set.set(doc);
    }
    return set;
  }

  @Override
  protected void assertEquals(BitSet set1, SparseFixedBitSet set2, int maxDoc) {
    super.assertEquals(set1, set2, maxDoc);
    // check invariants of the sparse set
    int nonZeroLongCount = 0;
    for (int i = 0; i < set2.indices.length; ++i) {
      final int n = Long.bitCount(set2.indices[i]);
      if (n != 0) {
        nonZeroLongCount += n;
        for (int j = n; j < set2.bits[i].length; ++j) {
          assertEquals(0, set2.bits[i][j]);
        }
      }
    }
    assertEquals(nonZeroLongCount, set2.nonZeroLongCount);
  }

  public void testApproximateCardinality() {
    final SparseFixedBitSet set = new SparseFixedBitSet(10000);
    final int first = random().nextInt(1000);
    final int interval = 200 + random().nextInt(1000);
    for (int i = first; i < set.length(); i += interval) {
      set.set(i);
    }
    assertEquals(set.cardinality(), set.approximateCardinality(), 20);
  }

  public void testApproximateCardinalityOnDenseSet() {
    // this tests that things work as expected in approximateCardinality when
    // all longs are different than 0, in which case we divide by zero
    final int numDocs = TestUtil.nextInt(random(), 1, 10000);
    final SparseFixedBitSet set = new SparseFixedBitSet(numDocs);
    for (int i = 0; i < set.length(); ++i) {
      set.set(i);
    }
    assertEquals(numDocs, set.approximateCardinality());
  }

  public void testRamBytesUsed() throws IOException {
    int size = 1000 + random().nextInt(10000);
    BitSet original = new SparseFixedBitSet(size);
    for (int i = 0; i < 3; i++) {
      original.set(random().nextInt(size));
    }
    assertTrue(original.ramBytesUsed() > 0);

    // Take union with a random sparse iterator, then check memory usage
    BitSet copy = copyOf(original, size);
    BitSet otherBitSet = new SparseFixedBitSet(size);
    int interval = 10 + random().nextInt(100);
    for (int i = 0; i < size; i += interval) {
      otherBitSet.set(i);
    }
    copy.or(new BitSetIterator(otherBitSet, size));
    assertTrue(copy.ramBytesUsed() > original.ramBytesUsed());

    // Take union with a dense iterator, then check memory usage
    copy = copyOf(original, size);
    copy.or(DocIdSetIterator.all(size));
    assertTrue(copy.ramBytesUsed() > original.ramBytesUsed());
    assertTrue(copy.ramBytesUsed() > size / Byte.SIZE);

    // Check that both "copy" strategies result in bit sets with
    // (roughly) same memory usage as original
    BitSet setCopy = copyOf(original, size);
    assertEquals(setCopy.ramBytesUsed(), original.ramBytesUsed());

    BitSet orCopy = new SparseFixedBitSet(size);
    orCopy.or(new BitSetIterator(original, size));
    assertTrue(Math.abs(original.ramBytesUsed() - orCopy.ramBytesUsed()) <= 64L);
  }

  private static int bruteNextUnset(SparseFixedBitSet set, int from, int upperBound) {
    for (int i = from; i < upperBound; i++) {
      if (set.get(i) == false) {
        return i;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  public void testSetRange() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      FixedBitSet expected = new FixedBitSet(numBits);
      final int numInitial = (int) (percentSet * numBits);
      for (int j = 0; j < numInitial; j++) {
        expected.set(random.nextInt(numBits));
      }
      SparseFixedBitSet set = copyOf(expected, numBits);
      final int iters = atLeast(random, 10);
      for (int i = 0; i < iters; ++i) {
        final int from = random.nextInt(numBits);
        final int to = random.nextInt(numBits + 1);
        expected.set(from, to);
        set.set(from, to);
        assertEquals(expected, set, numBits);
      }
    }
  }

  public void testNextClearBit() {
    Random rand = random();
    final int outer = TEST_NIGHTLY ? 300 : 60;
    for (int iter = 0; iter < outer; iter++) {
      final int n = TestUtil.nextInt(rand, 1, 40_000);
      final SparseFixedBitSet set = new SparseFixedBitSet(n);
      final int numSets = TestUtil.nextInt(rand, 0, Math.min(n, 8_000));
      for (int s = 0; s < numSets; s++) {
        set.set(rand.nextInt(n));
      }
      for (int t = 0; t < 300; t++) {
        final int from = rand.nextInt(n);
        assertEquals(bruteNextUnset(set, from, n), set.nextClearBit(from));
        final int ub = from + 1 + rand.nextInt(n - from);
        assertEquals(bruteNextUnset(set, from, ub), set.nextClearBit(from, ub));
      }
    }
  }
}
