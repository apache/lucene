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
package org.apache.lucene.tests.util;

import static org.apache.lucene.tests.util.BaseBitSetTestCase.randomSet;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

/** Base test class for {@link DocIdSet}s. */
public abstract class BaseDocIdSetTestCase<T extends DocIdSet> extends LuceneTestCase {

  /** Create a copy of the given {@link BitSet} which has <code>length</code> bits. */
  public abstract T copyOf(BitSet bs, int length) throws IOException;

  /** Test length=0. */
  public void testNoBit() throws IOException {
    final BitSet bs = new BitSet(1);
    final T copy = copyOf(bs, 1);
    assertEquals(1, bs, copy);
  }

  /** Test length=1. */
  public void test1Bit() throws IOException {
    final BitSet bs = new BitSet(1);
    if (random().nextBoolean()) {
      bs.set(0);
    }
    final T copy = copyOf(bs, 1);
    assertEquals(1, bs, copy);
  }

  /** Test length=2. */
  public void test2Bits() throws IOException {
    final BitSet bs = new BitSet(2);
    if (random().nextBoolean()) {
      bs.set(0);
    }
    if (random().nextBoolean()) {
      bs.set(1);
    }
    final T copy = copyOf(bs, 2);
    assertEquals(2, bs, copy);
  }

  /** Compare the content of the set against a {@link BitSet}. */
  public void testAgainstBitSet() throws IOException {
    Random random = random();
    final int numBits = TestUtil.nextInt(random, 100, 1 << 20);
    // test various random sets with various load factors
    for (float percentSet : new float[] {0f, 0.0001f, random.nextFloat(), 0.9f, 1f}) {
      final BitSet set = randomSet(numBits, percentSet);
      final T copy = copyOf(set, numBits);
      assertEquals(numBits, set, copy);
    }
    // test one doc
    BitSet set = new BitSet(numBits);
    set.set(0); // 0 first
    T copy = copyOf(set, numBits);
    assertEquals(numBits, set, copy);
    set.clear(0);
    set.set(random.nextInt(numBits));
    copy = copyOf(set, numBits); // then random index
    assertEquals(numBits, set, copy);
    // test regular increments
    int maxIterations = TEST_NIGHTLY ? Integer.MAX_VALUE : 10;
    int iterations = 0;
    for (int inc = 2; inc < 1000; inc += TestUtil.nextInt(random, 1, 100)) {
      // don't let this test run too many times, even if it gets unlucky with "inc"
      if (iterations >= maxIterations) {
        break;
      }
      iterations++;

      set = new BitSet(numBits);
      for (int d = random.nextInt(10); d < numBits; d += inc) {
        set.set(d);
      }
      copy = copyOf(set, numBits);
      assertEquals(numBits, set, copy);
    }
  }

  /** Test ram usage estimation. */
  public void testRamBytesUsed() throws IOException {
    Random random = random();
    final int iters = 100;
    for (int i = 0; i < iters; ++i) {
      final int pow = random.nextInt(20);
      final int maxDoc = TestUtil.nextInt(random, 1, 1 << pow);
      final int numDocs =
          TestUtil.nextInt(random, 0, Math.min(maxDoc, 1 << TestUtil.nextInt(random, 0, pow)));
      final BitSet set = randomSet(maxDoc, numDocs);
      final DocIdSet copy = copyOf(set, maxDoc);
      final long actualBytes = ramBytesUsed(copy, maxDoc);
      final long expectedBytes = copy.ramBytesUsed();
      assertEquals(expectedBytes, actualBytes);
    }
  }

  /**
   * Assert that the content of the {@link DocIdSet} is the same as the content of the {@link
   * BitSet}.
   */
  public void assertEquals(int numBits, BitSet ds1, T ds2) throws IOException {
    Random random = random();
    // nextDoc
    DocIdSetIterator it2 = ds2.iterator();
    if (it2 == null) {
      assertEquals(-1, ds1.nextSetBit(0));
    } else {
      assertEquals(-1, it2.docID());
      for (int doc = ds1.nextSetBit(0); doc != -1; doc = ds1.nextSetBit(doc + 1)) {
        assertEquals(doc, it2.nextDoc());
        assertEquals(doc, it2.docID());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.nextDoc());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.docID());
    }

    // nextDoc / advance
    it2 = ds2.iterator();
    if (it2 == null) {
      assertEquals(-1, ds1.nextSetBit(0));
    } else {
      for (int doc = -1; doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (random.nextBoolean()) {
          doc = ds1.nextSetBit(doc + 1);
          if (doc == -1) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
          }
          assertEquals(doc, it2.nextDoc());
          assertEquals(doc, it2.docID());
        } else {
          final int target =
              doc + 1 + random.nextInt(random.nextBoolean() ? 64 : Math.max(numBits / 8, 1));
          doc = ds1.nextSetBit(target);
          if (doc == -1) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
          }
          assertEquals(doc, it2.advance(target));
          assertEquals(doc, it2.docID());
        }
      }
    }
  }

  private static class Dummy {
    @SuppressWarnings("unused")
    Object o1, o2;
  }

  // same as RamUsageTester.sizeOf but tries to not take into account resources
  // that might be shared across instances
  private long ramBytesUsed(DocIdSet set, int length) throws IOException {
    Dummy dummy = new Dummy();
    dummy.o1 = copyOf(new BitSet(length), length);
    dummy.o2 = set;
    long bytes1 = RamUsageTester.ramUsed(dummy);
    dummy.o2 = null;
    long bytes2 = RamUsageTester.ramUsed(dummy);
    return bytes1 - bytes2;
  }

  public void testIntoBitSet() throws IOException {
    Random random = random();
    final int numBits = TestUtil.nextInt(random, 100, 1 << 20);
    // test various random sets with various load factors
    for (float percentSet : new float[] {0f, 0.0001f, random.nextFloat(), 0.9f, 1f}) {
      final BitSet set = randomSet(numBits, percentSet);
      final T copy = copyOf(set, numBits);
      int from = TestUtil.nextInt(random(), 0, numBits - 1);
      int to = TestUtil.nextInt(random(), from, numBits + 5);
      FixedBitSet actual = new FixedBitSet(to - from);
      DocIdSetIterator it1 = copy.iterator();
      if (it1 == null) {
        continue;
      }
      int fromDoc = it1.advance(from);
      // No docs to set
      it1.intoBitSet(from, actual, from);
      assertTrue(actual.scanIsEmpty());
      assertEquals(fromDoc, it1.docID());

      // Now actually set some bits
      it1.intoBitSet(to, actual, from);
      FixedBitSet expected = new FixedBitSet(to - from);
      DocIdSetIterator it2 = copy.iterator();
      for (int doc = it2.advance(from); doc < to; doc = it2.nextDoc()) {
        expected.set(doc - from);
      }
      assertEquals(expected, actual);
      // Check if docID() / nextDoc() return the same value after #intoBitSet has been called.
      assertEquals(it2.docID(), it1.docID());
      if (it2.docID() != DocIdSetIterator.NO_MORE_DOCS) {
        assertEquals(it2.nextDoc(), it1.nextDoc());
      }
    }
  }

  public void testIntoBitSetBoundChecks() throws IOException {
    final BitSet set = new BitSet();
    set.set(20);
    set.set(42);
    final T copy = copyOf(set, 256);
    int from = TestUtil.nextInt(random(), 0, 20);
    int to = TestUtil.nextInt(random(), 43, 256);
    int offset = TestUtil.nextInt(random(), 0, from);
    FixedBitSet dest1 = new FixedBitSet(42 - offset + 1);
    DocIdSetIterator it1 = copy.iterator();
    it1.advance(from);
    // This call is legal, since all "set" bits are in the range
    it1.intoBitSet(to, dest1, offset);
    for (int i = 0; i < dest1.length(); ++i) {
      assertEquals(offset + i == 20 || offset + i == 42, dest1.get(i));
    }

    FixedBitSet dest2 = new FixedBitSet(42 - offset);
    DocIdSetIterator it2 = copy.iterator();
    it2.advance(from);
    // This call is not legal, since there is one bit that is set beyond the end of the target bit
    // set
    expectThrows(Throwable.class, () -> it2.intoBitSet(to, dest2, offset));

    FixedBitSet dest3 = new FixedBitSet(42 - offset + 1);
    DocIdSetIterator it3 = copy.iterator();
    it3.advance(from);
    // This call is not legal, since offset is greater than the current doc
    expectThrows(Throwable.class, () -> it3.intoBitSet(to, dest3, 21));
  }
}
