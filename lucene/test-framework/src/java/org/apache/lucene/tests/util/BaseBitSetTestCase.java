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

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.junit.Ignore;

/** Base test case for BitSets. */
@Ignore
public abstract class BaseBitSetTestCase<T extends BitSet> extends LuceneTestCase {

  /** Create a copy of the given {@link BitSet} which has <code>length</code> bits. */
  public abstract T copyOf(BitSet bs, int length) throws IOException;

  /** Create a random set which has <code>numBitsSet</code> of its <code>numBits</code> bits set. */
  static java.util.BitSet randomSet(int numBits, int numBitsSet) {
    assert numBitsSet <= numBits;
    final java.util.BitSet set = new java.util.BitSet(numBits);
    if (numBitsSet == numBits) {
      set.set(0, numBits);
    } else {
      Random random = nonAssertingRandom(random());
      for (int i = 0; i < numBitsSet; ++i) {
        while (true) {
          final int o = random.nextInt(numBits);
          if (!set.get(o)) {
            set.set(o);
            break;
          }
        }
      }
    }
    return set;
  }

  /** Same as {@link #randomSet(int, int)} but given a load factor. */
  static java.util.BitSet randomSet(int numBits, float percentSet) {
    return randomSet(numBits, (int) (percentSet * numBits));
  }

  protected abstract T fromJavaUtilBitSet(java.util.BitSet set, int numBits);

  protected void assertEquals(java.util.BitSet set1, T set2, int maxDoc) {
    for (int i = 0; i < maxDoc; ++i) {
      assertEquals("Different at " + i, set1.get(i), set2.get(i));
    }
  }

  /** Test the {@link BitSet#cardinality()} method. */
  public void testCardinality() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      assertEquals(jdkSet.cardinality(), luceneSet.cardinality());
    }
  }

  /** Test {@link BitSet#prevSetBit(int)}. */
  public void testPrevSetBit() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      for (int i = 0; i < numBits; ++i) {
        assertEquals(Integer.toString(i), jdkSet.previousSetBit(i), luceneSet.prevSetBit(i));
      }
    }
  }

  /** Test {@link BitSet#nextSetBit(int)}. */
  public void testNextSetBit() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      for (int i = 0; i < numBits; ++i) {
        assertEquals(normalizeJdkNextSetBit(jdkSet.nextSetBit(i)), luceneSet.nextSetBit(i));
      }
    }
  }

  /** Test the {@link BitSet#set} method. */
  public void testSet() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    java.util.BitSet jdkSet = randomSet(numBits, 0);
    T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
    final int iters = 10000 + random.nextInt(10000);
    for (int i = 0; i < iters; ++i) {
      final int index = random.nextInt(numBits);
      jdkSet.set(index);
      luceneSet.set(index);
    }
    assertEquals(jdkSet, luceneSet, numBits);
  }

  /** Test the {@link BitSet#getAndSet} method. */
  public void testGetAndSet() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    java.util.BitSet jdkSet = randomSet(numBits, 0);
    T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
    final int iters = 10000 + random.nextInt(10000);
    for (int i = 0; i < iters; ++i) {
      final int index = random.nextInt(numBits);
      boolean v1 = jdkSet.get(index);
      jdkSet.set(index); // emulate getAndSet
      boolean v2 = luceneSet.getAndSet(index);
      assertEquals(v1, v2);
    }
    assertEquals(jdkSet, luceneSet, numBits);
  }

  /** Test the {@link BitSet#clear(int)} method. */
  public void testClear() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      final int iters = 1 + random.nextInt(numBits * 2);
      for (int i = 0; i < iters; ++i) {
        final int index = random.nextInt(numBits);
        jdkSet.clear(index);
        luceneSet.clear(index);
      }
      assertEquals(jdkSet, luceneSet, numBits);
    }
  }

  /** Test the {@link BitSet#clear(int,int)} method. */
  public void testClearRange() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      final int iters = atLeast(random, 10);
      for (int i = 0; i < iters; ++i) {
        final int from = random.nextInt(numBits);
        final int to = random.nextInt(numBits + 1);
        if (from > to) {
          // JDK would throw, Lucene no-ops: so skip
          continue;
        }
        jdkSet.clear(from, to);
        luceneSet.clear(from, to);
        assertEquals(jdkSet, luceneSet, numBits);
      }
    }
  }

  /** Test the {@link BitSet#clear()} method. */
  public void testClearAll() throws IOException {
    Random random = random();
    final int numBits = 1 + random.nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      java.util.BitSet jdkSet = randomSet(numBits, percentSet);
      T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);
      final int iters = atLeast(random, 10);
      for (int i = 0; i < iters; ++i) {
        jdkSet.clear();
        luceneSet.clear();
        assertEquals(jdkSet, luceneSet, numBits);
      }
    }
  }

  private static int normalizeJdkNextSetBit(int bit) {
    return bit == -1 ? DocIdSetIterator.NO_MORE_DOCS : bit;
  }

  private DocIdSet randomCopy(BitSet set, int numBits) throws IOException {
    switch (random().nextInt(5)) {
      case 0:
        return new BitDocIdSet(set, set.cardinality());
      case 1:
        return new BitDocIdSet(copyOf(set, numBits), set.cardinality());
      case 2:
        final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(numBits);
        for (int i = set.nextSetBit(0);
            i != DocIdSetIterator.NO_MORE_DOCS;
            i = i + 1 >= numBits ? DocIdSetIterator.NO_MORE_DOCS : set.nextSetBit(i + 1)) {
          builder.add(i);
        }
        return builder.build();
      case 3:
        FixedBitSet fbs = new FixedBitSet(numBits);
        fbs.or(new BitSetIterator(set, 0));
        return new BitDocIdSet(fbs);
      case 4:
        SparseFixedBitSet sfbs = new SparseFixedBitSet(numBits);
        sfbs.or(new BitSetIterator(set, 0));
        return new BitDocIdSet(sfbs);
      default:
        fail();
        return null;
    }
  }

  private void testOr(float load) throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    java.util.BitSet jdkSet = randomSet(numBits, 0); // empty
    T luceneSet = fromJavaUtilBitSet(jdkSet, numBits);

    final int iterations = atLeast(10);
    for (int iter = 0; iter < iterations; ++iter) {
      DocIdSet otherSet =
          randomCopy(fromJavaUtilBitSet(randomSet(numBits, load), numBits), numBits);
      DocIdSetIterator otherIterator = otherSet.iterator();
      if (otherIterator != null) {
        jdkSet.or(toJavaUtil(otherIterator, numBits));
        luceneSet.or(otherSet.iterator());
        assertEquals(jdkSet, luceneSet, numBits);
      }
    }
  }

  /** Helper: consume a DocIdSetIterator into a java.util.BitSet. */
  private static java.util.BitSet toJavaUtil(DocIdSetIterator it, int numBits) throws IOException {
    java.util.BitSet bs = new java.util.BitSet(numBits);
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      bs.set(doc);
    }
    return bs;
  }

  /** Test {@link BitSet#or(DocIdSetIterator)} on sparse sets. */
  public void testOrSparse() throws IOException {
    testOr(0.001f);
  }

  /** Test {@link BitSet#or(DocIdSetIterator)} on dense sets. */
  public void testOrDense() throws IOException {
    testOr(0.5f);
  }

  /** Test {@link BitSet#or(DocIdSetIterator)} on a random density. */
  public void testOrRandom() throws IOException {
    testOr(random().nextFloat());
  }
}
