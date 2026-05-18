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
import java.util.BitSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.BaseDocIdSetTestCase;

public class TestRoaringDocIdSet extends BaseDocIdSetTestCase<RoaringDocIdSet> {

  @Override
  public RoaringDocIdSet copyOf(BitSet bs, int length) throws IOException {
    final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(length);
    for (int i = bs.nextSetBit(0); i != -1; i = bs.nextSetBit(i + 1)) {
      builder.add(i);
    }
    return builder.build();
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, RoaringDocIdSet ds2) throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    assertEquals(ds1.cardinality(), ds2.cardinality());
  }

  public void testDocIDRunEndContiguousWithinBlock() throws IOException {
    final int maxDoc = random().nextInt(2, 50_000);
    BitSet bs = new BitSet();
    int start = random().nextInt(0, maxDoc - 1);
    int end = random().nextInt(start + 1, maxDoc);
    for (int d = start; d < end; d++) {
      bs.set(d);
    }
    RoaringDocIdSet set = copyOf(bs, maxDoc);
    assertDocIDRunEndMatches(bs, maxDoc, set);
  }

  public void testDocIDRunEndFinishInBoundary() throws IOException {
    final int boundary = 1 << 16;
    final int maxDoc = boundary + 100;
    BitSet bs = new BitSet();
    for (int d = boundary - 3; d < boundary; d++) {
      bs.set(d);
    }
    RoaringDocIdSet set = copyOf(bs, maxDoc);
    assertDocIDRunEndMatches(bs, maxDoc, set);
  }

  public void testDocIDRunEndAcross64KBoundary() throws IOException {
    final int boundary = 1 << 16;
    final int maxDoc = boundary + 100;
    BitSet bs = new BitSet();
    for (int d = boundary - 3; d < boundary + 7; d++) {
      bs.set(d);
    }
    RoaringDocIdSet set = copyOf(bs, maxDoc);
    assertDocIDRunEndMatches(bs, maxDoc, set);
  }

  private static BitSet randomBitSet(int numBits, float percentSet) {
    final int numBitsSet = Math.min(numBits, (int) (percentSet * numBits));
    final BitSet set = new BitSet(numBits);
    if (numBitsSet >= numBits) {
      set.set(0, numBits);
      return set;
    }
    for (int i = 0; i < numBitsSet; ++i) {
      while (true) {
        final int o = random().nextInt(numBits);
        if (!set.get(o)) {
          set.set(o);
          break;
        }
      }
    }
    return set;
  }

  public void testRandomDocIDRunEnd() throws IOException {
    final int iters = TEST_NIGHTLY ? 50 : 5;
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc = random().nextInt(50, 1 << 18);
      final BitSet bs = randomBitSet(maxDoc, random().nextFloat());
      if (bs.isEmpty()) {
        assertNull(copyOf(bs, maxDoc).iterator());
        continue;
      }
      assertDocIDRunEndMatches(bs, maxDoc, copyOf(bs, maxDoc));
    }
  }

  private static int expectedDocIDRunEnd(BitSet bs, int maxDoc, int doc) {
    int end = doc + 1;
    while (end < maxDoc && bs.get(end)) {
      end++;
    }
    return end;
  }

  private static void assertDocIDRunEndMatches(BitSet bs, int maxDoc, RoaringDocIdSet set)
      throws IOException {
    DocIdSetIterator it = set.iterator();
    assertNotNull(it);
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      assertTrue(bs.get(doc));
      assertEquals(expectedDocIDRunEnd(bs, maxDoc, doc), it.docIDRunEnd());
    }
  }
}
