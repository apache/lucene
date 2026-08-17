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

public class TestNotDocIdSet extends BaseDocIdSetTestCase<NotDocIdSet> {

  private static final int BLOCK_SIZE = 1 << 16;

  @Override
  public NotDocIdSet copyOf(BitSet bs, int length) throws IOException {
    final FixedBitSet set = new FixedBitSet(length);
    for (int doc = bs.nextClearBit(0); doc < length; doc = bs.nextClearBit(doc + 1)) {
      set.set(doc);
    }
    return new NotDocIdSet(length, new BitDocIdSet(set));
  }

  public void testIntoBitSetDenseComplement() throws IOException {
    final int maxDoc = 1024;
    FixedBitSet excluded = new FixedBitSet(maxDoc);
    excluded.set(3);
    excluded.set(10);
    excluded.set(511);
    excluded.set(900);

    NotDocIdSet set = new NotDocIdSet(maxDoc, new BitDocIdSet(excluded));
    DocIdSetIterator it = set.iterator();
    assertEquals(0, it.nextDoc());

    FixedBitSet actual = new FixedBitSet(maxDoc + 16);
    actual.set(maxDoc + 1);
    it.intoBitSet(maxDoc, actual, 0);

    for (int doc = 0; doc < maxDoc; doc++) {
      assertEquals("doc " + doc, excluded.get(doc) == false, actual.get(doc));
    }
    assertTrue(actual.get(maxDoc + 1));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.docID());
  }

  public void testDocIDRunEndContiguousRun() throws IOException {
    final int maxDoc = random().nextInt(2, 1_000);
    BitSet bs = new BitSet();
    int start = random().nextInt(0, maxDoc - 1);
    int end = random().nextInt(start + 1, maxDoc);
    bs.set(start, end);
    assertDocIDRunEndMatches(bs, maxDoc, copyOf(bs, maxDoc));
  }

  public void testRandomDocIDRunEnd() throws IOException {
    final int iters = TEST_NIGHTLY ? 50 : 5;
    for (int iter = 0; iter < iters; iter++) {
      final int maxDoc = random().nextInt(50, 1 << 18);
      final BitSet bs = randomBitSet(maxDoc, random().nextFloat());
      if (bs.isEmpty()) {
        continue;
      }
      assertDocIDRunEndMatches(bs, maxDoc, copyOf(bs, maxDoc));
    }
  }

  public void testIntoBitSetFullBlockComplement() throws IOException {
    FixedBitSet excluded = new FixedBitSet(BLOCK_SIZE);
    for (int doc = 3; doc < BLOCK_SIZE; doc += 1024) {
      excluded.set(doc);
    }

    DocIdSetIterator it = new NotDocIdSet(BLOCK_SIZE, new BitDocIdSet(excluded)).iterator();
    assertEquals(0, it.nextDoc());

    FixedBitSet actual = new FixedBitSet(BLOCK_SIZE);
    it.intoBitSet(BLOCK_SIZE, actual, 0);

    assertEquals(BLOCK_SIZE - 64, actual.cardinality());
    for (int doc = 3; doc < BLOCK_SIZE; doc += 1024) {
      assertFalse("doc " + doc, actual.get(doc));
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.docID());
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

  private static int expectedDocIDRunEnd(BitSet bs, int maxDoc, int doc) {
    int end = doc + 1;
    while (end < maxDoc && bs.get(end)) {
      end++;
    }
    return end;
  }

  private static void assertDocIDRunEndMatches(BitSet bs, int maxDoc, NotDocIdSet set)
      throws IOException {
    DocIdSetIterator it = set.iterator();
    assertNotNull(it);
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      assertTrue(bs.get(doc));
      assertEquals(expectedDocIDRunEnd(bs, maxDoc, doc), it.docIDRunEnd());
    }
  }
}
