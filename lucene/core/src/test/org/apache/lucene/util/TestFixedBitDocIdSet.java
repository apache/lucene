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

public class TestFixedBitDocIdSet extends BaseDocIdSetTestCase<BitDocIdSet> {

  @Override
  public BitDocIdSet copyOf(BitSet bs, int length) {
    final FixedBitSet set = new FixedBitSet(length);
    for (int doc = bs.nextSetBit(0); doc != -1; doc = bs.nextSetBit(doc + 1)) {
      set.set(doc);
    }
    return new BitDocIdSet(set);
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, BitDocIdSet ds2) throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    // bits()
    final Bits bits = ds2.bits();
    if (bits != null) {
      // test consistency between bits and iterator
      DocIdSetIterator it2 = ds2.iterator();
      for (int previousDoc = -1, doc = it2.nextDoc(); ; previousDoc = doc, doc = it2.nextDoc()) {
        final int max = doc == DocIdSetIterator.NO_MORE_DOCS ? bits.length() : doc;
        for (int i = previousDoc + 1; i < max; ++i) {
          assertFalse(bits.get(i));
        }
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        assertTrue(bits.get(doc));
      }
    }
  }
}
