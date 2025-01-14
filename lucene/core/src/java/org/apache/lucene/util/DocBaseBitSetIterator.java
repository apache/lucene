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
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} like {@link BitSetIterator} but has a doc base in onder to avoid
 * storing previous 0s.
 */
public class DocBaseBitSetIterator extends DocIdSetIterator {

  private final FixedBitSet bits;
  private final int length;
  private final long cost;
  private final int docBase;
  private int doc = -1;

  public DocBaseBitSetIterator(FixedBitSet bits, long cost, int docBase) {
    if (cost < 0) {
      throw new IllegalArgumentException("cost must be >= 0, got " + cost);
    }
    if ((docBase & 63) != 0) {
      throw new IllegalArgumentException("docBase need to be a multiple of 64, got " + docBase);
    }
    this.bits = bits;
    this.length = bits.length() + docBase;
    this.cost = cost;
    this.docBase = docBase;
  }

  /**
   * Get the {@link FixedBitSet}. A docId will exist in this {@link DocIdSetIterator} if the bitset
   * contains the (docId - {@link #getDocBase})
   *
   * @return the offset docId bitset
   */
  public FixedBitSet getBitSet() {
    return bits;
  }

  @Override
  public int docID() {
    return doc;
  }

  /**
   * Get the docBase. It is guaranteed that docBase is a multiple of 64.
   *
   * @return the docBase
   */
  public int getDocBase() {
    return docBase;
  }

  @Override
  public int nextDoc() {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) {
    if (target >= length) {
      return doc = NO_MORE_DOCS;
    }
    int next = bits.nextSetBit(Math.max(0, target - docBase));
    if (next == NO_MORE_DOCS) {
      return doc = NO_MORE_DOCS;
    } else {
      return doc = next + docBase;
    }
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    upTo = Math.min(upTo, length);
    // This doc id set is a bit hacky as it is sometimes OR'ed into a smaller bit set, which only
    // works because trailing bits are unset.
    if (upTo - offset > bitSet.length()) {
      upTo = offset + bitSet.length();
      assert bits.nextSetBit(upTo - docBase) == NO_MORE_DOCS;
    }
    if (upTo > doc) {
      FixedBitSet.orRange(bits, doc - docBase, bitSet, doc - offset, upTo - doc);
      advance(upTo); // set the current doc
    }
  }
}
