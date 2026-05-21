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
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} like {@link BitSetIterator} but has a doc base in order to avoid
 * storing previous 0s.
 */
public class DocBaseBitSetIterator extends AbstractDocIdSetIterator {

  private final FixedBitSet bits;
  private final int length;
  private final long cost;
  private final int docBase;

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
    int actualUpto = Math.min(upTo, length);
    // The destination bit set may be shorter than this bit set. This is only legal if all bits
    // beyond offset + bitSet.length() are clear. If not, the below call to `super.intoBitSet` will
    // throw an exception.
    actualUpto = MathUtil.unsignedMin(actualUpto, offset + bitSet.length());
    if (actualUpto > doc) {
      FixedBitSet.orRange(bits, doc - docBase, bitSet, doc - offset, actualUpto - doc);
      advance(actualUpto); // set the current doc
    }
    super.intoBitSet(upTo, bitSet, offset);
  }
}
