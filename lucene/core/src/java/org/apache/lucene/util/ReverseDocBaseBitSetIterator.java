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

import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} like {@link DocBaseBitSetIterator} but iterate over the documents in
 * reverse order.
 */
public class ReverseDocBaseBitSetIterator extends DocIdSetIterator {

  private final FixedBitSet bits;
  private final int length;
  private final long cost;
  private final int docBase;
  private int doc;

  public ReverseDocBaseBitSetIterator(FixedBitSet bits, int cost, int docBase) {
    if (cost < 0) {
      throw new IllegalArgumentException("cost must be >= 0, got " + cost);
    }
    if ((docBase & 63) != 0) {
      throw new IllegalArgumentException("docBase need to be a multiple of 64, got " + docBase);
    }
    this.bits = bits;
    this.length = bits.length() + docBase;
    this.cost = cost;
    this.doc = length;
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
    return advance(doc - 1);
  }

  @Override
  public int advance(int target) {
    if (target - docBase < 0) {
      return doc = NO_MORE_DOCS;
    }
    int next = bits.prevSetBit(target - docBase);
    if (next == -1) {
      return doc = NO_MORE_DOCS;
    } else {
      return doc = next + docBase;
    }
  }

  @Override
  public long cost() {
    return cost;
  }
}
