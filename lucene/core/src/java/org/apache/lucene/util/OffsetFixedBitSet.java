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
 * a {@link FixedBitSet} that has a min value of this set, so the bits before the min value can be
 * saved.
 */
public class OffsetFixedBitSet extends BitSet {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(OffsetFixedBitSet.class);

  private final FixedBitSet bitSet;
  private final int offsetWords;
  private final int offsetBits;

  public OffsetFixedBitSet(int min, int numBits) {
    this.offsetWords = min >> 6;
    this.offsetBits = this.offsetWords << 6;
    this.bitSet = new FixedBitSet(numBits - offsetBits + 1);
  }

  public OffsetFixedBitSet(int offsetBits, FixedBitSet bits) {
    assert offsetBits % 64 == 0;
    this.offsetBits = offsetBits;
    this.offsetWords = this.offsetBits >> 6;
    this.bitSet = bits;
  }

  @Override
  public void set(int i) {
    if (i < offsetBits) {
      throw new IllegalArgumentException();
    }
    bitSet.set(i - offsetBits);
  }

  @Override
  public boolean getAndSet(int i) {
    if (i < offsetBits) {
      throw new IllegalArgumentException();
    }
    return bitSet.getAndSet(i - offsetBits);
  }

  @Override
  public void clear(int i) {
    bitSet.clear(Math.max(0, i - offsetBits));
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    bitSet.clear(Math.max(0, startIndex - offsetBits), Math.max(0, endIndex - offsetBits));
  }

  @Override
  public int cardinality() {
    return bitSet.cardinality();
  }

  @Override
  public int prevSetBit(int index) {
    if (index < offsetBits) {
      return -1;
    }
    int i = bitSet.prevSetBit(index - offsetBits);
    return i == -1 ? -1 : i + offsetBits;
  }

  @Override
  public int nextSetBit(int index) {
    int i = bitSet.nextSetBit(Math.max(0, index - offsetBits));
    return i == DocIdSetIterator.NO_MORE_DOCS ? DocIdSetIterator.NO_MORE_DOCS : i + offsetBits;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + bitSet.ramBytesUsed();
  }

  @Override
  public boolean get(int index) {
    return index >= offsetBits && bitSet.get(index - offsetBits);
  }

  @Override
  public int length() {
    return bitSet.length() + offsetBits;
  }

  public FixedBitSet getBitSet() {
    return bitSet;
  }

  public int getOffsetWords() {
    return offsetWords;
  }

  public int getOffsetBits() {
    return offsetBits;
  }
}
