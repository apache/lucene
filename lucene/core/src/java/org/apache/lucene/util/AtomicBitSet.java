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

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link BitSet} implementation that offers concurrent, lock-free access through an {@link
 * AtomicLongArray} as bit storage.
 */
public class AtomicBitSet extends BitSet {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(AtomicBitSet.class);

  private final AtomicReference<AtomicLongArray> storage;

  public AtomicBitSet(int numBits) {
    int numLongs = (numBits + 63) >>> 6;
    storage = new AtomicReference<>(new AtomicLongArray(numLongs));
  }

  private static int index(int bit) {
    return bit >>> 6;
  }

  private static long mask(int bit) {
    return 1L << (bit & 63);
  }

  @Override
  public int length() {
    return storage.get().length() << 6;
  }

  private void expandStorage(int minCapacity) {
    int newNumLongs = (minCapacity + 63) >>> 6;
    storage.updateAndGet(
        currentStorage -> {
          if (minCapacity <= length()) {
            return currentStorage;
          }

          AtomicLongArray newStorage = new AtomicLongArray(newNumLongs);
          for (int i = 0; i < currentStorage.length(); i++) {
            newStorage.set(i, currentStorage.get(i));
          }
          return newStorage;
        });
  }

  @Override
  public void set(int i) {
    int currentLength = length();
    if (i >= currentLength) {
      expandStorage(Math.max(i + 1, 2 * currentLength));
    }
    int idx = index(i);
    long mask = mask(i);
    storage.get().getAndAccumulate(idx, mask, (prev, m) -> prev | m);
  }

  @Override
  public boolean get(int i) {
    if (i >= length()) {
      return false;
    }
    int idx = index(i);
    long mask = mask(i);
    long value = storage.get().get(idx);
    return (value & mask) != 0;
  }

  @Override
  public boolean getAndSet(int i) {
    int currentLength = length();
    if (i >= currentLength) {
      expandStorage(Math.max(i + 1, 2 * currentLength));
    }
    int idx = index(i);
    long mask = mask(i);
    long prev = storage.get().getAndAccumulate(idx, mask, (p, m) -> p | m);
    return (prev & mask) != 0;
  }

  @Override
  public void clear(int i) {
    if (i >= length()) {
      return;
    }
    int idx = index(i);
    long mask = mask(i);
    storage.get().getAndAccumulate(idx, mask, (prev, m) -> prev & ~m);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    int startIdx = index(startIndex);
    int endIdx = index(endIndex - 1);
    for (int i = startIdx; i <= endIdx; i++) {
      long startMask = (i == startIdx) ? (-1L << (startIndex & 63)) : -1L;
      long endMask = (i == endIdx) ? (-1L << (endIndex & 63)) : 0L;
      long mask = startMask | endMask;
      storage.get().getAndAccumulate(i, mask, (prev, m) -> prev & ~m);
    }
  }

  @Override
  public int cardinality() {
    int count = 0;
    for (int i = 0; i < storage.get().length(); i++) {
      count += Long.bitCount(storage.get().get(i));
    }
    return count;
  }

  @Override
  public int approximateCardinality() {
    return cardinality();
  }

  @Override
  public int prevSetBit(int index) {
    int idx = index(index);
    long mask = (1L << (index & 63)) - 1;

    for (int i = idx; i >= 0; i--) {
      long word = storage.get().get(i) & mask;
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(Long.lowestOneBit(word));
      }
      mask = -1L;
    }
    return -1;
  }

  @Override
  public int nextSetBit(int index) {
    int idx = index(index);
    long mask = -1L >>> (63 - (index & 63));

    AtomicLongArray currentStorage = storage.get();
    for (int i = idx; i < currentStorage.length(); i++) {
      long word = currentStorage.get(i) & mask;
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(Long.lowestOneBit(word));
      }
      mask = -1L;
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public long ramBytesUsed() {
    final int longSizeInBytes = Long.BYTES;
    final int arrayOverhead = 16; // Estimated overhead of AtomicLongArray object in bytes
    long storageSize = (long) storage.get().length() * longSizeInBytes + arrayOverhead;
    return BASE_RAM_BYTES_USED + storageSize;
  }
}
