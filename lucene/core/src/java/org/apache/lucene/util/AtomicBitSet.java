package org.apache.lucene.util;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link BitSet} implementation that offers concurrent, lock-free access through an {@link
 * AtomicLongArray} as bit storage.
 */
public class AtomicBitSet extends BitSet {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(AtomicBitSet.class);

  private AtomicLongArray storage;
  private int numBits;

  public AtomicBitSet(int numBits) {
    this.numBits = numBits;
    int numLongs = (numBits + 63) >>> 6;
    storage = new AtomicLongArray(numLongs);
  }

  private static int index(int bit) {
    return bit >>> 6;
  }

  private static long mask(int bit) {
    return 1L << (bit & 63);
  }

  @Override
  public int length() {
    return numBits;
  }

  private void expandStorage(int minCapacity) {
    int numLongs = (minCapacity + 63) >>> 6;
    AtomicLongArray newStorage = new AtomicLongArray(numLongs);
    for (int i = 0; i < storage.length(); i++) {
      newStorage.set(i, storage.get(i));
    }
    storage = newStorage;
    numBits = numLongs << 6;
  }

  @Override
  public void set(int i) {
    if (i >= numBits) {
      expandStorage(i + 1);
    }
    int idx = index(i);
    long mask = mask(i);
    storage.getAndAccumulate(idx, mask, (prev, m) -> prev | m);
  }

  @Override
  public boolean get(int i) {
    if (i >= numBits) {
      return false;
    }
    int idx = index(i);
    long mask = mask(i);
    long value = storage.get(idx);
    return (value & mask) != 0;
  }

  @Override
  public boolean getAndSet(int i) {
    if (i >= numBits) {
      expandStorage(i + 1);
    }
    int idx = index(i);
    long mask = mask(i);
    long prev = storage.getAndAccumulate(idx, mask, (p, m) -> p | m);
    return (prev & mask) != 0;
  }

  @Override
  public void clear(int i) {
    if (i >= numBits) {
      return;
    }
    int idx = index(i);
    long mask = mask(i);
    storage.getAndAccumulate(idx, mask, (prev, m) -> prev & ~m);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    int startIdx = index(startIndex);
    int endIdx = index(endIndex - 1);
    for (int i = startIdx; i <= endIdx; i++) {
      long startMask = (i == startIdx) ? (-1L << (startIndex & 63)) : -1L;
      long endMask = (i == endIdx) ? (-1L << (endIndex & 63)) : 0L;
      long mask = startMask | endMask;
      storage.getAndAccumulate(i, mask, (prev, m) -> prev & ~m);
    }
  }

  @Override
  public int cardinality() {
    int count = 0;
    for (int i = 0; i < storage.length(); i++) {
      count += Long.bitCount(storage.get(i));
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
      long word = storage.get(i) & mask;
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

    for (int i = idx; i < storage.length(); i++) {
      long word = storage.get(i) & mask;
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
    long storageSize = (long) storage.length() * longSizeInBytes + arrayOverhead;
    return BASE_RAM_BYTES_USED + storageSize;
  }
}
