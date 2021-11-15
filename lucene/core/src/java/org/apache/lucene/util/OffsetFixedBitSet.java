package org.apache.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * a {@link FixedBitSet} that knows its min value.
 */
public class OffsetFixedBitSet extends BitSet implements Bits, Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OffsetFixedBitSet.class);

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
    bitSet.clear(i - offsetBits);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    bitSet.clear(startIndex - offsetBits, endIndex - offsetBits);
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
