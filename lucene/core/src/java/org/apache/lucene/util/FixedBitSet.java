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
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * BitSet of fixed length (numBits), backed by accessible ({@link #getBits}) long[], accessed with
 * an int index, implementing {@link Bits} and {@link DocIdSet}. If you need to manage more than
 * 2.1B bits, use {@link LongBitSet}.
 *
 * @lucene.internal
 */
public final class FixedBitSet extends BitSet {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

  // An array that is small enough to use reasonable amounts of RAM and large enough to allow
  // Arrays#mismatch to use SIMD instructions and multiple registers under the hood.
  private static final long[] ZEROES = new long[32];

  private final long[] bits; // Array of longs holding the bits
  private final int numBits; // The number of bits in use
  private final int numWords; // The exact number of longs needed to hold numBits (<= bits.length)

  /**
   * If the given {@link FixedBitSet} is large enough to hold {@code numBits+1}, returns the given
   * bits, otherwise returns a new {@link FixedBitSet} which can hold {@code numBits+1} bits. That
   * means the bitset returned by this method can be safely called with {@code bits.set(numBits)}
   *
   * <p><b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of the given {@code
   * bits} if possible. Also, calling {@link #length()} on the returned bits may return a value
   * greater than {@code numBits+1}.
   */
  public static FixedBitSet ensureCapacity(FixedBitSet bits, int numBits) {
    if (numBits < bits.numBits) {
      return bits;
    } else {
      // Depends on the ghost bits being clear!
      // (Otherwise, they may become visible in the new instance)
      int numWords = bits2words(numBits);
      long[] arr = bits.getBits();
      if (numWords >= arr.length) {
        arr = ArrayUtil.grow(arr, numWords + 1);
      }
      return new FixedBitSet(arr, arr.length << 6);
    }
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0
    // returns 0!)
    return ((numBits - 1) >> 6) + 1;
  }

  /**
   * Returns the popcount or cardinality of the intersection of the two sets. Neither set is
   * modified.
   */
  public static long intersectionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    for (int i = 0; i < numCommonWords; ++i) {
      tot += Long.bitCount(a.bits[i] & b.bits[i]);
    }
    return tot;
  }

  /** Returns the popcount or cardinality of the union of the two sets. Neither set is modified. */
  public static long unionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    for (int i = 0; i < numCommonWords; ++i) {
      tot += Long.bitCount(a.bits[i] | b.bits[i]);
    }
    for (int i = numCommonWords; i < a.numWords; ++i) {
      tot += Long.bitCount(a.bits[i]);
    }
    for (int i = numCommonWords; i < b.numWords; ++i) {
      tot += Long.bitCount(b.bits[i]);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))". Neither set
   * is modified.
   */
  public static long andNotCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    for (int i = 0; i < numCommonWords; ++i) {
      tot += Long.bitCount(a.bits[i] & ~b.bits[i]);
    }
    for (int i = numCommonWords; i < a.numWords; ++i) {
      tot += Long.bitCount(a.bits[i]);
    }
    return tot;
  }

  /**
   * Creates a new FixedBitSet. The internally allocated long array will be exactly the size needed
   * to accommodate the numBits specified.
   *
   * @param numBits the number of bits needed
   */
  public FixedBitSet(int numBits) {
    this.numBits = numBits;
    bits = new long[bits2words(numBits)];
    numWords = bits.length;
  }

  /**
   * Creates a new FixedBitSet using the provided long[] array as backing store. The storedBits
   * array must be large enough to accommodate the numBits specified, but may be larger. In that
   * case the 'extra' or 'ghost' bits must be clear (or they may provoke spurious side-effects)
   *
   * @param storedBits the array to use as backing store
   * @param numBits the number of bits actually needed
   */
  public FixedBitSet(long[] storedBits, int numBits) {
    this.numWords = bits2words(numBits);
    if (numWords > storedBits.length) {
      throw new IllegalArgumentException(
          "The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits;

    assert verifyGhostBitsClear();
  }

  @Override
  public void clear() {
    Arrays.fill(bits, 0L);
  }

  /**
   * Checks if the bits past numBits are clear. Some methods rely on this implicit assumption:
   * search for "Depends on the ghost bits being clear!"
   *
   * @return true if the bits past numBits are clear.
   */
  private boolean verifyGhostBitsClear() {
    for (int i = numWords; i < bits.length; i++) {
      if (bits[i] != 0) return false;
    }

    if ((numBits & 0x3f) == 0) return true;

    long mask = -1L << numBits;

    return (bits[numWords - 1] & mask) == 0;
  }

  @Override
  public int length() {
    return numBits;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bits);
  }

  /** Expert. */
  public long[] getBits() {
    return bits;
  }

  /**
   * Returns number of set bits. NOTE: this visits every long in the backing bits array, and the
   * result is not internally cached!
   */
  @Override
  public int cardinality() {
    // Depends on the ghost bits being clear!
    long tot = 0;
    for (int i = 0; i < numWords; ++i) {
      tot += Long.bitCount(bits[i]);
    }
    return Math.toIntExact(tot);
  }

  /**
   * Return the number of set bits between indexes {@code from} inclusive and {@code to} exclusive.
   */
  public int cardinality(int from, int to) {
    Objects.checkFromToIndex(from, to, length());

    int cardinality = 0;

    // First, align `from` with a word start, ie. a multiple of Long.SIZE (64)
    if ((from & 0x3F) != 0) {
      long bits = this.bits[from >> 6] >>> from;
      int numBitsTilNextWord = -from & 0x3F;
      if (to - from < numBitsTilNextWord) {
        bits &= (1L << (to - from)) - 1L;
        return Long.bitCount(bits);
      }
      cardinality += Long.bitCount(bits);
      from += numBitsTilNextWord;
      assert (from & 0x3F) == 0;
    }

    for (int i = from >> 6, end = to >> 6; i < end; ++i) {
      cardinality += Long.bitCount(bits[i]);
    }

    // Now handle bits between the last complete word and to
    if ((to & 0x3F) != 0) {
      long bits = this.bits[to >> 6] << -to;
      cardinality += Long.bitCount(bits);
    }

    return cardinality;
  }

  @Override
  public int approximateCardinality() {
    // Naive sampling: compute the number of bits that are set on the first 16 longs every 1024
    // longs and scale the result by 1024/16.
    // This computes the pop count on ranges instead of single longs in order to take advantage of
    // vectorization.

    final int rangeLength = 16;
    final int interval = 1024;

    if (numWords <= interval) {
      return cardinality();
    }

    long popCount = 0;
    int maxWord;
    for (maxWord = 0; maxWord + interval < numWords; maxWord += interval) {
      for (int i = 0; i < rangeLength; ++i) {
        popCount += Long.bitCount(bits[maxWord + i]);
      }
    }

    popCount *= (interval / rangeLength) * numWords / maxWord;
    return (int) popCount;
  }

  @Override
  public boolean get(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (bits[i] & bitmask) != 0;
  }

  @Override
  public void set(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    bits[wordNum] |= bitmask;
  }

  @Override
  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }

  @Override
  public void clear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    bits[wordNum] &= ~bitmask;
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] &= ~bitmask;
    return val;
  }

  @Override
  public int nextSetBit(int index) {
    // Override with a version that skips the bound check on the result since we know it will not
    // go OOB:
    return nextSetBitInRange(index, numBits);
  }

  @Override
  public int nextSetBit(int start, int upperBound) {
    int res = nextSetBitInRange(start, upperBound);
    return res < upperBound ? res : DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Returns the next set bit in the specified range, but treats `upperBound` as a best-effort hint
   * rather than a hard requirement. Note that this may return a result that is >= upperBound in
   * some cases, so callers must add their own check if `upperBound` is a hard requirement.
   */
  private int nextSetBitInRange(int start, int upperBound) {
    // Depends on the ghost bits being clear!
    assert start >= 0 && start < numBits : "index=" + start + ", numBits=" + numBits;
    assert start < upperBound : "index=" + start + ", upperBound=" + upperBound;
    assert upperBound <= numBits : "upperBound=" + upperBound + ", numBits=" + numBits;
    int i = start >> 6;
    long word = bits[i] >> start; // skip all the bits to the right of index

    if (word != 0) {
      return start + Long.numberOfTrailingZeros(word);
    }

    int limit = upperBound == numBits ? numWords : bits2words(upperBound);
    while (++i < limit) {
      word = bits[i];
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(word);
      }
    }

    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f; // index within the word
    long word = (bits[i] << (63 - subIndex)); // skip all the bits to the left of index

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = bits[i];
      if (word != 0) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  @Override
  public void or(DocIdSetIterator iter) throws IOException {
    checkUnpositioned(iter);
    iter.nextDoc();
    iter.intoBitSet(DocIdSetIterator.NO_MORE_DOCS, this, 0);
  }

  /** Read {@code numBits} (between 1 and 63) bits from {@code bitSet} at {@code from}. */
  private static long readNBits(long[] bitSet, int from, int numBits) {
    assert numBits > 0 && numBits < Long.SIZE;
    long bits = bitSet[from >> 6] >>> from;
    int numBitsSoFar = Long.SIZE - (from & 0x3F);
    if (numBitsSoFar < numBits) {
      bits |= bitSet[(from >> 6) + 1] << -from;
    }
    return bits & ((1L << numBits) - 1);
  }

  /**
   * Or {@code length} bits starting at {@code sourceFrom} from {@code source} into {@code dest}
   * starting at {@code destFrom}.
   */
  public static void orRange(
      FixedBitSet source, int sourceFrom, FixedBitSet dest, int destFrom, int length) {
    assert length >= 0;
    Objects.checkFromIndexSize(sourceFrom, length, source.length());
    Objects.checkFromIndexSize(destFrom, length, dest.length());

    if (length == 0) {
      return;
    }

    long[] sourceBits = source.getBits();
    long[] destBits = dest.getBits();

    // First, align `destFrom` with a word start, ie. a multiple of Long.SIZE (64)
    if ((destFrom & 0x3F) != 0) {
      int numBitsNeeded = Math.min(-destFrom & 0x3F, length);
      long bits = readNBits(sourceBits, sourceFrom, numBitsNeeded) << destFrom;
      destBits[destFrom >> 6] |= bits;

      sourceFrom += numBitsNeeded;
      destFrom += numBitsNeeded;
      length -= numBitsNeeded;
    }

    if (length == 0) {
      return;
    }

    assert (destFrom & 0x3F) == 0;

    // Now OR at the word level
    int numFullWords = length >> 6;
    int sourceWordFrom = sourceFrom >> 6;
    int destWordFrom = destFrom >> 6;

    // Note: these two for loops auto-vectorize
    if ((sourceFrom & 0x3F) == 0) {
      // sourceFrom and destFrom are both aligned with a long[]
      for (int i = 0; i < numFullWords; ++i) {
        destBits[destWordFrom + i] |= sourceBits[sourceWordFrom + i];
      }
    } else {
      for (int i = 0; i < numFullWords; ++i) {
        destBits[destWordFrom + i] |=
            (sourceBits[sourceWordFrom + i] >>> sourceFrom)
                | (sourceBits[sourceWordFrom + i + 1] << -sourceFrom);
      }
    }

    sourceFrom += numFullWords << 6;
    destFrom += numFullWords << 6;
    length -= numFullWords << 6;

    // Finally handle tail bits
    if (length > 0) {
      long bits = readNBits(sourceBits, sourceFrom, length);
      destBits[destFrom >> 6] |= bits;
    }
  }

  /**
   * And {@code length} bits starting at {@code sourceFrom} from {@code source} into {@code dest}
   * starting at {@code destFrom}.
   */
  public static void andRange(
      FixedBitSet source, int sourceFrom, FixedBitSet dest, int destFrom, int length) {
    assert length >= 0 : length;
    Objects.checkFromIndexSize(sourceFrom, length, source.length());
    Objects.checkFromIndexSize(destFrom, length, dest.length());

    if (length == 0) {
      return;
    }

    long[] sourceBits = source.getBits();
    long[] destBits = dest.getBits();

    // First, align `destFrom` with a word start, ie. a multiple of Long.SIZE (64)
    if ((destFrom & 0x3F) != 0) {
      int numBitsNeeded = Math.min(-destFrom & 0x3F, length);
      long bits = readNBits(sourceBits, sourceFrom, numBitsNeeded) << destFrom;
      bits |= ~(((1L << numBitsNeeded) - 1) << destFrom);
      destBits[destFrom >> 6] &= bits;

      sourceFrom += numBitsNeeded;
      destFrom += numBitsNeeded;
      length -= numBitsNeeded;
    }

    if (length == 0) {
      return;
    }

    assert (destFrom & 0x3F) == 0;

    // Now AND at the word level
    int numFullWords = length >> 6;
    int sourceWordFrom = sourceFrom >> 6;
    int destWordFrom = destFrom >> 6;

    // Note: these two for loops auto-vectorize
    if ((sourceFrom & 0x3F) == 0) {
      // sourceFrom and destFrom are both aligned with a long[]
      for (int i = 0; i < numFullWords; ++i) {
        destBits[destWordFrom + i] &= sourceBits[sourceWordFrom + i];
      }
    } else {
      for (int i = 0; i < numFullWords; ++i) {
        destBits[destWordFrom + i] &=
            (sourceBits[sourceWordFrom + i] >>> sourceFrom)
                | (sourceBits[sourceWordFrom + i + 1] << -sourceFrom);
      }
    }

    sourceFrom += numFullWords << 6;
    destFrom += numFullWords << 6;
    length -= numFullWords << 6;

    // Finally handle tail bits
    if (length > 0) {
      long bits = readNBits(sourceBits, sourceFrom, length);
      bits |= (~0L << length);
      destBits[destFrom >> 6] &= bits;
    }
  }

  /** this = this OR other */
  public void or(FixedBitSet other) {
    orRange(other, 0, this, 0, other.length());
  }

  /** this = this XOR other */
  public void xor(FixedBitSet other) {
    xor(other.bits, other.numWords);
  }

  /** Does in-place XOR of the bits provided by the iterator. */
  public void xor(DocIdSetIterator iter) throws IOException {
    checkUnpositioned(iter);
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter);
      xor(bits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        flip(doc);
      }
    }
  }

  private void xor(long[] otherBits, int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", other.numWords=" + otherNumWords;
    final long[] thisBits = this.bits;
    int pos = Math.min(numWords, otherNumWords);
    while (--pos >= 0) {
      thisBits[pos] ^= otherBits[pos];
    }
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(FixedBitSet other) {
    // Depends on the ghost bits being clear!
    int pos = Math.min(numWords, other.numWords);
    while (--pos >= 0) {
      if ((bits[pos] & other.bits[pos]) != 0) return true;
    }
    return false;
  }

  /** this = this AND other */
  public void and(FixedBitSet other) {
    and(other.bits, other.numWords);
  }

  private void and(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits;
    int pos = Math.min(this.numWords, otherNumWords);
    while (--pos >= 0) {
      thisArr[pos] &= otherArr[pos];
    }
    if (this.numWords > otherNumWords) {
      Arrays.fill(thisArr, otherNumWords, this.numWords, 0L);
    }
  }

  public void andNot(DocIdSetIterator iter) throws IOException {
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      checkUnpositioned(iter);
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter);
      assert bits != null;
      andNot(bits);
    } else if (iter instanceof DocBaseBitSetIterator) {
      checkUnpositioned(iter);
      DocBaseBitSetIterator baseIter = (DocBaseBitSetIterator) iter;
      andNot(baseIter.getDocBase() >> 6, baseIter.getBitSet());
    } else {
      checkUnpositioned(iter);
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        clear(doc);
      }
    }
  }

  /** this = this AND NOT other */
  public void andNot(FixedBitSet other) {
    andNot(0, other.bits, other.numWords);
  }

  private void andNot(final int otherOffsetWords, FixedBitSet other) {
    andNot(otherOffsetWords, other.bits, other.numWords);
  }

  private void andNot(final int otherOffsetWords, final long[] otherArr, final int otherNumWords) {
    int pos = Math.min(numWords - otherOffsetWords, otherNumWords);
    final long[] thisArr = this.bits;
    while (--pos >= 0) {
      thisArr[pos + otherOffsetWords] &= ~otherArr[pos];
    }
  }

  /**
   * Scans the backing store to check if all bits are clear. The method is deliberately not called
   * "isEmpty" to emphasize it is not low cost (as isEmpty usually is).
   *
   * @return true if all bits are clear.
   */
  public boolean scanIsEmpty() {
    // This 'slow' implementation is still faster than any external one could be
    // (e.g.: (bitSet.length() == 0 || bitSet.nextSetBit(0) == -1))
    // especially for small BitSets
    // Depends on the ghost bits being clear!
    final int count = numWords;

    for (int i = 0; i < count; i += ZEROES.length) {
      int cmpLen = Math.min(ZEROES.length, bits.length - i);
      if (Arrays.equals(bits, i, i + cmpLen, ZEROES, 0, cmpLen) == false) {
        return false;
      }
    }

    return true;
  }

  /**
   * Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    /* Grrr, java shifting uses only the lower 6 bits of the count so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
     * long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
     * long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
     */

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    for (int i = startWord + 1; i < endWord; i++) {
      bits[i] = ~bits[i];
    }

    bits[endWord] ^= endmask;
  }

  /** Flip the bit at the provided index. */
  public void flip(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index; // mod 64 is implicit
    bits[wordNum] ^= bitmask;
  }

  /**
   * Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits
        : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    if (startWord == endWord) {
      bits[startWord] |= (startmask & endmask);
      return;
    }

    bits[startWord] |= startmask;
    Arrays.fill(bits, startWord + 1, endWord, -1L);
    bits[endWord] |= endmask;
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits
        : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      bits[startWord] &= (startmask | endmask);
      return;
    }

    bits[startWord] &= startmask;
    Arrays.fill(bits, startWord + 1, endWord, 0L);
    bits[endWord] &= endmask;
  }

  @Override
  public FixedBitSet clone() {
    long[] bits = new long[this.bits.length];
    System.arraycopy(this.bits, 0, bits, 0, numWords);
    return new FixedBitSet(bits, numBits);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FixedBitSet)) {
      return false;
    }
    FixedBitSet other = (FixedBitSet) o;
    if (numBits != other.numBits) {
      return false;
    }
    // Depends on the ghost bits being clear!
    return Arrays.equals(bits, other.bits);
  }

  @Override
  public int hashCode() {
    // Depends on the ghost bits being clear!
    long h = 0;
    for (int i = numWords; --i >= 0; ) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h >> 32) ^ h) + 0x98761234;
  }

  /** Make a copy of the given bits. */
  public static FixedBitSet copyOf(Bits bits) {
    if (bits instanceof FixedBits fixedBits) {
      // restore the original FixedBitSet
      bits = fixedBits.bitSet;
    }

    if (bits instanceof FixedBitSet) {
      return ((FixedBitSet) bits).clone();
    } else {
      int length = bits.length();
      FixedBitSet bitSet = new FixedBitSet(length);
      bitSet.set(0, length);
      for (int i = 0; i < length; ++i) {
        if (bits.get(i) == false) {
          bitSet.clear(i);
        }
      }
      return bitSet;
    }
  }

  /**
   * Convert this instance to read-only {@link Bits}. This is useful in the case that this {@link
   * FixedBitSet} is returned as a {@link Bits} instance, to make sure that consumers may not get
   * write access back by casting to a {@link FixedBitSet}. NOTE: Changes to this {@link
   * FixedBitSet} will be reflected on the returned {@link Bits}.
   */
  public Bits asReadOnlyBits() {
    return new FixedBits(bits, numBits);
  }

  @Override
  public void applyMask(FixedBitSet bitSet, int offset) {
    // Note: Some scorers don't track maxDoc and may thus call this method with an offset that is
    // beyond bitSet.length()
    int length = Math.min(bitSet.length(), length() - offset);
    if (length >= 0) {
      andRange(this, offset, bitSet, 0, length);
    }
    if (length < bitSet.length()
        && bitSet.nextSetBit(Math.max(0, length)) != DocIdSetIterator.NO_MORE_DOCS) {
      throw new IllegalArgumentException("Some bits are set beyond the end of live docs");
    }
  }

  /**
   * For each set bit from {@code from} inclusive to {@code to} exclusive, add {@code base} to the
   * bit index and call {@code consumer} on it. This is internally used by queries that use bit sets
   * as intermediate representations of their matches.
   */
  public void forEach(int from, int to, int base, IOIntConsumer consumer) throws IOException {
    Objects.checkFromToIndex(from, to, length());

    // First, align `from` with a word start, ie. a multiple of Long.SIZE (64)
    if ((from & 0x3F) != 0) {
      long bits = this.bits[from >> 6] >>> from;
      int numBitsTilNextWord = -from & 0x3F;
      if (to - from < numBitsTilNextWord) {
        // All bits are in a single word
        bits &= (1L << (to - from)) - 1L;
        forEach(bits, from + base, consumer);
        return;
      }
      forEach(bits, from + base, consumer);
      from += numBitsTilNextWord;
      assert (from & 0x3F) == 0;
    }

    for (int i = from >> 6, end = to >> 6; i < end; ++i) {
      forEach(bits[i], base + (i << 6), consumer);
    }

    // Now handle remaining bits in the last partial word
    if ((to & 0x3F) != 0) {
      long bits = this.bits[to >> 6] & ((1L << to) - 1);
      forEach(bits, base + (to & ~0x3F), consumer);
    }
  }

  private static void forEach(long bits, int base, IOIntConsumer consumer) throws IOException {
    while (bits != 0L) {
      int ntz = Long.numberOfTrailingZeros(bits);
      consumer.accept(base + ntz);
      bits ^= 1L << ntz;
    }
  }

  /**
   * Converts set bits in this bitset to an array of document IDs. Only processes bits from index
   * {@code from} (inclusive) to {@code to} (exclusive) and returns the number of bits copied.
   *
   * <p>Each set bit's position is converted to a document ID by adding the {@code base} value and
   * stored in the provided {@code array}. This method stops when there are no more set bits before
   * {@code to} or when there is no capacity left in the given {@code array}, whichever comes first.
   */
  public int intoArray(int from, int to, int base, int[] array) {
    Objects.checkFromToIndex(from, to, length());

    int offset = 0;
    // First, align `from` with a word start, ie. a multiple of Long.SIZE (64)
    if ((from & 0x3F) != 0) {
      long word = bits[from >> 6] >>> from;
      int numBitsTilNextWord = -from & 0x3F;
      if (to - from < numBitsTilNextWord) {
        // All bits are in a single word
        word &= (1L << (to - from)) - 1L;
        return word2Array(word, from + base, array, offset);
      }
      offset = word2Array(word, from + base, array, offset);
      from += numBitsTilNextWord;
      assert (from & 0x3F) == 0;
    }

    for (int i = from >> 6, end = to >> 6; i < end; ++i) {
      long word = bits[i];
      offset = word2Array(word, base + (i << 6), array, offset);
    }

    // Now handle remaining bits in the last partial word
    if ((to & 0x3F) != 0) {
      long word = bits[to >> 6] & ((1L << to) - 1);
      offset = word2Array(word, base + (to & ~0x3F), array, offset);
    }

    return offset;
  }

  private static int word2Array(long word, int base, int[] docs, int offset) {
    final int bitCount = Long.bitCount(word);

    if (bitCount >= 32 && docs.length - offset > bitCount) {
      return denseWord2Array(word, base, docs, offset);
    }

    int numBitsToCopy = Math.min(bitCount, docs.length - offset);

    for (int i = 0; i < numBitsToCopy; i++) {
      int ntz = Long.numberOfTrailingZeros(word);
      docs[offset + i] = base + ntz;
      word ^= 1L << ntz;
    }

    return offset + numBitsToCopy;
  }

  private static int denseWord2Array(long word, int base, int[] docs, int offset) {
    assert docs.length - offset >= Long.bitCount(word) + 1;

    final int lWord = (int) word;
    final int hWord = (int) (word >>> 32);
    final int offset32 = offset + Integer.bitCount(lWord);
    int hOffset = offset32;

    for (int i = 0; i < 32; i++) {
      docs[offset] = base + i;
      docs[hOffset] = base + i + 32;
      offset += (lWord >>> i) & 1;
      hOffset += (hWord >>> i) & 1;
    }

    docs[offset32] = base + 32 + Integer.numberOfTrailingZeros(hWord);

    return hOffset;
  }
}
