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
package org.apache.lucene.codecs.lucene99;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Specialized for Lucene99PostingsFormat. Cloned from FixedBitSet so it can be reused and resized.
 * Also provides a count of bits traversed when advancing using nextSetBit.
 */
final class PostingBits {
  private long[] bits; // Array of longs holding the bits
  private int numBits; // The number of bits in use
  private int numWords; // The exact number of longs needed to hold numBits (<= bits.length)

  PostingBits(int numWords) {
    this.numWords = numWords;
    this.bits = new long[numWords];
  }

  /**
   * Resizes the internal array if capacity needed to increase. This is a destructive operation; any
   * existing bits may be lost. It is the caller's responsibility to fill the array with bits,
   * properly zeroing out any trailing "ghost bits", and then call updateNumBits. It is only
   * necessary to zero out the number of words that are actually in use. It is safe to leave garbage
   * in bits[i] | i &gt; numWords.
   */
  void resize(int numBytes) {
    numWords = (numBytes + 7) >> 3;
    if (numWords > bits.length) {
      bits = new long[numWords];
    }
  }

  void updateNumBits() {
    int maxBits = numWords << 6;
    numBits = maxBits; // satisfy assertion in prevSetBit?
    numBits = prevSetBit(maxBits - 1) + 1;
  }

  int length() {
    return numBits;
  }

  long[] getBits() {
    return bits;
  }

  int nextSetBit(int index) {
    // Depends on the ghost bits being clear!
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;
    long word = bits[i] >> index; // skip all the bits to the right of index

    if (word != 0) {
      return index + Long.numberOfTrailingZeros(word);
    }

    while (++i < numWords) {
      word = bits[i];
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(word);
      }
    }

    return NO_MORE_DOCS;
  }

  int prevSetBit(int index) {
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

  /** Like nextSetBit, but returns NO_MORE_DOCS if index > length(). */
  int checkNextSetBit(int index) {
    if (index > numBits) {
      return NO_MORE_DOCS;
    }
    return nextSetBit(index);
  }

  /**
   * @param startIndex low end of the range, inclusive
   * @param endIndex high end of the range, exclusive
   * @return the number of set (1) bits in the given range
   */
  public int count(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits
        : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return 0;
    }
    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    // maybe we can have fewer operations if we shift the word and get rid of these masks...
    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    if (startWord == endWord) {
      return Long.bitCount(bits[startWord] & startmask & endmask);
    }
    int total = Long.bitCount(bits[startWord] & startmask) + Long.bitCount(bits[endWord] & endmask);
    for (int word = startWord + 1; word < endWord; word++) {
      total += Long.bitCount(bits[word]);
    }
    return total;
  }
}
