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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.FixedBitSet;

/** Utility class to decode postings. */
public class PostingDecodingUtil {

  /** The wrapper {@link IndexInput}. */
  public final IndexInput in;

  /** Sole constructor, called by sub-classes. */
  protected PostingDecodingUtil(IndexInput in) {
    this.in = in;
  }

  /**
   * Core methods for decoding blocks of docs / freqs / positions / offsets.
   *
   * <ul>
   *   <li>Read {@code count} ints.
   *   <li>For all {@code i} &gt;= 0 so that {@code bShift - i * dec} &gt; 0, apply shift {@code
   *       bShift - i * dec} and store the result in {@code b} at offset {@code count * i}.
   *   <li>Apply mask {@code cMask} and store the result in {@code c} starting at offset {@code
   *       cIndex}.
   * </ul>
   */
  public void splitInts(
      int count, int[] b, int bShift, int dec, int bMask, int[] c, int cIndex, int cMask)
      throws IOException {
    // Default implementation, which takes advantage of the C2 compiler's loop unrolling and
    // auto-vectorization.
    in.readInts(c, cIndex, count);
    int maxIter = (bShift - 1) / dec;
    for (int i = 0; i < count; ++i) {
      for (int j = 0; j <= maxIter; ++j) {
        b[count * j + i] = (c[cIndex + i] >>> (bShift - j * dec)) & bMask;
      }
      c[cIndex + i] &= cMask;
    }
  }

  public final int denseBitsetToArray(FixedBitSet bitSet, int from, int to, int base, int[] array) {
    Objects.checkFromToIndex(from, to, bitSet.length());

    int offset = 0;
    long[] bits = bitSet.getBits();
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

  int word2Array(long word, int base, int[] docs, int offset) {
    if (word == 0) {
      return offset;
    }

    final int bitCount = Long.bitCount(word);
    int i = 0;

    // Unroll the loop for 4 iterations at a time, as we know it is dense.
    for (; i < bitCount - 3; i += 4) {
      int ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
    }

    // Handle the remaining bits
    for (; i < bitCount; i++) {
      int ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
    }

    return offset;
  }
}
