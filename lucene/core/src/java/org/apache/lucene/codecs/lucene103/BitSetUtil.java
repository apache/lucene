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
package org.apache.lucene.codecs.lucene103;

import java.util.Objects;
import org.apache.lucene.util.FixedBitSet;

class BitSetUtil {

  /**
   * Converts set bits in the given bitset to an array of document IDs. Only processes bits from
   * index {@code from} (inclusive) to {@code to} (exclusive) and returns the number of bits set in
   * this range.
   *
   * <p>Each set bit's position is converted to a document ID by adding the {@code base} value and
   * stored in the provided {@code array}.
   *
   * <p>NOTE: Caller need to ensure the {@code array} has a length greater than or equal to {@code
   * bitSet.cardinality(from, to) + 1}.
   */
  static int denseBitsetToArray(FixedBitSet bitSet, int from, int to, int base, int[] array) {
    assert bitSet.cardinality(from, to) + 1 <= array.length
        : "Array length must be at least bitSet.cardinality(from, to) + 1";

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

  private static int word2Array(long word, int base, int[] docs, int offset) {
    final int bitCount = Long.bitCount(word);

    if (bitCount >= 32) {
      return denseWord2Array(word, base, docs, offset);
    }

    for (int i = 0; i < bitCount; i++) {
      int ntz = Long.numberOfTrailingZeros(word);
      docs[offset++] = base + ntz;
      word ^= 1L << ntz;
    }

    return offset;
  }

  private static int denseWord2Array(long word, int base, int[] docs, int offset) {
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
