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

  private final int[] scratch = new int[64];

  final int denseBitsetToArray(FixedBitSet bitSet, int from, int to, int base, int[] array) {
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

  private int word2Array(long word, int base, int[] docs, int offset) {
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

  private int denseWord2Array(long word, int base, int[] docs, int offset) {
    final int lWord = (int) word;
    final int hWord = (int) (word >>> 32);
    final int[] scratch = this.scratch;

    for (int i = 0; i < 32; i++) {
      scratch[i] = (lWord >>> i) & 1;
      scratch[i + 32] = (hWord >>> i) & 1;
    }

    for (int i = 0; i < 64; i++) {
      docs[offset] = base + i;
      offset += scratch[i];
    }

    return offset;
  }
}
