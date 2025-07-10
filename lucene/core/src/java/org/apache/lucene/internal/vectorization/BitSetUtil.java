package org.apache.lucene.internal.vectorization;

import java.util.Objects;
import org.apache.lucene.util.FixedBitSet;

public class BitSetUtil {

  static BitSetUtil INSTANCE = new BitSetUtil();

  BitSetUtil() {}

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
