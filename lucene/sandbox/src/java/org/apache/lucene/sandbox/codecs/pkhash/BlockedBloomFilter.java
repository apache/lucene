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
package org.apache.lucene.sandbox.codecs.pkhash;

import org.apache.lucene.util.ArrayUtil;

/**
 * The Bloom filter of a field of {@link PrimaryKeyHashPostingsFormat}: a register-blocked Bloom
 * filter, in which a key sets {@value #K} bits of one 64-bit word, both chosen from its {@link
 * TermHash#filterHash filter hash}. The reference and the exact layout are in the format's file
 * format documentation. Only batched lookups read it.
 */
final class BlockedBloomFilter {

  /** The number of bits that a key sets in its word. */
  static final int K = 5;

  /** Bits of the filter hash per position of a bit of a key: enough for a bit of a long. */
  private static final int POSITION_BITS = 6;

  /** The low bits of a filter hash that the positions of the bits of a key are taken from. */
  static final int POSITION_HASH_BITS = K * POSITION_BITS;

  /** The bits of a filter hash that its word is taken from: all but its positions. */
  private static final long WORD_HASH_MASK = -1L << POSITION_HASH_BITS;

  /**
   * The most words a filter may have, so that word indices are ints, and the writer can hold all
   * the words of a field of one partition in one array.
   */
  static final int MAX_WORDS = ArrayUtil.MAX_ARRAY_LENGTH;

  private BlockedBloomFilter() {}

  /**
   * The number of words of the filter of {@code numKeys} keys at {@code bitsPerKey} bits per key,
   * at least one.
   */
  static long numWords(long numKeys, int bitsPerKey) {
    assert numKeys >= 0 && bitsPerKey > 0;
    return Math.max(1, Math.ceilDiv(Math.multiplyExact(numKeys, bitsPerKey), Long.SIZE));
  }

  /**
   * The word of a key, by multiply-shift (Lemire's "fastrange") of the filter hash bits above the
   * bit positions. Monotonic in the filter hash.
   */
  static int word(long filterHash, int numWords) {
    return (int) Math.unsignedMultiplyHigh(filterHash & WORD_HASH_MASK, numWords);
  }

  /**
   * Where the {@link #word} of a key whose filter hash is {@code filterHash} starts in a filter of
   * {@code numWords} words.
   */
  static long wordOffset(long filterHash, int numWords) {
    return (long) word(filterHash, numWords) * Long.BYTES;
  }

  /**
   * The first word that a key of partition {@code p} can be in. A filter hash starts with its
   * partition and {@link #word} is monotonic, so each partition's keys are in consecutive words,
   * and the writer only holds the words of one partition at a time.
   */
  static int firstWord(int p, int partitionBits, int numWords) {
    assert p >= 0 && p < 1 << partitionBits : p + " of " + partitionBits + " partition bits";
    return p == 0 ? 0 : word((long) p << (Long.SIZE - partitionBits), numWords);
  }

  /**
   * The bits that a key sets in its word: bit {@code (filterHash >>> 6i) & 63} for {@code i} from 0
   * to 4 (a long shift only uses the low 6 bits of its distance). They depend only on the key's
   * {@link TermHash#mix}, so a set of keys computes them once for all segments ({@link
   * HashedTerms#filterMasks}).
   */
  static long mask(long filterHash) {
    return (1L << filterHash)
        | (1L << (filterHash >>> POSITION_BITS))
        | (1L << (filterHash >>> (2 * POSITION_BITS)))
        | (1L << (filterHash >>> (3 * POSITION_BITS)))
        | (1L << (filterHash >>> (4 * POSITION_BITS)));
  }

  /** Adds a key, given its filter hash, to the filter {@code words}. For tests. */
  static void add(long[] words, long filterHash) {
    add(words, 0, words.length, filterHash);
  }

  /**
   * Adds a key, given its filter hash, to a filter of {@code numWords} words, of which {@code
   * window} holds the words from word {@code windowStart} on; the key's word must be one of them.
   */
  static void add(long[] window, int windowStart, int numWords, long filterHash) {
    final int index = word(filterHash, numWords) - windowStart;
    assert index >= 0 && index < window.length
        : "word " + word(filterHash, numWords) + " is not in the window from " + windowStart;
    window[index] |= mask(filterHash);
  }

  /**
   * 1 if all of a key's bits are set in its {@code word}, else 0, without a branch: {@code mask &
   * ~word} is 0 exactly when it has 64 leading zeros.
   */
  static int mayContain(long word, long mask) {
    return Long.numberOfLeadingZeros(mask & ~word) >>> 6;
  }
}
