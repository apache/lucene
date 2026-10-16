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
 * The Bloom filter that a field of {@link PrimaryKeyHashPostingsFormat} may have. The staged
 * lookups read it before the control bytes; exact seeks do not read it. It is a register-blocked
 * Bloom filter (Lang, Neumann, Kemper and Boncz, <i>Performance-Optimal Filtering: Bloom Overtakes
 * Cuckoo at High Throughput</i>, PVLDB 12(5), 2019), the variant of the blocked Bloom filters of
 * Putze, Sanders and Singler (<i>Cache-, Hash- and Space-Efficient Bloom Filters</i>, WEA 2007)
 * whose blocks are machine words, applied to the {@link TermHash#filterHash filter hash} of a key.
 *
 * <p>The filter is an array of 64-bit words. A key sets {@value #K} bits of one word. It may be in
 * the set if all of them are set, and is certainly not in it otherwise. A lookup reads one word, 8
 * bytes at a multiple of 8, which never crosses a cache line, and checks it with an and-not and a
 * count of leading zeros, without a branch ({@link #mayContain}). These bits of a filter hash
 * decide where a key's bits are, from the highest:
 *
 * <ul>
 *   <li>bits {@code [30, 64)} choose the key's word ({@link #word}): as a fraction of
 *       2<sup>34</sup>, times the number of words, by multiply-shift (Lemire's "fastrange"), as
 *       home slots are, so that the mapping is monotonic in them;
 *   <li>bits {@code [6i, 6i + 6)}, for {@code i} from 0 to 4, are the positions in the word of the
 *       five bits of the key ({@link #mask}).
 * </ul>
 *
 * No bit of the filter hash decides both, so a key's bits are independent of its word. The top bits
 * of a filter hash are the key's partition, so the keys of a partition are in consecutive words
 * ({@link #firstWord}), and the writer only holds those of one partition at a time ({@link
 * #add(long[], int, int, long)}). Its other bits are those of the key's {@link TermHash#mix}, and
 * are independent of its home slot, control byte and tag. The five positions are taken from
 * separate bits, so they may repeat, and computing them takes no table or loop.
 *
 * <p>On disk, a word is a little-endian long, the first word first.
 *
 * <p>With {@code b} bits per key, there are {@code ceil(numKeys * b / 64)} words, and a word holds
 * {@code 64 / b} keys on average. A key that is not in the set passes the filter if each of its
 * bits is set in its word. With the number of keys of a word following a Poisson distribution, that
 * happens for about 20% of such keys at 4 bits per key, 7.6% at 6, 3.4% at 8, 1.78% at 10, 1.04% at
 * 12 and 0.43% at 16. A key sets five bits at any number of bits per key, so a set of keys computes
 * the bits of every key once for the filters of all segments ({@link HashedTerms#filterMasks}).
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
   * The number of words of the filter of {@code numKeys} keys at {@code bitsPerKey} bits per key:
   * enough for that many bits, and at least one.
   */
  static long numWords(long numKeys, int bitsPerKey) {
    assert numKeys >= 0 && bitsPerKey > 0;
    return Math.max(1, Math.ceilDiv(Math.multiplyExact(numKeys, bitsPerKey), Long.SIZE));
  }

  /**
   * The word of a key whose filter hash is {@code filterHash}, in a filter of {@code numWords}
   * words: the bits of the filter hash above its {@link #POSITION_HASH_BITS} low bits, which are
   * the positions of the key's bits, mapped onto {@code [0, numWords)} by the high 64 bits of their
   * product with the number of words, which is monotonic in them.
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
   * The first word that a key of partition {@code p} of a field with {@code partitionBits}
   * partition bits can be in, in a filter of {@code numWords} words: the word of the smallest
   * filter hash of the partition, whose top {@code partitionBits} bits are {@code p} ({@link
   * TermHash#filterHash(long, int)}). Since {@link #word} is monotonic, the keys of partition
   * {@code p} are in the words from this one to the first word of partition {@code p + 1}, which
   * they may share with the keys of the next partitions, and at most {@code ceil(numWords / 2^
   * partitionBits) + 1} words.
   */
  static int firstWord(int p, int partitionBits, int numWords) {
    assert p >= 0 && p < 1 << partitionBits : p + " of " + partitionBits + " partition bits";
    return p == 0 ? 0 : word((long) p << (Long.SIZE - partitionBits), numWords);
  }

  /**
   * The bits that a key whose filter hash is {@code filterHash} sets in its word: bit {@code
   * (filterHash >>> 6i) & 63} for {@code i} from 0 to 4, from 1 to 5 bits. A long shifted by a long
   * only takes the low 6 bits of the distance, so the positions are not masked. The low 32 bits of
   * a filter hash are those of the key's {@link TermHash#mix}, whatever the field's number of
   * partitions, so a set of keys computes the masks of its keys once for all segments ({@link
   * HashedTerms#filterMasks}).
   */
  static long mask(long filterHash) {
    return (1L << filterHash)
        | (1L << (filterHash >>> POSITION_BITS))
        | (1L << (filterHash >>> (2 * POSITION_BITS)))
        | (1L << (filterHash >>> (3 * POSITION_BITS)))
        | (1L << (filterHash >>> (4 * POSITION_BITS)));
  }

  /**
   * Adds a key, given its filter hash, to a filter of {@code words.length} words held as longs. For
   * tests.
   */
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
   * 1 if a key whose {@link #mask} is {@code mask} may be in the set, since all of its bits are set
   * in its word, {@code word}, and 0 if it is not in it. There is no branch, so a loop over keys of
   * which some pass and others do not has nothing to mispredict: the bits of the key that are
   * missing from the word are 0 if and only if they have 64 leading zeros, and 64 is the only count
   * of leading zeros of a long that has bit 6 set.
   */
  static int mayContain(long word, long mask) {
    return Long.numberOfLeadingZeros(mask & ~word) >>> 6;
  }
}
