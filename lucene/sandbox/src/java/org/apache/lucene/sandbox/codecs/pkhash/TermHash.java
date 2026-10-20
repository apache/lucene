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

/**
 * The hash function persisted by {@link PrimaryKeyHashPostingsFormat}, and the mapping from a hash
 * to a partition, a home slot, a tag and a control byte.
 *
 * <p>The function is XXH3-64 ({@link Xxh3}) with the fixed seed {@link #SEED}. A key has a single
 * 64-bit hash, whose bits are split as follows, from the highest:
 *
 * <ul>
 *   <li>the top {@code partitionBits} bits select the partition ({@link #partition});
 *   <li>the next {@link #HOME_BITS} bits are mapped onto the home slots of the partition ({@link
 *       #homeSlot});
 *   <li>bits {@code [TAG_BITS, TAG_BITS + CONTROL_BITS)} are the low bits of the control byte
 *       ({@link #control});
 *   <li>bits {@code [0, TAG_BITS)} are the tag ({@link #tag}).
 * </ul>
 *
 * <p>There are at most 10 partition bits ({@link PrimaryKeyHashPostingsFormat#MAX_PARTITION_BITS}),
 * so the lowest bit of the home slot is bit {@code 64 - 10 - 31 = 23} or higher: the four parts
 * never share a bit, and keys with the same home slot are told apart by 23 independent bits.
 *
 * <p>If a field has a filter, a key's word and bits in it come from the key's {@link #filterHash}:
 * its partition, then the bits of its hash put through a finalizer ({@link #mix}) rather than those
 * of the hash itself.
 *
 * <p>The seed is a constant, not {@code StringHelper.GOOD_FAST_HASH_SEED} (which is random per
 * JVM), because the hash determines where keys live on disk. A constant seed also means a query can
 * hash each key once and reuse the hash for every segment. Correctness never depends on the hash:
 * every hit is verified against the full stored key, so a collision costs a probe, never a wrong
 * answer.
 */
final class TermHash {

  /** Identifies this function in the meta file. A different function must use a new id. */
  static final int XXH3_64_ID = 0;

  /**
   * The fixed seed: the 32-bit golden ratio, as an unsigned value. Changing it changes the on-disk
   * format.
   */
  static final long SEED = 0x9E3779B9L;

  private static final Xxh3 XXH3 = new Xxh3(SEED);

  /** Number of bits of the hash, just below the partition bits, that select the home slot. */
  static final int HOME_BITS = 31;

  /**
   * A slot word holds its record's offset plus one above this many low bits: the tag, then zero
   * bits.
   */
  static final int POINTER_SHIFT = 24;

  /** Number of bits of the hash kept as the in-slot tag: the lowest bits of a slot word. */
  static final int TAG_BITS = 16;

  /** Mask selecting the tag bits of a slot word. */
  static final long TAG_MASK = (1L << TAG_BITS) - 1;

  /**
   * Number of bits of the hash, just above the tag bits, that the control byte of a slot holds
   * below its high bit.
   */
  static final int CONTROL_BITS = 7;

  /** The control byte of an empty slot. */
  static final int EMPTY_CONTROL = 0;

  private TermHash() {}

  /** Hashes {@code length} bytes of {@code data} starting at {@code offset}. */
  static long hash(byte[] data, int offset, int length) {
    return XXH3.hash(data, offset, length);
  }

  /**
   * The control byte of a slot that holds a key: the high bit set, above bits {@code [TAG_BITS,
   * TAG_BITS + CONTROL_BITS)} of the key's hash. Never {@link #EMPTY_CONTROL}.
   */
  static int control(long hash) {
    return 0x80 | ((int) (hash >>> TAG_BITS) & ((1 << CONTROL_BITS) - 1));
  }

  /**
   * The bits of the hash that the tag and the control byte are taken from, in place. The writer
   * keeps them in the low bits of a slot word until the slot is known, then moves the control bits
   * to the slot's control byte, which is why {@link #tag} and {@link #control} give the same result
   * on such a word as on the hash.
   */
  static long tagAndControlBits(long hash) {
    return hash & ((1L << (TAG_BITS + CONTROL_BITS)) - 1);
  }

  /** The partition of a key: the top {@code partitionBits} bits of its hash. */
  static int partition(long hash, int partitionBits) {
    return partitionBits == 0 ? 0 : (int) (hash >>> (Long.SIZE - partitionBits));
  }

  /**
   * The home slot of a key in a partition of {@code capacity} slots: the {@link #HOME_BITS} bits of
   * its hash just below the partition bits, mapped onto {@code [0, capacity)} by multiply-shift
   * (Lemire's "fastrange"), which needs no power-of-two capacity. The mapping is monotonic in those
   * bits. A partition has fewer than 2<sup>31</sup> slots, so every slot can be a home slot.
   */
  static int homeSlot(long hash, int partitionBits, int capacity) {
    long bits = (hash << partitionBits) >>> (Long.SIZE - HOME_BITS);
    return (int) ((bits * capacity) >>> HOME_BITS);
  }

  /** The tag stored in a slot next to the record pointer: the low {@link #TAG_BITS} of the hash. */
  static int tag(long hash) {
    return (int) (hash & TAG_MASK);
  }

  /**
   * The key's hash through the 64-bit finalizer of MurmurHash3 (fmix64), written here because the
   * format depends on it: the bits of a key's {@link #filterHash} below its partition bits.
   *
   * <p>With {@link PrimaryKeyHashPostingsFormat#MAX_PARTITION_BITS} partition bits, the partition,
   * the home slot, the control byte and the tag use all 64 bits of the hash, so the filter cannot
   * take its bits from the hash itself without sharing some of them. The finalizer is a bijection
   * in which every bit of the result depends on every bit of the hash, so a key's word and bits in
   * the filter are unrelated to its home slot, control byte and tag.
   */
  static long mix(long hash) {
    long h = hash;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  /**
   * The hash that the {@link BlockedBloomFilter} of a field with {@code partitionBits} partition
   * bits takes the word and the bits of a key from: the top {@code partitionBits} bits of the key's
   * hash, which are its partition, then the other bits of its {@link #mix}.
   *
   * <p>The word is taken from the high bits of the filter hash, by a mapping that is monotonic in
   * them ({@link BlockedBloomFilter#word}), so the keys of a partition are in consecutive words,
   * from {@link BlockedBloomFilter#firstWord} of the partition to that of the next one, in
   * partition order: the writer builds the filter one partition at a time, as it builds the slots,
   * and only holds the words of one partition in heap. Within its partition's words, a key's word
   * and its bits come from its mix, see there.
   */
  static long filterHash(long hash, int partitionBits) {
    return filterHash(mix(hash), hash, partitionBits);
  }

  /**
   * {@link #filterHash(long, int)} of a key whose {@link #mix} is {@code mixed}, so that a key
   * looked up in fields with different numbers of partitions is mixed once: the top {@code
   * partitionBits} bits of {@code hash}, then the other bits of {@code mixed}.
   */
  static long filterHash(long mixed, long hash, int partitionBits) {
    final long mixedBits = -1L >>> partitionBits;
    return (hash & ~mixedBits) | (mixed & mixedBits);
  }
}
