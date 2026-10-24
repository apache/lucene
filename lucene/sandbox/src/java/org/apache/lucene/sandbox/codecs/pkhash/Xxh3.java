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

/*
 * This is a Java implementation of the 64-bit XXH3 hash function of xxHash
 * (https://github.com/Cyan4973/xxHash). Its algorithm, constants and default secret are those of
 * the reference implementation, xxhash.h, whose copyright follows:
 */

/*
 * xxHash - Extremely Fast Hash algorithm
 * Copyright (C) 2012-2021 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.lucene.sandbox.codecs.pkhash;

import java.util.HexFormat;
import org.apache.lucene.util.BitUtil;

/**
 * XXH3-64, the 64-bit variant of the XXH3 hash function of <a
 * href="https://github.com/Cyan4973/xxHash">xxHash</a>, with a seed and the default secret: for
 * inputs of any length, the same hash as {@code XXH3_64bits_withSeed} of xxHash 0.8.
 *
 * <p>An instance holds everything that derives from its seed alone, so that hashing allocates
 * nothing. Instances are immutable, and so safe to share between threads.
 */
final class Xxh3 {

  private static final long PRIME32_1 = 0x9E3779B1L;
  private static final long PRIME32_2 = 0x85EBCA77L;
  private static final long PRIME32_3 = 0xC2B2AE3DL;
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;
  private static final long PRIME_MX1 = 0x165667919E3779F9L;
  private static final long PRIME_MX2 = 0x9FB21C651E98DF25L;

  /** The default secret of xxHash, {@code XXH3_kSecret}, in the byte order of the reference. */
  private static final byte[] DEFAULT_SECRET =
      HexFormat.of()
          .parseHex(
              "b8fe6c3923a44bbe7c01812cf721ad1c"
                  + "ded46de9839097db7240a4a4b7b3671f"
                  + "cb79e64eccc0e578825ad07dccff7221"
                  + "b8084674f743248ee03590e6813a264c"
                  + "3c2852bb91c300cb88d0658b1b532ea3"
                  + "71644897a20df94e3819ef46a9deacd8"
                  + "a8fa763fe39c343ff9dcbbc7c70b4f1d"
                  + "8a51e04bcdb45931c89f7ec9d9787364"
                  + "eac5ac8334d3ebc3c581a0fffa1363eb"
                  + "170ddd51b7f0da49d316552629d4689e"
                  + "2b16be587d47a1fc8ff8b8d17ad031ce"
                  + "45cb3a8f95160428afd7fbcabb4b407e");

  private static final int SECRET_SIZE = 192;

  /** The smallest secret the reference accepts; the 129-240 byte path reads it near its end. */
  private static final int SECRET_SIZE_MIN = 136;

  /** Inputs longer than this take the striped accumulation. */
  private static final int MIDSIZE_MAX = 240;

  private static final int MIDSIZE_START_OFFSET = 3;
  private static final int MIDSIZE_LAST_OFFSET = 17;

  private static final int STRIPE_LEN = 64;
  private static final int SECRET_CONSUME_RATE = 8;
  private static final int STRIPES_PER_BLOCK = (SECRET_SIZE - STRIPE_LEN) / SECRET_CONSUME_RATE;
  private static final int BLOCK_LEN = STRIPE_LEN * STRIPES_PER_BLOCK;
  private static final int SECRET_LAST_ACC_START = 7;
  private static final int SECRET_MERGE_ACCS_START = 11;

  /** The hash of the empty input. */
  private final long emptyHash;

  /** What inputs of 1 to 3 bytes are xored with. */
  private final long bitflip1to3;

  /** What inputs of 4 to 8 bytes are xored with. */
  private final long bitflip4to8;

  /** What the first and the last 8 bytes of inputs of 9 to 16 bytes are xored with. */
  private final long bitflip9to16First, bitflip9to16Last;

  /**
   * The secret derived from the seed, as {@code XXH3_initCustomSecret} derives it. Its first 128
   * bytes are what 16-byte blocks of inputs of 17 to 240 bytes are xored with, and all of it is the
   * secret of longer inputs, for which the reference derives it on every call (or uses the default
   * secret if the seed is 0, which is what the derivation gives then).
   */
  private final byte[] secret;

  /**
   * For the 16-byte blocks after the first 128 bytes of inputs of 129 to 240 bytes: the 8 bytes of
   * the default secret at {@code MIDSIZE_START_OFFSET + 16 * i} plus the seed, and the next 8 minus
   * the seed, for i in {@code [0, 7)}; then the same at {@code SECRET_SIZE_MIN -
   * MIDSIZE_LAST_OFFSET}, for the last 16 bytes of the input.
   */
  private final long[] midsizeKeys;

  /** Creates the XXH3-64 function with the given seed. */
  Xxh3(long seed) {
    final byte[] k = DEFAULT_SECRET;
    emptyHash = xxh64Avalanche(seed ^ readLong(k, 56) ^ readLong(k, 64));
    bitflip1to3 = ((readInt(k, 0) ^ readInt(k, 4)) & 0xFFFFFFFFL) + seed;
    final long seed4to8 = seed ^ ((long) Integer.reverseBytes((int) seed) << 32);
    bitflip4to8 = (readLong(k, 8) ^ readLong(k, 16)) - seed4to8;
    bitflip9to16First = (readLong(k, 24) ^ readLong(k, 32)) + seed;
    bitflip9to16Last = (readLong(k, 40) ^ readLong(k, 48)) - seed;

    secret = new byte[SECRET_SIZE];
    for (int i = 0; i < SECRET_SIZE; i += 16) {
      BitUtil.VH_LE_LONG.set(secret, i, readLong(k, i) + seed);
      BitUtil.VH_LE_LONG.set(secret, i + 8, readLong(k, i + 8) - seed);
    }

    midsizeKeys = new long[2 * 8];
    for (int i = 0; i < 8; i++) {
      final int offset =
          i < 7 ? MIDSIZE_START_OFFSET + 16 * i : SECRET_SIZE_MIN - MIDSIZE_LAST_OFFSET;
      midsizeKeys[2 * i] = readLong(k, offset) + seed;
      midsizeKeys[2 * i + 1] = readLong(k, offset + 8) - seed;
    }
  }

  /** Hashes the {@code length} bytes of {@code data} from {@code offset}. */
  long hash(byte[] data, int offset, int length) {
    if (length <= 16) {
      if (length > 8) {
        return hash9to16(data, offset, length);
      } else if (length >= 4) {
        return hash4to8(data, offset, length);
      } else if (length > 0) {
        return hash1to3(data, offset, length);
      } else {
        return emptyHash;
      }
    } else if (length <= 128) {
      return hash17to128(data, offset, length);
    } else if (length <= MIDSIZE_MAX) {
      return hash129to240(data, offset, length);
    } else {
      return hashLong(data, offset, length);
    }
  }

  private long hash1to3(byte[] data, int offset, int length) {
    final int c1 = data[offset] & 0xFF;
    final int c2 = data[offset + (length >> 1)] & 0xFF;
    final int c3 = data[offset + length - 1] & 0xFF;
    final long combined = ((c1 << 16) | (c2 << 24) | c3 | (length << 8)) & 0xFFFFFFFFL;
    return xxh64Avalanche(combined ^ bitflip1to3);
  }

  private long hash4to8(byte[] data, int offset, int length) {
    final long first = readInt(data, offset) & 0xFFFFFFFFL;
    final long last = readInt(data, offset + length - 4) & 0xFFFFFFFFL;
    return rrmxmx((last + (first << 32)) ^ bitflip4to8, length);
  }

  private long hash9to16(byte[] data, int offset, int length) {
    final long first = readLong(data, offset) ^ bitflip9to16First;
    final long last = readLong(data, offset + length - 8) ^ bitflip9to16Last;
    return avalanche(length + Long.reverseBytes(first) + last + multiplyFold(first, last));
  }

  /**
   * Mixes the 16 bytes of {@code data} at {@code position} with the 16 bytes of the derived secret
   * at {@code secretOffset}: {@code XXH3_mix16B}.
   */
  private long mix16(byte[] data, int position, int secretOffset) {
    return multiplyFold(
        readLong(data, position) ^ readLong(secret, secretOffset),
        readLong(data, position + 8) ^ readLong(secret, secretOffset + 8));
  }

  private long hash17to128(byte[] data, int offset, int length) {
    final int end = offset + length;
    long acc = length * PRIME64_1;
    if (length > 32) {
      if (length > 64) {
        if (length > 96) {
          acc += mix16(data, offset + 48, 96);
          acc += mix16(data, end - 64, 112);
        }
        acc += mix16(data, offset + 32, 64);
        acc += mix16(data, end - 48, 80);
      }
      acc += mix16(data, offset + 16, 32);
      acc += mix16(data, end - 32, 48);
    }
    acc += mix16(data, offset, 0);
    acc += mix16(data, end - 16, 16);
    return avalanche(acc);
  }

  private long hash129to240(byte[] data, int offset, int length) {
    long acc = length * PRIME64_1;
    for (int i = 0; i < 8; i++) {
      acc += mix16(data, offset + 16 * i, 16 * i);
    }
    acc = avalanche(acc);
    final long[] keys = midsizeKeys;
    // the last 16 bytes of the input
    long accEnd =
        multiplyFold(
            readLong(data, offset + length - 16) ^ keys[14],
            readLong(data, offset + length - 8) ^ keys[15]);
    final int rounds = length / 16;
    for (int i = 8; i < rounds; i++) {
      final int position = offset + 16 * i;
      final int key = 2 * (i - 8);
      accEnd +=
          multiplyFold(
              readLong(data, position) ^ keys[key], readLong(data, position + 8) ^ keys[key + 1]);
    }
    return avalanche(acc + accEnd);
  }

  /**
   * Inputs longer than {@link #MIDSIZE_MAX} bytes: {@code XXH3_hashLong_64b_internal}, with the 8
   * accumulators in local variables, so that nothing is allocated. The last 64 bytes of the input
   * are accumulated as one more stripe, which may overlap the previous one.
   */
  private long hashLong(byte[] data, int offset, int length) {
    long acc0 = PRIME32_3;
    long acc1 = PRIME64_1;
    long acc2 = PRIME64_2;
    long acc3 = PRIME64_3;
    long acc4 = PRIME64_4;
    long acc5 = PRIME32_2;
    long acc6 = PRIME64_5;
    long acc7 = PRIME32_1;

    final int fullBlocks = (length - 1) / BLOCK_LEN;
    // stripes of the last block, plus the last 64 bytes of the input
    final int lastStripes = ((length - 1) - BLOCK_LEN * fullBlocks) / STRIPE_LEN + 1;
    for (int block = 0; ; block++) {
      final boolean lastBlock = block == fullBlocks;
      final int blockStart = offset + block * BLOCK_LEN;
      final int stripes = lastBlock ? lastStripes : STRIPES_PER_BLOCK;
      for (int stripe = 0; stripe < stripes; stripe++) {
        final int p;
        final int s;
        if (lastBlock && stripe == stripes - 1) {
          p = offset + length - STRIPE_LEN;
          s = SECRET_SIZE - STRIPE_LEN - SECRET_LAST_ACC_START;
        } else {
          p = blockStart + stripe * STRIPE_LEN;
          s = stripe * SECRET_CONSUME_RATE;
        }
        // XXH3_accumulate_512: lane i adds its input to lane i ^ 1
        final long d0 = readLong(data, p);
        final long d1 = readLong(data, p + 8);
        final long d2 = readLong(data, p + 16);
        final long d3 = readLong(data, p + 24);
        final long d4 = readLong(data, p + 32);
        final long d5 = readLong(data, p + 40);
        final long d6 = readLong(data, p + 48);
        final long d7 = readLong(data, p + 56);
        acc0 += d1 + multiplyHalves(d0 ^ readLong(secret, s));
        acc1 += d0 + multiplyHalves(d1 ^ readLong(secret, s + 8));
        acc2 += d3 + multiplyHalves(d2 ^ readLong(secret, s + 16));
        acc3 += d2 + multiplyHalves(d3 ^ readLong(secret, s + 24));
        acc4 += d5 + multiplyHalves(d4 ^ readLong(secret, s + 32));
        acc5 += d4 + multiplyHalves(d5 ^ readLong(secret, s + 40));
        acc6 += d7 + multiplyHalves(d6 ^ readLong(secret, s + 48));
        acc7 += d6 + multiplyHalves(d7 ^ readLong(secret, s + 56));
      }
      if (lastBlock) {
        break;
      }
      // XXH3_scrambleAcc, with the last 64 bytes of the secret
      final int s = SECRET_SIZE - STRIPE_LEN;
      acc0 = scramble(acc0, readLong(secret, s));
      acc1 = scramble(acc1, readLong(secret, s + 8));
      acc2 = scramble(acc2, readLong(secret, s + 16));
      acc3 = scramble(acc3, readLong(secret, s + 24));
      acc4 = scramble(acc4, readLong(secret, s + 32));
      acc5 = scramble(acc5, readLong(secret, s + 40));
      acc6 = scramble(acc6, readLong(secret, s + 48));
      acc7 = scramble(acc7, readLong(secret, s + 56));
    }

    // XXH3_mergeAccs
    final int s = SECRET_MERGE_ACCS_START;
    long result = length * PRIME64_1;
    result += multiplyFold(acc0 ^ readLong(secret, s), acc1 ^ readLong(secret, s + 8));
    result += multiplyFold(acc2 ^ readLong(secret, s + 16), acc3 ^ readLong(secret, s + 24));
    result += multiplyFold(acc4 ^ readLong(secret, s + 32), acc5 ^ readLong(secret, s + 40));
    result += multiplyFold(acc6 ^ readLong(secret, s + 48), acc7 ^ readLong(secret, s + 56));
    return avalanche(result);
  }

  private static long scramble(long acc, long key) {
    return ((acc ^ (acc >>> 47)) ^ key) * PRIME32_1;
  }

  /** The product of the low and the high 32 bits of {@code x}, as unsigned values. */
  private static long multiplyHalves(long x) {
    return (x & 0xFFFFFFFFL) * (x >>> 32);
  }

  /** The 128-bit product of {@code a} and {@code b}, unsigned, its two halves xored together. */
  private static long multiplyFold(long a, long b) {
    return (a * b) ^ Math.unsignedMultiplyHigh(a, b);
  }

  /** {@code XXH3_avalanche}. */
  private static long avalanche(long h) {
    h ^= h >>> 37;
    h *= PRIME_MX1;
    return h ^ (h >>> 32);
  }

  /** {@code XXH3_rrmxmx}. */
  private static long rrmxmx(long h, int length) {
    h ^= Long.rotateLeft(h, 49) ^ Long.rotateLeft(h, 24);
    h *= PRIME_MX2;
    h ^= (h >>> 35) + length;
    h *= PRIME_MX2;
    return h ^ (h >>> 28);
  }

  /** {@code XXH64_avalanche}. */
  private static long xxh64Avalanche(long h) {
    h ^= h >>> 33;
    h *= PRIME64_2;
    h ^= h >>> 29;
    h *= PRIME64_3;
    return h ^ (h >>> 32);
  }

  private static long readLong(byte[] bytes, int offset) {
    return (long) BitUtil.VH_LE_LONG.get(bytes, offset);
  }

  private static int readInt(byte[] bytes, int offset) {
    return (int) BitUtil.VH_LE_INT.get(bytes, offset);
  }
}
