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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestXxh3 extends LuceneTestCase {

  /**
   * The lengths of the golden values: every length of {@code [0, 16]}, both ends of each branch of
   * 17-128 and of 129-240, the lengths where the number of stripes in the last block changes (257,
   * 1025, 1089), one full block with each number of stripes from 1 to 16 in the last block, then
   * several full blocks.
   */
  private static final int[] GOLDEN_LENGTHS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 32, 33, 64, 65, 96, 97, 128, 129,
    200, 240, 241, 257, 500, 1024, 1025, 1038, 1089, 1115, 1192, 1269, 1282, 1359, 1436, 1513, 1590,
    1603, 1680, 1757, 1834, 1911, 1924, 2001, 2048, 3000, 4096, 32766
  };

  /**
   * The input of the golden values: the little-endian outputs of splitmix64 from a state of 0. They
   * do not repeat, and start with bytes of 0x80 or more (0xAF, 0xCD), which the arithmetic of
   * inputs of 1 to 3 bytes must not sign-extend.
   */
  private static byte[] goldenInput(int length) {
    final byte[] bytes = new byte[length];
    long state = 0;
    for (int i = 0; i < length; i += Long.BYTES) {
      state += 0x9E3779B97F4A7C15L;
      long z = state;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      z ^= z >>> 31;
      for (int j = 0; j < Long.BYTES && i + j < length; j++) {
        bytes[i + j] = (byte) (z >>> (j * Byte.SIZE));
      }
    }
    return bytes;
  }

  /*
   * Golden values: XXH3_64bits_withSeed of the golden input with TermHash.SEED, for every length of
   * GOLDEN_LENGTHS, generated with python's xxhash 3.6.0 (libxxhash 0.8.2):
   *
   *   import xxhash
   *   M = (1 << 64) - 1
   *   def splitmix64(n):
   *       out, state = bytearray(), 0
   *       while len(out) < n:
   *           state = (state + 0x9E3779B97F4A7C15) & M
   *           z = state
   *           z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & M
   *           z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & M
   *           out += (z ^ (z >> 31)).to_bytes(8, 'little')
   *       return bytes(out[:n])
   *   data = splitmix64(32766)
   *   [xxhash.xxh3_64_intdigest(data[:n], seed=0x9E3779B9) for n in GOLDEN_LENGTHS]
   */
  private static final long[] GOLDEN_VALUES = {
    0x64ce21f217f362b1L, 0x2a0b0838d1d5334fL, 0xece4aa9da8fb41edL, 0x2e566d4505160f38L,
    0x12a84c4a2b97adb7L, 0xadc12f2f1736749cL, 0x0f84630f126b5abaL, 0xabcc20809f71d579L,
    0xc5287311358e1d9fL, 0x54c7117c7e40363aL, 0x560b653b0b73c545L, 0x7af31a3531753a29L,
    0x26dfc0e097730215L, 0x936359808c12d083L, 0x0b003ca54f195581L, 0xd03aec41fbc61aadL,
    0xe0a01cf72f70db10L, 0x564b34b248903773L, 0x23179cccd8a423fdL, 0x6f172ce1e2aa64afL,
    0xd01cbc6d210d911fL, 0x9eebf2ac93ed7716L, 0x60adee9f73534aa5L, 0x8fe26471f350d778L,
    0xa0e44329ae7e4003L, 0x03fdf459bcb2fef6L, 0x5cfc286015f8bafaL, 0x4cdc50e332887058L,
    0xb82b19a0ada66d96L, 0xba095fcdc3a5cf64L, 0xb7be6d1d79d675f7L, 0xca6732750206c9e1L,
    0x03d822fe7e3b16c0L, 0x6aff3a06806e6fa5L, 0x75a649e4d1fee733L, 0x42f10c2842c2de01L,
    0x1746dd658f799c71L, 0x1ca74a4e73436dc9L, 0xf33f40b57c7fbd68L, 0x77978fca66e38726L,
    0x13af44687c3057a6L, 0xb790890db3ed6d05L, 0xf0ed04fdbc80741fL, 0x125acec2094c8868L,
    0xe4cdb751633a8c79L, 0x1b66f70f1b2784fcL, 0xfa361571038a4048L, 0xa8e78f32c59729edL,
    0xee1eb8c5f3d6f3f7L, 0xef0dd21edc74d5c6L, 0x854872cc62ddd81eL, 0x97958711b40dad21L,
    0x65b64705f2a6ddd2L, 0x30798013f158e3c8L,
  };

  public void testGoldenValues() {
    assertEquals(GOLDEN_LENGTHS.length, GOLDEN_VALUES.length);
    final byte[] input = goldenInput(GOLDEN_LENGTHS[GOLDEN_LENGTHS.length - 1]);
    final Xxh3 xxh3 = new Xxh3(TermHash.SEED);
    for (int i = 0; i < GOLDEN_LENGTHS.length; i++) {
      final int length = GOLDEN_LENGTHS[i];
      assertEquals("length=" + length, GOLDEN_VALUES[i], xxh3.hash(input, 0, length));
    }
  }

  /** The hash of a range depends on its bytes only, wherever they are and whatever is around. */
  public void testRanges() {
    final byte[] input = goldenInput(GOLDEN_LENGTHS[GOLDEN_LENGTHS.length - 1]);
    final Xxh3 xxh3 = new Xxh3(TermHash.SEED);
    for (int iter = 0; iter < 500; iter++) {
      final int index = random().nextInt(GOLDEN_LENGTHS.length);
      final int length = GOLDEN_LENGTHS[index];
      final int offset = random().nextInt(64);
      final byte[] bytes = new byte[offset + length + random().nextInt(64)];
      random().nextBytes(bytes);
      System.arraycopy(input, 0, bytes, offset, length);
      final byte[] copy = bytes.clone();
      assertEquals("length=" + length, GOLDEN_VALUES[index], xxh3.hash(bytes, offset, length));
      assertArrayEquals(copy, bytes); // not modified
    }
  }

  /** Random inputs: hashing is deterministic, and depends on the seed and on every byte. */
  public void testRandomInputs() {
    final long seed = random().nextLong();
    final Xxh3 xxh3 = new Xxh3(seed);
    final Xxh3 same = new Xxh3(seed);
    final Xxh3 other = new Xxh3(seed + 1);
    for (int iter = 0; iter < 1000; iter++) {
      final int length =
          random().nextBoolean() ? random().nextInt(300) : TestUtil.nextInt(random(), 0, 5000);
      final byte[] bytes = new byte[length];
      random().nextBytes(bytes);
      final long hash = xxh3.hash(bytes, 0, length);
      assertEquals(hash, xxh3.hash(bytes, 0, length));
      assertEquals(hash, same.hash(bytes, 0, length));
      // a 64-bit hash collides with another one with probability 2^-64
      assertNotEquals(hash, other.hash(bytes, 0, length));
      if (length > 0) {
        final int i = random().nextInt(length);
        bytes[i] ^= (byte) (1 << random().nextInt(8));
        assertNotEquals(
            "length=" + length + " flipped byte " + i, hash, xxh3.hash(bytes, 0, length));
      }
    }
  }
}
