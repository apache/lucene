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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestTermHash extends LuceneTestCase {

  /** The persisted hash must be XXH3-64 with the fixed seed. */
  public void testIsXxh3WithSeed() {
    Xxh3 xxh3 = new Xxh3(TermHash.SEED);
    for (int iter = 0; iter < 1000; iter++) {
      int length = random().nextInt(300);
      int offset = random().nextInt(8);
      byte[] bytes = new byte[offset + length + random().nextInt(8)];
      random().nextBytes(bytes);
      assertEquals(xxh3.hash(bytes, offset, length), TermHash.hash(bytes, offset, length));
    }
  }

  /**
   * Hard-coded values, generated with python's xxhash ({@code xxhash.xxh3_64_intdigest(key,
   * seed=0x9E3779B9)}), so that changing the function or its seed is noticed: the slot layout of
   * written segments depends on them. Short keys with bytes of 0x80 or more, such as single-byte
   * ids or sortable numeric bytes, must not have them sign-extended.
   */
  public void testGoldenValues() {
    assertHash("", 0x64ce21f217f362b1L);
    assertHash("a", 0xed836f3f49179506L);
    assertHash("abc", 0x18ce44bde5a6523dL);
    assertHash("abcd", 0x0047b9aeaa4d288dL);
    assertHash("abcdefgh", 0xf5cb45eda28cac39L);
    assertHash("abcdefghi", 0x113c68ea97e3145cL);
    assertHash("0123456789abcdef", 0x71710276a161af25L);
    assertHash("0123456789abcdefg", 0x6640cf2a2359493bL);
    assertHash("123e4567-e89b-12d3-a456-426614174000", 0x8bc7e4829507fab1L);
    assertHash("The quick brown fox jumps over the lazy dog", 0xa6bd6a770460c100L);
    assertHash(new byte[] {(byte) 0x80}, 0xdadd40303f00a84cL);
    assertHash(new byte[] {(byte) 0xFF}, 0x470b83587350a195L);
    assertHash(new byte[] {0x00, (byte) 0xFF}, 0xbb36b513a3622bbcL);
    assertHash(new byte[] {(byte) 0xFF, 0x00, (byte) 0x80}, 0xe8a07ae0feacbdf5L);
    assertHash(new byte[] {(byte) 0x80, 0x00, 0x00, 0x01}, 0xb0184f5848dba758L);
  }

  private static void assertHash(String input, long expected) {
    assertHash(input.getBytes(StandardCharsets.UTF_8), expected);
  }

  private static void assertHash(byte[] input, long expected) {
    final byte[] bytes = new byte[input.length + 2];
    System.arraycopy(input, 0, bytes, 1, input.length);
    assertEquals(Arrays.toString(input), expected, TermHash.hash(bytes, 1, input.length));
  }

  public void testSlotMapping() {
    for (int iter = 0; iter < 10_000; iter++) {
      long hash = random().nextLong();
      int partitionBits = random().nextInt(PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS + 1);
      int capacity = TestUtil.nextInt(random(), 2, Integer.MAX_VALUE);
      int partition = TermHash.partition(hash, partitionBits);
      assertTrue(partition >= 0 && partition < (1 << partitionBits));
      int slot = TermHash.homeSlot(hash, partitionBits, capacity);
      assertTrue(slot >= 0 && slot < capacity);
      int tag = TermHash.tag(hash);
      assertTrue(tag >= 0 && tag < (1 << TermHash.TAG_BITS));
      int control = TermHash.control(hash);
      assertTrue(control >= 0x80 && control <= 0xFF);
    }
    // the home slot is monotonic in the bits below the partition bits
    assertEquals(0, TermHash.homeSlot(0L, 3, 1000));
    assertEquals(999, TermHash.homeSlot(-1L, 3, 1000));
    assertEquals(500, TermHash.homeSlot(1L << (Long.SIZE - 3 - 1), 3, 1000));
    assertEquals(7, TermHash.partition(-1L, 3));
    assertEquals(0, TermHash.partition(-1L, 0));
    assertEquals(Integer.MAX_VALUE - 1, TermHash.homeSlot(-1L, 0, Integer.MAX_VALUE));
  }

  /**
   * The partition, the home slot, the control bits and the tag are disjoint bits of the hash, so
   * that keys with the same home slot are told apart by all the bits of the tag and control byte.
   */
  public void testDisjointBits() {
    final int maxPartitionBits = PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS;
    final int lowBits = TermHash.TAG_BITS + TermHash.CONTROL_BITS;
    assertTrue(Long.SIZE - maxPartitionBits - TermHash.HOME_BITS >= lowBits);
    assertTrue(TermHash.POINTER_SHIFT >= lowBits);
    assertEquals((0x7FL << TermHash.TAG_BITS) | TermHash.TAG_MASK, TermHash.tagAndControlBits(-1L));
    for (int iter = 0; iter < 10_000; iter++) {
      final long hash = random().nextLong();
      final int partitionBits = random().nextInt(maxPartitionBits + 1);
      final int capacity = TestUtil.nextInt(random(), 2, Integer.MAX_VALUE);
      // other tag and control bits: same partition and home slot
      final long otherLow = hash ^ (random().nextLong() & ((1L << lowBits) - 1));
      assertEquals(
          TermHash.partition(hash, partitionBits), TermHash.partition(otherLow, partitionBits));
      assertEquals(
          TermHash.homeSlot(hash, partitionBits, capacity),
          TermHash.homeSlot(otherLow, partitionBits, capacity));
      // other partition and home bits: same tag and control byte
      final long otherHigh = hash ^ (random().nextLong() << lowBits);
      assertEquals(TermHash.tag(hash), TermHash.tag(otherHigh));
      assertEquals(TermHash.control(hash), TermHash.control(otherHigh));
      assertEquals(TermHash.tagAndControlBits(hash), TermHash.tagAndControlBits(otherHigh));
      // a slot word built from the hash gives the same tag and control byte
      final long word = (123L << TermHash.POINTER_SHIFT) | TermHash.tagAndControlBits(hash);
      assertEquals(TermHash.tag(hash), TermHash.tag(word));
      assertEquals(TermHash.control(hash), TermHash.control(word));
    }
  }
}
