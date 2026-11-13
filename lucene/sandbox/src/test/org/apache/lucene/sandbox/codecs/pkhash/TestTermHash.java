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
   * Pins the partition, home slot, tag, control byte and mix of a hash: the on-disk layout depends
   * on them.
   */
  public void testSlotMapping() {
    // the home slot is monotonic in the bits below the partition bits
    assertEquals(0, TermHash.homeSlot(0L, 3, 1000));
    assertEquals(999, TermHash.homeSlot(-1L, 3, 1000));
    assertEquals(500, TermHash.homeSlot(1L << (Long.SIZE - 3 - 1), 3, 1000));
    assertEquals(7, TermHash.partition(-1L, 3));
    assertEquals(0, TermHash.partition(-1L, 0));
    assertEquals(Integer.MAX_VALUE - 1, TermHash.homeSlot(-1L, 0, Integer.MAX_VALUE));
    assertEquals(0x9234, TermHash.tag(0xABF49234L));
    assertEquals(0xF4, TermHash.control(0xABF49234L));
    assertEquals(0xb456bcfc34c2cb2cL, TermHash.mix(1L));
  }

  /**
   * The partition, the home slot, the control bits and the tag are in range and are disjoint bits
   * of the hash, so that keys with the same home slot are told apart by all the bits of the tag and
   * control byte.
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
      final int partition = TermHash.partition(hash, partitionBits);
      assertTrue(partition >= 0 && partition < (1 << partitionBits));
      final int slot = TermHash.homeSlot(hash, partitionBits, capacity);
      assertTrue(slot >= 0 && slot < capacity);
      final int tag = TermHash.tag(hash);
      assertTrue(tag >= 0 && tag < (1 << TermHash.TAG_BITS));
      final int control = TermHash.control(hash);
      assertTrue(control >= 0x80 && control <= 0xFF);
      // other tag and control bits: same partition and home slot
      final long otherLow = hash ^ (random().nextLong() & ((1L << lowBits) - 1));
      assertEquals(partition, TermHash.partition(otherLow, partitionBits));
      assertEquals(slot, TermHash.homeSlot(otherLow, partitionBits, capacity));
      // other partition and home bits: same tag and control byte
      final long otherHigh = hash ^ (random().nextLong() << lowBits);
      assertEquals(tag, TermHash.tag(otherHigh));
      assertEquals(control, TermHash.control(otherHigh));
      assertEquals(TermHash.tagAndControlBits(hash), TermHash.tagAndControlBits(otherHigh));
      // a slot word built from the hash gives the same tag and control byte
      final long word = (123L << TermHash.POINTER_SHIFT) | TermHash.tagAndControlBits(hash);
      assertEquals(tag, TermHash.tag(word));
      assertEquals(control, TermHash.control(word));
    }
  }
}
