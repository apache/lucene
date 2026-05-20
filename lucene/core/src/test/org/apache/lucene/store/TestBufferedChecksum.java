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
package org.apache.lucene.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitUtil;

public class TestBufferedChecksum extends LuceneTestCase {

  public void testSimple() {
    Checksum c = new BufferedChecksum(new CRC32());
    c.update(1);
    c.update(2);
    c.update(3);
    assertEquals(1438416925L, c.getValue());
  }

  public void testRandom() {
    Checksum c1 = new CRC32();
    Checksum c2 = new BufferedChecksum(new CRC32());
    int iterations = atLeast(10000);
    for (int i = 0; i < iterations; i++) {
      switch (random().nextInt(4)) {
        case 0:
          // update(byte[], int, int)
          int length = random().nextInt(1024);
          byte[] bytes = new byte[length];
          random().nextBytes(bytes);
          c1.update(bytes, 0, bytes.length);
          c2.update(bytes, 0, bytes.length);
          break;
        case 1:
          // update(int)
          int b = random().nextInt(256);
          c1.update(b);
          c2.update(b);
          break;
        case 2:
          // reset()
          c1.reset();
          c2.reset();
          break;
        case 3:
          // getValue()
          assertEquals(c1.getValue(), c2.getValue());
          break;
      }
    }
    assertEquals(c1.getValue(), c2.getValue());
  }

  public void testDifferentInputTypes() {
    Checksum crc = new CRC32();
    BufferedChecksum buffered = new BufferedChecksum(new CRC32());
    int iterations = atLeast(1000);
    for (int i = 0; i < iterations; i++) {
      byte[] input = new byte[4096];
      random().nextBytes(input);
      crc.update(input);
      final long checksum = crc.getValue();
      crc.reset();
      updateByShorts(checksum, buffered, input);
      updateByInts(checksum, buffered, input);
      updateByLongs(checksum, buffered, input);
      updateByChunkOfBytes(checksum, buffered, input);
      updateByChunkOfLongs(checksum, buffered, input);
    }
  }

  private void updateByChunkOfBytes(long expected, BufferedChecksum checksum, byte[] input) {
    for (int i = 0; i < input.length; i++) {
      checksum.update(input[i]);
    }
    checkChecksumValueAndReset(expected, checksum);

    checksum.update(input);
    checkChecksumValueAndReset(expected, checksum);

    int iterations = atLeast(10);
    for (int ite = 0; ite < iterations; ite++) {
      int len0 = random().nextInt(input.length / 2);
      checksum.update(input, 0, len0);
      checksum.update(input, len0, input.length - len0);
      checkChecksumValueAndReset(expected, checksum);

      checksum.update(input, 0, len0);
      int len1 = random().nextInt(input.length / 4);
      for (int i = 0; i < len1; i++) {
        checksum.update(input[len0 + i]);
      }
      checksum.update(input, len0 + len1, input.length - len1 - len0);
      checkChecksumValueAndReset(expected, checksum);
    }
  }

  private void updateByShorts(long expected, BufferedChecksum checksum, byte[] input) {
    int ix = shiftArray(checksum, input);
    while (ix <= input.length - Short.BYTES) {
      checksum.updateShort((short) BitUtil.VH_LE_SHORT.get(input, ix));
      ix += Short.BYTES;
    }
    checksum.update(input, ix, input.length - ix);
    checkChecksumValueAndReset(expected, checksum);
  }

  private void updateByInts(long expected, BufferedChecksum checksum, byte[] input) {
    int ix = shiftArray(checksum, input);
    while (ix <= input.length - Integer.BYTES) {
      checksum.updateInt((int) BitUtil.VH_LE_INT.get(input, ix));
      ix += Integer.BYTES;
    }
    checksum.update(input, ix, input.length - ix);
    checkChecksumValueAndReset(expected, checksum);
  }

  private void updateByLongs(long expected, BufferedChecksum checksum, byte[] input) {
    int ix = shiftArray(checksum, input);
    while (ix <= input.length - Long.BYTES) {
      checksum.updateLong((long) BitUtil.VH_LE_LONG.get(input, ix));
      ix += Long.BYTES;
    }
    checksum.update(input, ix, input.length - ix);
    checkChecksumValueAndReset(expected, checksum);
  }

  private static int shiftArray(BufferedChecksum checksum, byte[] input) {
    int ix = random().nextInt(input.length / 4);
    checksum.update(input, 0, ix);
    return ix;
  }

  private void updateByChunkOfLongs(long expected, BufferedChecksum checksum, byte[] input) {
    int ix = random().nextInt(input.length / 4);
    int remaining = Long.BYTES - ix & 7;
    LongBuffer b =
        ByteBuffer.wrap(input).position(ix).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    long[] longInput = new long[(input.length - ix) / Long.BYTES];
    b.get(longInput);

    checksum.update(input, 0, ix);
    for (int i = 0; i < longInput.length; i++) {
      checksum.updateLong(longInput[i]);
    }
    checksum.update(input, input.length - remaining, remaining);
    checkChecksumValueAndReset(expected, checksum);

    checksum.update(input, 0, ix);
    checksum.updateLongs(longInput, 0, longInput.length);
    checksum.update(input, input.length - remaining, remaining);
    checkChecksumValueAndReset(expected, checksum);

    int iterations = atLeast(10);
    for (int ite = 0; ite < iterations; ite++) {
      int len0 = random().nextInt(longInput.length / 2);
      checksum.update(input, 0, ix);
      checksum.updateLongs(longInput, 0, len0);
      checksum.updateLongs(longInput, len0, longInput.length - len0);
      checksum.update(input, input.length - remaining, remaining);
      checkChecksumValueAndReset(expected, checksum);

      checksum.update(input, 0, ix);
      checksum.updateLongs(longInput, 0, len0);
      int len1 = random().nextInt(longInput.length / 4);
      for (int i = 0; i < len1; i++) {
        checksum.updateLong(longInput[len0 + i]);
      }
      checksum.updateLongs(longInput, len0 + len1, longInput.length - len1 - len0);
      checksum.update(input, input.length - remaining, remaining);
      checkChecksumValueAndReset(expected, checksum);

      checksum.update(input, 0, ix);
      checksum.updateLongs(longInput, 0, len0);
      checksum.update(input, ix + len0 * Long.BYTES, input.length - len0 * Long.BYTES - ix);
      checkChecksumValueAndReset(expected, checksum);
    }
  }

  private void checkChecksumValueAndReset(long expected, Checksum checksum) {
    assertEquals(expected, checksum.getValue());
    checksum.reset();
  }
}
