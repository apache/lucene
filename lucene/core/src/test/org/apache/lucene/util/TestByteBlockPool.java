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
package org.apache.lucene.util;

import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestByteBlockPool extends LuceneTestCase {

  public void testAppendFromOtherPool() {
    Random random = random();

    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    final int numBytes = atLeast(2 << 16);
    byte[] bytes = RandomBytes.randomBytesOfLength(random, numBytes);
    pool.append(bytes);

    ByteBlockPool anotherPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    byte[] existingBytes = new byte[atLeast(500)];
    anotherPool.append(existingBytes);

    // now slice and append to another pool
    int offset = TestUtil.nextInt(random, 1, 2 << 15);
    int length = bytes.length - offset;
    if (random.nextBoolean()) {
      length = TestUtil.nextInt(random, 1, length);
    }
    anotherPool.append(pool, offset, length);

    assertEquals(existingBytes.length + length, anotherPool.getPosition());

    byte[] results = new byte[length];
    anotherPool.readBytes(existingBytes.length, results, 0, results.length);
    for (int i = 0; i < length; i++) {
      assertEquals("byte @ index=" + i, bytes[offset + i], results[i]);
    }
  }

  public void testReadAndWrite() {
    Counter bytesUsed = Counter.newCounter();
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    pool.nextBuffer();
    boolean reuseFirst = random().nextBoolean();
    for (int j = 0; j < 2; j++) {

      List<BytesRef> list = new ArrayList<>();
      int maxLength = atLeast(500);
      final int numValues = atLeast(100);
      BytesRefBuilder ref = new BytesRefBuilder();
      for (int i = 0; i < numValues; i++) {
        final String value = TestUtil.randomRealisticUnicodeString(random(), maxLength);
        list.add(new BytesRef(value));
        ref.copyChars(value);
        pool.append(ref.get());
      }
      // verify
      long position = 0;
      BytesRefBuilder builder = new BytesRefBuilder();
      for (BytesRef expected : list) {
        ref.grow(expected.length);
        ref.setLength(expected.length);
        switch (random().nextInt(2)) {
          case 0:
            // copy bytes
            pool.readBytes(position, ref.bytes(), 0, ref.length());
            break;
          case 1:
            BytesRef scratch = new BytesRef();
            pool.setBytesRef(builder, scratch, position, ref.length());
            System.arraycopy(scratch.bytes, scratch.offset, ref.bytes(), 0, ref.length());
            break;
          default:
            fail();
        }
        assertEquals(expected, ref.get());
        position += ref.length();
      }
      pool.reset(random().nextBoolean(), reuseFirst);
      if (reuseFirst) {
        assertEquals(ByteBlockPool.BYTE_BLOCK_SIZE, bytesUsed.get());
      } else {
        assertEquals(0, bytesUsed.get());
        pool.nextBuffer(); // prepare for next iter
      }
    }
  }

  public void testLargeRandomBlocks() {
    Counter bytesUsed = Counter.newCounter();
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    pool.nextBuffer();

    long totalBytes = 0;
    List<byte[]> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int size;
      if (random().nextBoolean()) {
        size = TestUtil.nextInt(random(), 100, 1000);
      } else {
        size = TestUtil.nextInt(random(), 50000, 100000);
      }
      byte[] bytes = new byte[size];
      random().nextBytes(bytes);
      items.add(bytes);
      pool.append(new BytesRef(bytes));
      totalBytes += size;

      // make sure we report the correct position
      assertEquals(totalBytes, pool.getPosition());
    }

    long position = 0;
    for (byte[] expected : items) {
      byte[] actual = new byte[expected.length];
      pool.readBytes(position, actual, 0, actual.length);
      assertTrue(Arrays.equals(expected, actual));
      position += expected.length;
    }
  }

  public void testTooManyAllocs() {
    // Use a mock allocator that doesn't waste memory
    ByteBlockPool pool =
        new ByteBlockPool(
            new ByteBlockPool.Allocator(0) {
              final byte[] buffer = new byte[0];

              @Override
              public void recycleByteBlocks(byte[][] blocks, int start, int end) {}

              @Override
              public byte[] getByteBlock() {
                return buffer;
              }
            });
    pool.nextBuffer();

    boolean throwsException = false;
    for (int i = 0; i < Integer.MAX_VALUE / ByteBlockPool.BYTE_BLOCK_SIZE + 1; i++) {
      try {
        pool.nextBuffer();
      } catch (
          @SuppressWarnings("unused")
          ArithmeticException ignored) {
        // The offset overflows on the last attempt to call nextBuffer()
        throwsException = true;
        break;
      }
    }
    assertTrue(throwsException);
    assertTrue(pool.byteOffset + ByteBlockPool.BYTE_BLOCK_SIZE < pool.byteOffset);
  }
}
