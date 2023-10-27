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
package org.apache.lucene.index;

import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.ByteSlicePool;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestByteSliceReader extends LuceneTestCase {
  private static byte[] RANDOM_DATA;
  private static ByteBlockPool BLOCK_POOL;
  private static int BLOCK_POOL_END;

  @BeforeClass
  public static void beforeClass() {
    int len = atLeast(100);
    RANDOM_DATA = new byte[len];
    random().nextBytes(RANDOM_DATA);

    BLOCK_POOL = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    BLOCK_POOL.nextBuffer();
    ByteSlicePool slicePool = new ByteSlicePool(BLOCK_POOL);
    byte[] buffer = BLOCK_POOL.buffer;
    int upto = slicePool.newSlice(ByteSlicePool.FIRST_LEVEL_SIZE);
    for (byte randomByte : RANDOM_DATA) {
      if ((buffer[upto] & 16) != 0) {
        upto = slicePool.allocSlice(buffer, upto);
        buffer = BLOCK_POOL.buffer;
      }
      buffer[upto++] = randomByte;
    }
    BLOCK_POOL_END = upto;
  }

  @AfterClass
  public static void afterClass() {
    RANDOM_DATA = null;
    BLOCK_POOL = null;
  }

  public void testReadByte() {
    ByteSliceReader sliceReader = new ByteSliceReader();
    sliceReader.init(BLOCK_POOL, 0, BLOCK_POOL_END);
    for (byte expected : RANDOM_DATA) {
      assertEquals(expected, sliceReader.readByte());
    }
  }

  public void testSkipBytes() {
    Random random = random();
    ByteSliceReader sliceReader = new ByteSliceReader();

    int maxSkipTo = RANDOM_DATA.length - 1;
    int iterations = atLeast(random, 10);
    for (int i = 0; i < iterations; i++) {
      sliceReader.init(BLOCK_POOL, 0, BLOCK_POOL_END);
      // skip random chunks of bytes until exhausted
      for (int curr = 0; curr < maxSkipTo; ) {
        int skipTo = TestUtil.nextInt(random, curr, maxSkipTo);
        int step = skipTo - curr;
        sliceReader.skipBytes(step);
        assertEquals(RANDOM_DATA[skipTo], sliceReader.readByte());
        curr = skipTo + 1; // +1 for read byte
      }
    }
  }

  public void testRandomSlices() {
    ByteBlockPool blockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    ByteSlicePool slicePool = new ByteSlicePool(blockPool);

    int size;
    if (random().nextBoolean()) {
      // size < ByteBlockPool.BYTE_BLOCK_SIZE
      size = TestUtil.nextInt(random(), 100, 1000);
    } else {
      // size > ByteBlockPool.BYTE_BLOCK_SIZE
      size = TestUtil.nextInt(random(), 50000, 100000);
    }
    byte[] randomData = new byte[size];
    random().nextBytes(randomData);

    // Write randomData to slices

    int dataOffset = 0; // Offset into the data buffer
    int sliceLength = ByteSlicePool.FIRST_LEVEL_SIZE;
    int sliceOffset = slicePool.newSlice(sliceLength);
    final int firstSliceOffset = sliceOffset;   // We will need this later
    final byte[] firstSlice = blockPool.buffer; // We will need this later
    int writeLength = Math.min(size, sliceLength - 1);
    System.arraycopy(randomData, dataOffset, blockPool.buffer, sliceOffset, writeLength);
    dataOffset += writeLength;

    while (dataOffset < size) {
      int offsetAndLength = slicePool.allocKnownSizeSlice(blockPool.buffer, sliceOffset + sliceLength - 1);
      sliceLength = offsetAndLength & 0xff;
      sliceOffset = offsetAndLength >> 8;
      writeLength = Math.min(size - dataOffset, sliceLength - 1);
      System.arraycopy(randomData, dataOffset, blockPool.buffer, sliceOffset, writeLength);
      dataOffset += writeLength;
    }

    // Read the slices back into readData

    ByteSliceReader sliceReader = new ByteSliceReader();
    sliceReader.init(blockPool, 0, blockPool.byteOffset + blockPool.byteUpto);

    byte[] readData = new byte[size];
    sliceReader.readBytes(readData, 0, size);

    assertArrayEquals(randomData, readData);
  }



  static class SliceWriter {
    boolean hasStarted = false;

    ByteBlockPool blockPool;
    ByteSlicePool slicePool;

    int size;
    byte[] randomData;
    int dataOffset;

    int sliceLength;
    int sliceOffset;

    int firstSliceOffset;
    byte[] firstSlice;

    SliceWriter(ByteSlicePool slicePool ) {
      this.slicePool = slicePool;
      this.blockPool = slicePool.pool;

      if (random().nextBoolean()) {
        // size < ByteBlockPool.BYTE_BLOCK_SIZE
        size = TestUtil.nextInt(random(), 100, 1000);
      } else {
        // size > ByteBlockPool.BYTE_BLOCK_SIZE
        size = TestUtil.nextInt(random(), 50000, 100000);
      }
      randomData = new byte[size];
      random().nextBytes(randomData);
    }

    boolean writeSlice() {
      // The first slice is special
      if (hasStarted == false) {
        dataOffset = 0; // Offset into the data buffer
        sliceLength = ByteSlicePool.FIRST_LEVEL_SIZE;
        sliceOffset = slicePool.newSlice(sliceLength);
        firstSliceOffset = sliceOffset; // We will need this later
        firstSlice = blockPool.buffer;  // We will need this later
        int writeLength = Math.min(size, sliceLength - 1);
        System.arraycopy(randomData, dataOffset, blockPool.buffer, sliceOffset, writeLength);
        dataOffset += writeLength;

        hasStarted = true;
        return true;
      }
      // Have we written everything?
      if (dataOffset == size) {
        return false;
      }
      // No, write more
      System.out.println(sliceOffset + sliceLength - 1);
      int offsetAndLength = slicePool.allocKnownSizeSlice(blockPool.buffer, sliceOffset + sliceLength - 1);
      sliceLength = offsetAndLength & 0xff;
      sliceOffset = offsetAndLength >> 8;
      int writeLength = Math.min(size - dataOffset, sliceLength - 1);
      System.arraycopy(randomData, dataOffset, blockPool.buffer, sliceOffset, writeLength);
      dataOffset += writeLength;
      return true;
    }
  }

  class SliceReader {
    ByteSliceReader sliceReader;
    int size;

    byte[] readData;

    SliceReader(ByteBlockPool blockPool, int firstSliceOffset, int size) {
      this.size = size;
      sliceReader = new ByteSliceReader();
      sliceReader.init(blockPool, firstSliceOffset, blockPool.byteOffset + blockPool.byteUpto);
      readData = new byte[size];
    }

    void read() {
      sliceReader.readBytes(readData, 0, size);
    }
  }

  public void testRandomInterleavedSlices() {
    ByteBlockPool blockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    ByteSlicePool slicePool = new ByteSlicePool(blockPool);

    int n = random().nextInt(2, 4); // 2 or 3 writers and readers
    SliceWriter[] sliceWriters = new SliceWriter[n];

    for (int i = 0; i < n; i++) {
      sliceWriters[i] = new SliceWriter(slicePool);
    }

    while (true) {
      int i = random().nextInt(n);
      boolean succeeded = sliceWriters[i].writeSlice();
      if (succeeded == false) {
        for (int j = 0; j < n; j++) {
          while (sliceWriters[j].writeSlice());
        }
        break;
      }
    }

    for (int i = 0; i < n; i++) {
      SliceWriter sliceWriter = sliceWriters[i];
      ByteSliceReader sliceReader = new ByteSliceReader();
      sliceReader.init(sliceWriter.blockPool, sliceWriter.firstSliceOffset,
              sliceWriter.blockPool.byteOffset + sliceWriter.blockPool.byteUpto);

      byte[] readData = new byte[sliceWriter.size];
      sliceReader.readBytes(readData, 0, sliceWriter.size);

      assertArrayEquals(sliceWriter.randomData, readData);
    }
  }
}
