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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestByteSlicePool extends LuceneTestCase {
  public void testAllocKnownSizeSlice() {
    Counter bytesUsed = Counter.newCounter();
    ByteBlockPool blockPool =
        new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    blockPool.nextBuffer();
    ByteSlicePool slicePool = new ByteSlicePool(blockPool);
    for (int i = 0; i < 100; i++) {
      int size;
      if (random().nextBoolean()) {
        size = TestUtil.nextInt(random(), 100, 1000);
      } else {
        size = TestUtil.nextInt(random(), 50000, 100000);
      }
      byte[] randomData = new byte[size];
      random().nextBytes(randomData);

      int upto = slicePool.newSlice(ByteSlicePool.FIRST_LEVEL_SIZE);

      for (int offset = 0; offset < size; ) {
        if ((blockPool.buffer[upto] & 16) == 0) {
          blockPool.buffer[upto++] = randomData[offset++];
        } else {
          int offsetAndLength = slicePool.allocKnownSizeSlice(blockPool.buffer, upto);
          int sliceLength = offsetAndLength & 0xff;
          upto = offsetAndLength >> 8;
          assertNotEquals(0, blockPool.buffer[upto + sliceLength - 1]);
          assertEquals(0, blockPool.buffer[upto]);
          int writeLength = Math.min(sliceLength - 1, size - offset);
          System.arraycopy(randomData, offset, blockPool.buffer, upto, writeLength);
          offset += writeLength;
          upto += writeLength;
        }
      }
    }
  }

  public void testAllocLargeSlice() {
    ByteBlockPool blockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    ByteSlicePool slicePool = new ByteSlicePool(blockPool);

    assertEquals(0, slicePool.newSlice(ByteBlockPool.BYTE_BLOCK_SIZE));
    assertArrayEquals(blockPool.buffer, blockPool.getBuffer(0));

    blockPool.nextBuffer();
    assertThrows(
        IllegalArgumentException.class,
        () -> slicePool.newSlice(ByteBlockPool.BYTE_BLOCK_SIZE + 1));
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

    dataOffset = 0;
    byte[] readData = new byte[size];
    int sliceSizeIdx = 0; // Index into LEVEL_SIZE_ARRAY, allowing us to find the size of the current slice

    byte[] slice = firstSlice;
    sliceOffset = firstSliceOffset;
    sliceLength = ByteSlicePool.LEVEL_SIZE_ARRAY[sliceSizeIdx] - 4; // 4 bytes are for the offset to the next slice
    int readLength;
    if (dataOffset + sliceLength + 3 >= size) {
      // We are reading the last slice, there is no more offset, just a byte for the level
      readLength = size - dataOffset;
    } else {
      readLength = sliceLength;
    }
    System.arraycopy(slice, sliceOffset, readData, dataOffset, readLength);
    dataOffset += readLength;
    sliceSizeIdx = Math.min(sliceSizeIdx + 1, ByteSlicePool.LEVEL_SIZE_ARRAY.length - 1);

    while (dataOffset < size) {
      int globalSliceOffset = (int) BitUtil.VH_LE_INT.get(slice, sliceOffset + sliceLength);
      slice = blockPool.getBuffer(globalSliceOffset / ByteBlockPool.BYTE_BLOCK_SIZE);
      sliceOffset = globalSliceOffset % ByteBlockPool.BYTE_BLOCK_SIZE;
      sliceLength = ByteSlicePool.LEVEL_SIZE_ARRAY[sliceSizeIdx] - 4;
      if (dataOffset + sliceLength + 3 >= size) {
        // We are reading the last slice, there is no more offset, just a byte for the level
        readLength = size - dataOffset;
      } else {
        readLength = sliceLength;
      }
      System.arraycopy(slice, sliceOffset, readData, dataOffset, readLength);
      dataOffset += readLength;
      sliceSizeIdx = Math.min(sliceSizeIdx + 1, ByteSlicePool.LEVEL_SIZE_ARRAY.length - 1);
    }

    assertArrayEquals(randomData, readData);
  }
}
