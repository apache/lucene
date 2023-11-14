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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RecyclingByteBlockAllocator;

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

  /** Create a random byte array and write it to a {@link ByteSlicePool} one slice at a time. */
  static class SliceWriter {
    boolean hasStarted = false;

    ByteBlockPool blockPool;
    ByteSlicePool slicePool;

    int size;
    byte[] randomData;
    int dataOffset;

    byte[] slice;
    int sliceLength;
    int sliceOffset;

    int firstSliceOffset;
    byte[] firstSlice;

    SliceWriter(ByteSlicePool slicePool) {
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

    /**
     * Write the next slice of data.
     *
     * @return true if we wrote a slice and false if we're out of data to write
     */
    boolean writeSlice() {
      // The first slice is special
      if (hasStarted == false) {
        dataOffset = 0; // Offset into the data buffer
        sliceLength = ByteSlicePool.FIRST_LEVEL_SIZE;
        sliceOffset = slicePool.newSlice(sliceLength);
        firstSliceOffset = sliceOffset; // We will need this later
        firstSlice = blockPool.buffer; // We will need this later
        slice = firstSlice;
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
      int offsetAndLength = slicePool.allocKnownSizeSlice(slice, sliceOffset + sliceLength - 1);
      slice = blockPool.buffer;
      sliceLength = offsetAndLength & 0xff;
      sliceOffset = offsetAndLength >> 8;
      int writeLength = Math.min(size - dataOffset, sliceLength - 1);
      System.arraycopy(randomData, dataOffset, slice, sliceOffset, writeLength);
      dataOffset += writeLength;
      return true;
    }
  }

  /** Read a sequence of slices into a byte array. */
  static class SliceReader {
    boolean hasStarted = false;

    ByteBlockPool blockPool;
    ByteSlicePool slicePool;

    int size;
    byte[] readData;
    int dataOffset;

    int sliceLength;
    int sliceOffset;

    byte[] slice;
    int sliceSizeIdx;

    SliceReader(ByteSlicePool slicePool, int size, int firstSliceOffset, byte[] firstSlice) {
      this.slicePool = slicePool;
      this.blockPool = slicePool.pool;
      this.size = size;
      this.sliceOffset = firstSliceOffset;
      this.slice = firstSlice;
      readData = new byte[size];
    }

    /**
     * Read the next slice of data.
     *
     * @return true if we read a slice and false if we'd already read the entire sequence of slices
     */
    boolean readSlice() {
      // The first slice is special
      if (hasStarted == false) {
        dataOffset = 0;
        // Index into LEVEL_SIZE_ARRAY, allowing us to find the size of the current slice
        sliceSizeIdx = 0;
        // 4 bytes are for the offset to the next slice, we can't use them for data
        sliceLength = ByteSlicePool.LEVEL_SIZE_ARRAY[sliceSizeIdx] - 4;
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

        hasStarted = true;
        return true;
      }
      // Have we read everything?
      if (dataOffset == size) {
        return false;
      }
      // No, read more
      int globalSliceOffset = (int) BitUtil.VH_LE_INT.get(slice, sliceOffset + sliceLength);
      slice = blockPool.getBuffer(globalSliceOffset / ByteBlockPool.BYTE_BLOCK_SIZE);
      sliceOffset = globalSliceOffset % ByteBlockPool.BYTE_BLOCK_SIZE;
      sliceLength = ByteSlicePool.LEVEL_SIZE_ARRAY[sliceSizeIdx] - 4;
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
      return true;
    }
  }

  /**
   * Run multiple slice writers, creating interleaved slices. Read the slices afterwards and check
   * that we read back the same data we wrote.
   */
  public void testRandomInterleavedSlices() {
    ByteBlockPool blockPool = new ByteBlockPool(new RecyclingByteBlockAllocator());
    ByteSlicePool slicePool = new ByteSlicePool(blockPool);

    int nIterations =
        TestUtil.nextInt(random(), 1, 3); // 1-3 iterations with buffer resets in between
    for (int iter = 0; iter < nIterations; iter++) {
      int n = TestUtil.nextInt(random(), 2, 3); // 2 or 3 writers and readers
      SliceWriter[] sliceWriters = new SliceWriter[n];
      SliceReader[] sliceReaders = new SliceReader[n];

      // Init slice writers
      for (int i = 0; i < n; i++) {
        sliceWriters[i] = new SliceWriter(slicePool);
      }

      // Write slices
      while (true) {
        int i = random().nextInt(n);
        boolean succeeded = sliceWriters[i].writeSlice();
        if (succeeded == false) {
          for (int j = 0; j < n; j++) {
            while (sliceWriters[j].writeSlice()) {}
          }
          break;
        }
      }

      // Init slice readers
      for (int i = 0; i < n; i++) {
        sliceReaders[i] =
            new SliceReader(
                slicePool,
                sliceWriters[i].size,
                sliceWriters[i].firstSliceOffset,
                sliceWriters[i].firstSlice);
      }

      // Read slices
      while (true) {
        int i = random().nextInt(n);
        boolean succeeded = sliceReaders[i].readSlice();
        if (succeeded == false) {
          for (int j = 0; j < n; j++) {
            while (sliceReaders[j].readSlice()) {}
          }
          break;
        }
      }

      // Compare written data with read data
      for (int i = 0; i < n; i++) {
        assertArrayEquals(sliceWriters[i].randomData, sliceReaders[i].readData);
      }

      // We don't rely on the buffers being filled with zeros because the SliceWriter keeps the
      // slice length as state, but ByteSlicePool.allocKnownSizeSlice asserts on zeros in the
      // buffer.
      blockPool.reset(true, random().nextBoolean());
    }
  }
}
