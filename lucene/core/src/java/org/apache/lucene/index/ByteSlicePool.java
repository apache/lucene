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

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;

/**
 * Class that Posting and PostingVector use to write interleaved byte streams into shared fixed-size
 * byte[] arrays. The idea is to allocate slices of increasing lengths. For example, the first slice
 * is 5 bytes, the next slice is 14, etc. We start by writing our bytes into the first 5 bytes. When
 * we hit the end of the slice, we allocate the next slice and then write the address of the new
 * slice into the last 4 bytes of the previous slice (the "forwarding address").
 *
 * <p>Each slice is filled with 0's initially, and we mark the end with a non-zero byte. This way
 * the methods that are writing into the slice don't need to record its length and instead allocate
 * a new slice once they hit a non-zero byte.
 *
 * @lucene.internal
 */
final class ByteSlicePool {
  /**
   * The underlying structure consists of fixed-size blocks. We overlay variable-length slices on
   * top. Each slice is contiguous in memory, i.e. it does not straddle multiple blocks.
   */
  public final ByteBlockPool pool;

  /**
   * An array holding the level sizes for byte slices. The first slice is 5 bytes, the second is 14,
   * and so on.
   */
  public static final int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};

  /**
   * An array holding indexes for the {@link #LEVEL_SIZE_ARRAY}, to quickly navigate to the next
   * slice level. These are encoded on 4 bits in the slice, so the values in this array should be
   * less than 16.
   *
   * <p>{@code NEXT_LEVEL_ARRAY[x] == x + 1}, except for the last element, where {@code
   * NEXT_LEVEL_ARRAY[x] == x}, pointing at the maximum slice size.
   */
  public static final int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

  /** The first level size for new slices. */
  public static final int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  public ByteSlicePool(ByteBlockPool pool) {
    this.pool = pool;
  }

  /**
   * Allocates a new slice with the given size and level 0.
   *
   * @return the position where the slice starts
   */
  public int newSlice(final int size) {
    if (size > ByteBlockPool.BYTE_BLOCK_SIZE) {
      throw new IllegalArgumentException(
          "Slice size "
              + size
              + " should be less than the block size "
              + ByteBlockPool.BYTE_BLOCK_SIZE);
    }

    if (pool.byteUpto > ByteBlockPool.BYTE_BLOCK_SIZE - size) {
      pool.nextBuffer();
    }
    final int upto = pool.byteUpto;
    pool.byteUpto += size;
    pool.buffer[pool.byteUpto - 1] = 16; // This codifies level 0.
    return upto;
  }

  /**
   * Creates a new byte slice in continuation of the provided slice and return its offset into the
   * pool.
   *
   * @param slice the current slice
   * @param upto the offset into the current slice, which is expected to point to the last byte of
   *     the slice
   * @return the new slice's offset in the pool
   */
  public int allocSlice(final byte[] slice, final int upto) {
    return allocKnownSizeSlice(slice, upto) >> 8;
  }

  /**
   * Create a new byte slice in continuation of the provided slice and return its length and offset
   * into the pool.
   *
   * @param slice the current slice
   * @param upto the offset into the current slice, which is expected to point to the last byte of
   *     the slice
   * @return the new slice's length on the lower 8 bits and the offset into the pool on the other 24
   *     bits
   */
  public int allocKnownSizeSlice(final byte[] slice, final int upto) {
    final int level = slice[upto] & 15; // The last 4 bits codify the level.
    final int newLevel = NEXT_LEVEL_ARRAY[level];
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];

    // Maybe allocate another block
    if (pool.byteUpto > ByteBlockPool.BYTE_BLOCK_SIZE - newSize) {
      pool.nextBuffer();
    }

    final int newUpto = pool.byteUpto;
    final int offset = newUpto + pool.byteOffset;
    pool.byteUpto += newSize;

    // Copy forward the past 3 bytes (which we are about to overwrite with the forwarding address).
    // We actually copy 4 bytes at once since VarHandles make it cheap.
    int past3Bytes = ((int) BitUtil.VH_LE_INT.get(slice, upto - 3)) & 0xFFFFFF;
    // Ensure we're not changing the content of `buffer` by setting 4 bytes instead of 3. This
    // should never happen since the next `newSize` bytes must be equal to 0.
    assert pool.buffer[newUpto + 3] == 0;
    BitUtil.VH_LE_INT.set(pool.buffer, newUpto, past3Bytes);

    // Write forwarding address at end of last slice:
    BitUtil.VH_LE_INT.set(slice, upto - 3, offset);

    // Write new level:
    pool.buffer[pool.byteUpto - 1] = (byte) (16 | newLevel);

    return ((newUpto + 3) << 8) | (newSize - 3);
  }
}
