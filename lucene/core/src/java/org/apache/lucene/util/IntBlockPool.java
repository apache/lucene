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

import java.util.Arrays;

/**
 * A pool for int blocks similar to {@link ByteBlockPool}
 *
 * @lucene.internal
 */
public class IntBlockPool {
  public static final int INT_BLOCK_SHIFT = 13;
  public static final int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  public static final int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing int blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleIntBlocks(int[][] blocks, int start, int end);

    public int[] getIntBlock() {
      return new int[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    /** Creates a new {@link DirectAllocator} with a default block size */
    public DirectAllocator() {
      super(INT_BLOCK_SIZE);
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int start, int end) {}
  }

  /**
   * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this
   * outside of this class
   */
  public int[][] buffers = new int[10][];

  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1;

  /** Pointer to the current position in head buffer */
  public int intUpto = INT_BLOCK_SIZE;

  /** Current head buffer */
  public int[] buffer;

  /** Current head offset */
  public int intOffset = -INT_BLOCK_SIZE;

  private final Allocator allocator;

  /**
   * Creates a new {@link IntBlockPool} with a default {@link Allocator}.
   *
   * @see IntBlockPool#nextBuffer()
   */
  public IntBlockPool() {
    this(new DirectAllocator());
  }

  /**
   * Creates a new {@link IntBlockPool} with the given {@link Allocator}.
   *
   * @see IntBlockPool#nextBuffer()
   */
  public IntBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Expert: Resets the pool to its initial state, while optionally reusing the first buffer.
   * Buffers that are not reused are reclaimed by {@link
   * ByteBlockPool.Allocator#recycleByteBlocks(byte[][], int, int)}. Buffers can be filled with
   * zeros before recycling them. This is useful if a slice pool works on top of this int pool and
   * relies on the buffers being filled with zeros to find the non-zero end of slices.
   *
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling {@link
   *     IntBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before ie.
   *     {@link IntBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, intUpto, 0);
      }

      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleIntBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, bufferUpto + 1, null);
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        intUpto = 0;
        intOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        intUpto = INT_BLOCK_SIZE;
        intOffset = -INT_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   * Advances the pool to its next buffer. This method should be called once after the constructor
   * to initialize the pool. In contrast to the constructor a {@link IntBlockPool#reset(boolean,
   * boolean)} call will advance the pool to its first buffer immediately.
   */
  public void nextBuffer() {
    if (1 + bufferUpto == buffers.length) {
      int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers;
    }
    buffer = buffers[1 + bufferUpto] = allocator.getIntBlock();
    bufferUpto++;

    intUpto = 0;
    intOffset = Math.addExact(intOffset, INT_BLOCK_SIZE);
  }
}
