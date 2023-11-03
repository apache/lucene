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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a logical byte[] as a series of blocks. You can write into it by using append and read
 * using the offset position (random access). The buffers can be reset to reuse the allocated
 * buffers.
 *
 * @lucene.internal
 */
public final class ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  public static final int BYTE_BLOCK_SHIFT = 15;
  public static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  public static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte blocks. */
  public abstract static class Allocator {
    // TODO: ByteBlockPool assume the blockSize is always {@link BYTE_BLOCK_SIZE}, but this class
    // allow arbitrary value of blockSize. We should make them consistent.
    protected final int blockSize;

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    public DirectAllocator() {
      super(BYTE_BLOCK_SIZE);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {}
  }

  /** A simple {@link Allocator} that never recycles, but tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;

    public DirectTrackingAllocator(Counter bytesUsed) {
      super(BYTE_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end - start) * blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  }
  ;

  /**
   * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this
   * outside of this class.
   */
  public byte[][] buffers = new byte[10][];

  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1; // Which buffer we are upto

  /** Where we are in head buffer */
  public int byteUpto = BYTE_BLOCK_SIZE;

  /** Current head buffer */
  public byte[] buffer;

  /** Current head offset */
  public int byteOffset = -BYTE_BLOCK_SIZE;

  private final Allocator allocator;

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Resets the pool to its initial state reusing the first buffer and fills all buffers with <code>
   * 0</code> bytes before they reused or passed to {@link Allocator#recycleByteBlocks(byte[][],
   * int, int)}. Calling {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }

  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling {@link
   * ByteBlockPool#nextBuffer()} is not needed after reset.
   *
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>. This
   *     should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling {@link
   *     ByteBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before
   *     ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }

      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, 1 + bufferUpto, null);
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        byteUpto = 0;
        byteOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        byteUpto = BYTE_BLOCK_SIZE;
        byteOffset = -BYTE_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   * Advances the pool to its next buffer. This method should be called once after the constructor
   * to initialize the pool. In contrast to the constructor a {@link ByteBlockPool#reset()} call
   * will advance the pool to its first buffer immediately.
   */
  public void nextBuffer() {
    if (1 + bufferUpto == buffers.length) {
      byte[][] newBuffers =
          new byte[ArrayUtil.oversize(buffers.length + 1, NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers;
    }
    buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();
    bufferUpto++;

    byteUpto = 0;
    byteOffset = Math.addExact(byteOffset, BYTE_BLOCK_SIZE);
  }

  /**
   * Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice. This
   * will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses the
   * provided {@link BytesRefBuilder} to copy bytes over.
   */
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly
      // without copying
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  /** Appends the bytes in the provided {@link BytesRef} at the current position. */
  public void append(final BytesRef bytes) {
    append(bytes.bytes, bytes.offset, bytes.length);
  }

  /**
   * Append the provided byte array at the current position.
   *
   * @param bytes the byte array to write
   */
  public void append(final byte[] bytes) {
    append(bytes, 0, bytes.length);
  }

  /**
   * Append some portion of the provided byte array at the current position.
   *
   * @param bytes the byte array to write
   * @param offset the offset of the byte array
   * @param length the number of bytes to write
   */
  public void append(final byte[] bytes, int offset, int length) {
    int bytesLeft = length;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) {
        // fits within current buffer
        System.arraycopy(bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else {
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }

  /**
   * Reads bytes out of the pool starting at the given offset with the given length into the given
   * byte array at offset <code>off</code>.
   *
   * <p>Note: this method allows to copy across block boundaries.
   */
  public void readBytes(final long offset, final byte[] bytes, int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    while (bytesLeft > 0) {
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Read a single byte at the given offset
   *
   * @param offset the offset to read
   * @return the byte
   */
  public byte readByte(final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    return buffers[bufferIndex][pos];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }

  /** the current position (in absolute value) of this byte pool */
  public long getPosition() {
    return bufferUpto * allocator.blockSize + byteUpto;
  }
}
