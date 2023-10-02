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

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link DataInput} implementing {@link RandomAccessInput} and reading data from a list of {@link
 * ByteBuffer}s.
 */
public final class ByteBuffersDataInput extends DataInput
    implements Accountable, RandomAccessInput {
  private final ByteBuffer[] blocks;
  private final FloatBuffer[] floatBuffers;
  private final LongBuffer[] longBuffers;
  private final int blockBits;
  private final int blockMask;
  private final long length;
  private final long offset;

  private long pos;

  /**
   * Read data from a set of contiguous buffers. All data buffers except for the last one must have
   * an identical remaining number of bytes in the buffer (that is a power of two). The last buffer
   * can be of an arbitrary remaining length.
   */
  public ByteBuffersDataInput(List<ByteBuffer> buffers) {
    ensureAssumptions(buffers);

    this.blocks = buffers.toArray(ByteBuffer[]::new);
    for (int i = 0; i < blocks.length; ++i) {
      blocks[i] = blocks[i].asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }
    // pre-allocate these arrays and create the view buffers lazily
    this.floatBuffers = new FloatBuffer[blocks.length * Float.BYTES];
    this.longBuffers = new LongBuffer[blocks.length * Long.BYTES];
    if (blocks.length == 1) {
      this.blockBits = 32;
      this.blockMask = ~0;
    } else {
      final int blockBytes = determineBlockPage(buffers);
      this.blockBits = Integer.numberOfTrailingZeros(blockBytes);
      this.blockMask = (1 << blockBits) - 1;
    }

    long length = 0;
    for (ByteBuffer block : blocks) {
      length += block.remaining();
    }
    this.length = length;

    // The initial "position" of this stream is shifted by the position of the first block.
    this.offset = blocks[0].position();
    this.pos = offset;
  }

  /**
   * Returns the total number of bytes in this stream.
   *
   * @deprecated Use {@link #length()} instead.
   */
  @Deprecated
  public long size() {
    return length();
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for what the type of buffer is (direct vs. heap-based).
    return (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.length
        + Arrays.stream(blocks).mapToLong(buf -> buf.capacity()).sum();
  }

  @Override
  public byte readByte() throws EOFException {
    try {
      ByteBuffer block = blocks[blockIndex(pos)];
      byte v = block.get(blockOffset(pos));
      pos++;
      return v;
    } catch (IndexOutOfBoundsException e) {
      if (pos >= length()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  /**
   * Reads exactly {@code len} bytes into the given buffer. The buffer must have enough remaining
   * limit.
   *
   * <p>If there are fewer than {@code len} bytes in the input, {@link EOFException} is thrown.
   */
  public void readBytes(ByteBuffer buffer, int len) throws EOFException {
    try {
      while (len > 0) {
        ByteBuffer block = blocks[blockIndex(pos)].duplicate();
        int blockOffset = blockOffset(pos);
        block.position(blockOffset);
        int chunk = Math.min(len, block.remaining());
        if (chunk == 0) {
          throw new EOFException();
        }

        // Update pos early on for EOF detection on output buffer, then try to get buffer content.
        pos += chunk;
        block.limit(blockOffset + chunk);
        buffer.put(block);

        len -= chunk;
      }
    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= length()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public void readBytes(byte[] arr, int off, int len) throws EOFException {
    try {
      while (len > 0) {
        ByteBuffer block = blocks[blockIndex(pos)].duplicate();
        block.position(blockOffset(pos));
        int chunk = Math.min(len, block.remaining());
        if (chunk == 0) {
          throw new EOFException();
        }

        // Update pos early on for EOF detection, then try to get buffer content.
        pos += chunk;
        block.get(arr, off, chunk);

        len -= chunk;
        off += chunk;
      }
    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= length()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public short readShort() throws IOException {
    int blockOffset = blockOffset(pos);
    if (blockOffset + Short.BYTES <= blockMask) {
      short v = blocks[blockIndex(pos)].getShort(blockOffset);
      pos += Short.BYTES;
      return v;
    } else {
      return super.readShort();
    }
  }

  @Override
  public int readInt() throws IOException {
    int blockOffset = blockOffset(pos);
    if (blockOffset + Integer.BYTES <= blockMask) {
      int v = blocks[blockIndex(pos)].getInt(blockOffset);
      pos += Integer.BYTES;
      return v;
    } else {
      return super.readInt();
    }
  }

  @Override
  public long readLong() throws IOException {
    int blockOffset = blockOffset(pos);
    if (blockOffset + Long.BYTES <= blockMask) {
      long v = blocks[blockIndex(pos)].getLong(blockOffset);
      pos += Long.BYTES;
      return v;
    } else {
      return super.readLong();
    }
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public byte readByte(long pos) {
    pos += offset;
    return blocks[blockIndex(pos)].get(blockOffset(pos));
  }

  @Override
  public short readShort(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    if (blockOffset + Short.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getShort(blockOffset);
    } else {
      return (short) ((readByte(pos) & 0xFF) | (readByte(pos + 1) & 0xFF) << 8);
    }
  }

  @Override
  public int readInt(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    if (blockOffset + Integer.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getInt(blockOffset);
    } else {
      return ((readByte(pos) & 0xFF)
          | (readByte(pos + 1) & 0xFF) << 8
          | (readByte(pos + 2) & 0xFF) << 16
          | (readByte(pos + 3) << 24));
    }
  }

  @Override
  public long readLong(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    if (blockOffset + Long.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getLong(blockOffset);
    } else {
      final byte b1 = readByte(pos);
      final byte b2 = readByte(pos + 1);
      final byte b3 = readByte(pos + 2);
      final byte b4 = readByte(pos + 3);
      final byte b5 = readByte(pos + 4);
      final byte b6 = readByte(pos + 5);
      final byte b7 = readByte(pos + 6);
      final byte b8 = readByte(pos + 7);
      return (b8 & 0xFFL) << 56
          | (b7 & 0xFFL) << 48
          | (b6 & 0xFFL) << 40
          | (b5 & 0xFFL) << 32
          | (b4 & 0xFFL) << 24
          | (b3 & 0xFFL) << 16
          | (b2 & 0xFFL) << 8
          | (b1 & 0xFFL);
    }
  }

  @Override
  public void readFloats(float[] arr, int off, int len) throws EOFException {
    try {
      while (len > 0) {
        FloatBuffer floatBuffer = getFloatBuffer(pos);
        floatBuffer.position(blockOffset(pos) >> 2);
        int chunk = Math.min(len, floatBuffer.remaining());
        if (chunk == 0) {
          // read a single float spanning the boundary between two buffers
          arr[off] = Float.intBitsToFloat(readInt(pos - offset));
          off++;
          len--;
          pos += Float.BYTES;
          continue;
        }

        // Update pos early on for EOF detection, then try to get buffer content.
        pos += chunk << 2;
        floatBuffer.get(arr, off, chunk);

        len -= chunk;
        off += chunk;
      }
    } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
      if (pos - offset + Float.BYTES > length()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public void readLongs(long[] arr, int off, int len) throws EOFException {
    try {
      while (len > 0) {
        LongBuffer longBuffer = getLongBuffer(pos);
        longBuffer.position(blockOffset(pos) >> 3);
        int chunk = Math.min(len, longBuffer.remaining());
        if (chunk == 0) {
          // read a single long spanning the boundary between two buffers
          arr[off] = readLong(pos - offset);
          off++;
          len--;
          pos += Long.BYTES;
          continue;
        }

        // Update pos early on for EOF detection, then try to get buffer content.
        pos += chunk << 3;
        longBuffer.get(arr, off, chunk);

        len -= chunk;
        off += chunk;
      }
    } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
      if (pos - offset + Long.BYTES > length()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  private FloatBuffer getFloatBuffer(long pos) {
    // This creates a separate FloatBuffer for each observed combination of ByteBuffer/alignment
    int bufferIndex = blockIndex(pos);
    int alignment = (int) pos & 0x3;
    int floatBufferIndex = bufferIndex * Float.BYTES + alignment;
    if (floatBuffers[floatBufferIndex] == null) {
      ByteBuffer dup = blocks[bufferIndex].duplicate();
      dup.position(alignment);
      floatBuffers[floatBufferIndex] = dup.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    }
    return floatBuffers[floatBufferIndex];
  }

  private LongBuffer getLongBuffer(long pos) {
    // This creates a separate LongBuffer for each observed combination of ByteBuffer/alignment
    int bufferIndex = blockIndex(pos);
    int alignment = (int) pos & 0x7;
    int longBufferIndex = bufferIndex * Long.BYTES + alignment;
    if (longBuffers[longBufferIndex] == null) {
      ByteBuffer dup = blocks[bufferIndex].duplicate();
      dup.position(alignment);
      longBuffers[longBufferIndex] = dup.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    }
    return longBuffers[longBufferIndex];
  }

  public long position() {
    return pos - offset;
  }

  public void seek(long position) throws EOFException {
    this.pos = position + offset;
    if (position > length()) {
      this.pos = length();
      throw new EOFException();
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    long skipTo = position() + numBytes;
    seek(skipTo);
  }

  public ByteBuffersDataInput slice(long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "slice(offset=%s, length=%s) is out of bounds: %s",
              offset,
              length,
              this));
    }

    return new ByteBuffersDataInput(sliceBufferList(Arrays.asList(this.blocks), offset, length));
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%,d bytes, block size: %,d, blocks: %,d, position: %,d%s",
        length(),
        blockSize(),
        blocks.length,
        position(),
        offset == 0 ? "" : String.format(Locale.ROOT, " [offset: %,d]", offset));
  }

  private final int blockIndex(long pos) {
    return Math.toIntExact(pos >> blockBits);
  }

  private final int blockOffset(long pos) {
    return (int) pos & blockMask;
  }

  private int blockSize() {
    return 1 << blockBits;
  }

  private static final boolean isPowerOfTwo(int v) {
    return (v & (v - 1)) == 0;
  }

  private static void ensureAssumptions(List<ByteBuffer> buffers) {
    if (buffers.isEmpty()) {
      throw new IllegalArgumentException("Buffer list must not be empty.");
    }

    if (buffers.size() == 1) {
      // Special case of just a single buffer, conditions don't apply.
    } else {
      final int blockPage = determineBlockPage(buffers);

      // First buffer decides on block page length.
      if (!isPowerOfTwo(blockPage)) {
        throw new IllegalArgumentException(
            "The first buffer must have power-of-two position() + remaining(): 0x"
                + Integer.toHexString(blockPage));
      }

      // Any block from 2..last-1 should have the same page size.
      for (int i = 1, last = buffers.size() - 1; i < last; i++) {
        ByteBuffer buffer = buffers.get(i);
        if (buffer.position() != 0) {
          throw new IllegalArgumentException(
              "All buffers except for the first one must have position() == 0: " + buffer);
        }
        if (i != last && buffer.remaining() != blockPage) {
          throw new IllegalArgumentException(
              "Intermediate buffers must share an identical remaining() power-of-two block size: 0x"
                  + Integer.toHexString(blockPage));
        }
      }
    }
  }

  static int determineBlockPage(List<ByteBuffer> buffers) {
    ByteBuffer first = buffers.get(0);
    final int blockPage = Math.toIntExact((long) first.position() + first.remaining());
    return blockPage;
  }

  private static List<ByteBuffer> sliceBufferList(
      List<ByteBuffer> buffers, long offset, long length) {
    ensureAssumptions(buffers);

    if (buffers.size() == 1) {
      ByteBuffer cloned = buffers.get(0).asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
      ;
      cloned.position(Math.toIntExact(cloned.position() + offset));
      cloned.limit(Math.toIntExact(cloned.position() + length));
      return Arrays.asList(cloned);
    } else {
      long absStart = buffers.get(0).position() + offset;
      long absEnd = absStart + length;

      int blockBytes = ByteBuffersDataInput.determineBlockPage(buffers);
      int blockBits = Integer.numberOfTrailingZeros(blockBytes);
      long blockMask = (1L << blockBits) - 1;

      int endOffset = Math.toIntExact(absEnd & blockMask);

      ArrayList<ByteBuffer> cloned =
          buffers
              .subList(
                  Math.toIntExact(absStart / blockBytes),
                  Math.toIntExact(absEnd / blockBytes + (endOffset == 0 ? 0 : 1)))
              .stream()
              .map(buf -> buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN))
              .collect(Collectors.toCollection(ArrayList::new));

      if (endOffset == 0) {
        cloned.add(ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN));
      }

      cloned.get(0).position(Math.toIntExact(absStart & blockMask));
      cloned.get(cloned.size() - 1).limit(endOffset);
      return cloned;
    }
  }
}
