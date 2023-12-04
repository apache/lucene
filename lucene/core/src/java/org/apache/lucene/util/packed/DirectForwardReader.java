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
package org.apache.lucene.util.packed;

import static org.apache.lucene.util.packed.UnrollingDecoder.*;

import java.io.IOException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectWriter}
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = 100;
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = DirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i &lt; numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 *
 * @see DirectWriter
 */
public class DirectForwardReader {

  static final int BLOCK_SHIFT = 7;
  private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
  private static final int BLOCK_MASK = BLOCK_SIZE - 1;
  // warm up if we read more than 4/5 times in the first block
  private static final int WARM_UP_THRESHOLD = (int) (BLOCK_SIZE * 0.8);

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(
      RandomAccessInput slice, int bitsPerValue, long offset, long numValues) {
    switch (bitsPerValue) {
      case 1:
        return new DirectForwardReader1(slice, offset, numValues);
      case 2:
        return new DirectForwardReader2(slice, offset, numValues);
      case 4:
        return new DirectForwardReader4(slice, offset, numValues);
      case 8:
        return new DirectForwardReader8(slice, offset, numValues);
      case 12:
        return new DirectForwardReader12(slice, offset, numValues);
      case 16:
        return new DirectForwardReader16(slice, offset, numValues);
      case 20:
        return new DirectForwardReader20(slice, offset, numValues);
      case 24:
        return new DirectForwardReader24(slice, offset, numValues);
      case 28:
        return new DirectForwardReader28(slice, offset, numValues);
      case 32:
        return new DirectForwardReader32(slice, offset, numValues);
      case 40:
        return new DirectForwardReader40(slice, offset, numValues);
      case 48:
        return new DirectForwardReader48(slice, offset, numValues);
      case 56:
        return new DirectForwardReader56(slice, offset, numValues);
      case 64:
        return new DirectForwardReader64(slice, offset, numValues);
      default:
        throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  private abstract static class ForwardWarmUpDirectReader extends LongValues {
    private final long[] buffer = new long[BLOCK_SIZE];
    private final int remainderBlock;
    private boolean checking = true;
    private boolean warm = true;
    private long maxIndex;
    private int counter = 0;
    final RandomAccessInput in;
    final long offset;
    long currentBlock = -1;

    public ForwardWarmUpDirectReader(RandomAccessInput in, long offset, long numValues) {
      this.in = in;
      this.offset = offset;
      this.remainderBlock = (numValues & BLOCK_MASK) == 0 ? -1 : (int) (numValues >>> BLOCK_SHIFT);
    }

    @Override
    public long get(long index) {
      if (checking) {
        if (counter++ == 0) {
          maxIndex = index + BLOCK_SIZE;
        }
        if (index >= maxIndex) {
          warm = counter >= WARM_UP_THRESHOLD;
          checking = false;
        }
      }
      try {
        if (warm) {
          final long block = index >> BLOCK_SHIFT;
          if (block == currentBlock) {
            return buffer[(int) (index & BLOCK_MASK)];
          } else if (block == remainderBlock) {
            return doGet(index);
          } else {
            fillBuffer(block, buffer);
            currentBlock = block;
            return buffer[(int) (index & BLOCK_MASK)];
          }
        } else {
          return doGet(index);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected void readLongs(long pos, long[] dst, int off, int len) throws IOException {
      in.readLongs(pos, dst, off, len);
    }

    abstract long doGet(long index) throws IOException;

    abstract void fillBuffer(long block, long[] buffer) throws IOException;
  }

  static final class DirectForwardReader1 extends ForwardWarmUpDirectReader {
    static final int BPV = 1;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    static final int NUM_VALUES_PER_LONG = Long.SIZE / BPV;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader1(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      int shift = (int) (index & 7);
      return (in.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode1(tmp, buffer);
    }
  }

  static final class DirectForwardReader2 extends ForwardWarmUpDirectReader {
    static final int BPV = 2;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    static final int NUM_VALUES_PER_LONG = Long.SIZE / BPV;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader2(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      int shift = ((int) (index & 3)) << 1;
      return (in.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode2(tmp, buffer);
    }
  }

  static final class DirectForwardReader4 extends ForwardWarmUpDirectReader {
    static final int BPV = 4;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader4(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      int shift = (int) (index & 1) << 2;
      return (in.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode4(tmp, buffer);
    }
  }

  static final class DirectForwardReader8 extends ForwardWarmUpDirectReader {
    static final int BPV = 8;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader8(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      return in.readByte(offset + index) & 0xFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode8(tmp, buffer);
    }
  }

  static final class DirectForwardReader12 extends ForwardWarmUpDirectReader {
    static final int BPV = 12;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader12(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      long offset = (index * 12) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readShort(this.offset + offset) >>> shift) & 0xFFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode12(tmp, buffer);
    }
  }

  static final class DirectForwardReader16 extends ForwardWarmUpDirectReader {
    static final int BPV = 16;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader16(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      return in.readShort(offset + (index << 1)) & 0xFFFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode16(tmp, buffer);
    }
  }

  static final class DirectForwardReader20 extends ForwardWarmUpDirectReader {
    static final int BPV = 20;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader20(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      long offset = (index * 20) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode20(tmp, buffer);
    }
  }

  static final class DirectForwardReader24 extends ForwardWarmUpDirectReader {
    static final int BPV = 24;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader24(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      return in.readInt(this.offset + index * 3) & 0xFFFFFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode24(tmp, buffer);
    }
  }

  static final class DirectForwardReader28 extends ForwardWarmUpDirectReader {
    static final int BPV = 28;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader28(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      long offset = (index * 28) >>> 3;
      int shift = (int) (index & 1) << 2;
      return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFFFF;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode28(tmp, buffer);
    }
  }

  static final class DirectForwardReader32 extends ForwardWarmUpDirectReader {
    static final int BPV = 32;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    public DirectForwardReader32(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      return in.readInt(this.offset + (index << 2)) & 0xFFFFFFFFL;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode32(tmp, buffer);
    }
  }

  static final class DirectForwardReader40 extends ForwardWarmUpDirectReader {
    static final int BPV = 40;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader40(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      return in.readLong(this.offset + index * 5) & 0xFFFFFFFFFFL;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode40(tmp, buffer);
    }
  }

  static final class DirectForwardReader48 extends ForwardWarmUpDirectReader {
    static final int BPV = 48;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader48(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      return in.readLong(this.offset + index * 6) & 0xFFFFFFFFFFFFL;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode48(tmp, buffer);
    }
  }

  static final class DirectForwardReader56 extends ForwardWarmUpDirectReader {
    static final int BPV = 56;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    final long[] tmp = new long[TMP_LENGTH];

    DirectForwardReader56(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    public long doGet(long index) throws IOException {
      return in.readLong(this.offset + index * 7) & 0xFFFFFFFFFFFFFFL;
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, tmp, 0, TMP_LENGTH);
      decode56(tmp, buffer);
    }
  }

  static final class DirectForwardReader64 extends ForwardWarmUpDirectReader {
    static final int BPV = 64;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE;

    public DirectForwardReader64(RandomAccessInput in, long offset, long numValues) {
      super(in, offset, numValues);
    }

    @Override
    long doGet(long index) throws IOException {
      return in.readLong(offset + (index << 3));
    }

    @Override
    void fillBuffer(long block, long[] buffer) throws IOException {
      readLongs(offset + BLOCK_BYTES * block, buffer, 0, BLOCK_SIZE);
    }
  }
}
