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

import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

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
  private static final int WARM_UP_SAMPLE_TIME = BLOCK_SIZE;
  private static final int WARM_UP_DELTA_THRESHOLD = (WARM_UP_SAMPLE_TIME * 3) >> 1;

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset, long numValues) {
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

  private static abstract class ForwardWarmUpDirectReader extends LongValues {
    private final long[] buffer = new long[BLOCK_SIZE];
    private final long remainderIndex;
    private boolean checking = true;
    private boolean warm = false;
    private long firstIndex;
    private int counter = 0;
    final RandomAccessInput in;
    final long offset;
    long currentBlock = -1;

    public ForwardWarmUpDirectReader(RandomAccessInput in, long offset, long numValues) {
      this.in = in;
      this.offset = offset;
      this.remainderIndex = numValues - (numValues & BLOCK_MASK);
    }

    @Override
    public long get(long index) {
      if (checking) {
        check(index);
      }
      try {
        return (warm && index < remainderIndex) ? warm(index) : doGet(index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private void check(long index) {
      if (counter == 0) {
        firstIndex = index;
      } else if (counter == WARM_UP_SAMPLE_TIME) {
        warm = index - firstIndex <= WARM_UP_DELTA_THRESHOLD;
        checking = false;
      }
      counter++;
    }

    private long warm(long index) throws IOException {
      final long block = index >> BLOCK_SHIFT;
      if (block != currentBlock) {
        fillBuffer(block, buffer);
        currentBlock = block;
      }
      return buffer[(int) (index & BLOCK_MASK)];
    }

    protected void readLongs(long pos, long[] dst, int off, int len) throws IOException {
      in.readLongs(pos, dst, off, len);
    }

    abstract long doGet(long index) throws IOException;

    abstract void fillBuffer(long block, long[] buffer) throws IOException;
  }

  static final class DirectForwardReader1 extends ForwardWarmUpDirectReader {
    static final int BPV = 1;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      for (int i = 0; i < TMP_LENGTH; i++) {
        long l = tmp[i];
        int pos = i << 6;
        int end = pos + NUM_VALUES_PER_LONG;
        while (pos < end) {
          buffer[pos++] = l & 1L;
          l >>>= BPV;
        }
      }
    }
  }

  static final class DirectForwardReader2 extends ForwardWarmUpDirectReader {
    static final int BPV = 2;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      for (int i = 0; i < TMP_LENGTH; i++) {
        long l = tmp[i];
        int pos = i << 5;
        int end = pos + NUM_VALUES_PER_LONG;
        while (pos < end) {
          buffer[pos++] = l & 3L;
          l >>>= BPV;
        }
      }
    }
  }

  static final class DirectForwardReader4 extends ForwardWarmUpDirectReader {
    static final int BPV = 4;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    static final int NUM_VALUES_PER_LONG = Long.SIZE / BPV;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l = tmp[++tmpIndex];
        buffer[pos++] = l & 0xFL;
        buffer[pos++] = (l >>> 4) & 0xFL;
        buffer[pos++] = (l >>> 8) & 0xFL;
        buffer[pos++] = (l >>> 12) & 0xFL;
        buffer[pos++] = (l >>> 16) & 0xFL;
        buffer[pos++] = (l >>> 20) & 0xFL;
        buffer[pos++] = (l >>> 24) & 0xFL;
        buffer[pos++] = (l >>> 28) & 0xFL;
        buffer[pos++] = (l >>> 32) & 0xFL;
        buffer[pos++] = (l >>> 36) & 0xFL;
        buffer[pos++] = (l >>> 40) & 0xFL;
        buffer[pos++] = (l >>> 44) & 0xFL;
        buffer[pos++] = (l >>> 48) & 0xFL;
        buffer[pos++] = (l >>> 52) & 0xFL;
        buffer[pos++] = (l >>> 56) & 0xFL;
        buffer[pos++] = (l >>> 60) & 0xFL;
      }
    }
  }

  static final class DirectForwardReader8 extends ForwardWarmUpDirectReader {
    static final int BPV = 8;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
    static final int TMP_LENGTH = BLOCK_BYTES / Long.BYTES;
    static final int NUM_VALUES_PER_LONG = Long.SIZE / BPV;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l = tmp[++tmpIndex];
        buffer[pos++] = l & 0xFFL;
        buffer[pos++] = (l >>> 8) & 0xFFL;
        buffer[pos++] = (l >>> 16) & 0xFFL;
        buffer[pos++] = (l >>> 24) & 0xFFL;
        buffer[pos++] = (l >>> 32) & 0xFFL;
        buffer[pos++] = (l >>> 40) & 0xFFL;
        buffer[pos++] = (l >>> 48) & 0xFFL;
        buffer[pos++] = (l >>> 56) & 0xFFL;
      }
    }
  }

  static final class DirectForwardReader12 extends ForwardWarmUpDirectReader {
    static final int BPV = 12;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l1 = tmp[++tmpIndex];
        final long l2 = tmp[++tmpIndex];
        final long l3 = tmp[++tmpIndex];
        buffer[pos++] = l1 & 0xFFFL;
        buffer[pos++] = (l1 >>> 12) & 0xFFFL;
        buffer[pos++] = (l1 >>> 24) & 0XFFFL;
        buffer[pos++] = (l1 >>> 36) & 0xFFFL;
        buffer[pos++] = (l1 >>> 48) & 0xFFFL;
        buffer[pos++] = ((l1 >>> 60) & 0xFFFL) | ((l2 & 0xFFL) << 4);
        buffer[pos++] = (l2 >>> 8) & 0xFFFL;
        buffer[pos++] = (l2 >>> 20) & 0xFFFL;
        buffer[pos++] = (l2 >>> 32) & 0xFFFL;
        buffer[pos++] = (l2 >>> 44) & 0xFFFL;
        buffer[pos++] = ((l2 >>> 56) & 0xFFFL) | ((l3 & 0xFL) << 8);
        buffer[pos++] = (l3 >>> 4) & 0xFFFL;
        buffer[pos++] = (l3 >>> 16) & 0xFFFL;
        buffer[pos++] = (l3 >>> 28) & 0xFFFL;
        buffer[pos++] = (l3 >>> 40) & 0xFFFL;
        buffer[pos++] = (l3 >>> 52) & 0xFFFL;
      }
    }
  }

  static final class DirectForwardReader16 extends ForwardWarmUpDirectReader {
    static final int BPV = 16;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l = tmp[++tmpIndex];
        buffer[pos++] = l & 0xFFFFL;
        buffer[pos++] = (l >>> 16) & 0xFFFFL;
        buffer[pos++] = (l >>> 32) & 0xFFFFL;
        buffer[pos++] = (l >>> 48) & 0xFFFFL;
      }
    }
  }

  static final class DirectForwardReader20 extends ForwardWarmUpDirectReader {
    static final int BPV = 20;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l1 = tmp[++tmpIndex];
        final long l2 = tmp[++tmpIndex];
        final long l3 = tmp[++tmpIndex];
        final long l4 = tmp[++tmpIndex];
        final long l5 = tmp[++tmpIndex];
        buffer[pos++] = l1 & 0xFFFFFL;
        buffer[pos++] = (l1 >>> 20) & 0xFFFFFL;
        buffer[pos++] = (l1 >>> 40) & 0XFFFFFL;
        buffer[pos++] = (l1 >>> 60) & 0XFFFFFL | ((l2 & 0xFFFFL) << 4);
        buffer[pos++] = (l2 >>> 16) & 0xFFFFFL;
        buffer[pos++] = (l2 >>> 36) & 0xFFFFFL;
        buffer[pos++] = (l2 >>> 56) & 0xFFFFFL | ((l3 & 0xFFFL) << 8);
        buffer[pos++] = (l3 >>> 12) & 0xFFFFFL;
        buffer[pos++] = (l3 >>> 32) & 0xFFFFFL;
        buffer[pos++] = (l3 >>> 52) & 0xFFFFFL | ((l4 & 0xFFL) << 12);
        buffer[pos++] = (l4 >>> 8) & 0xFFFFFL;
        buffer[pos++] = (l4 >>> 28) & 0xFFFFFL;
        buffer[pos++] = (l4 >>> 48) & 0xFFFFFL | ((l5 & 0xFL) << 16);
        buffer[pos++] = (l5 >>> 4) & 0xFFFFFL;
        buffer[pos++] = (l5 >>> 24) & 0xFFFFFL;
        buffer[pos++] = (l5 >>> 44) & 0xFFFFFL;
      }
    }
  }

  static final class DirectForwardReader24 extends ForwardWarmUpDirectReader {
    static final int BPV = 24;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l1 = tmp[++tmpIndex];
        final long l2 = tmp[++tmpIndex];
        final long l3 = tmp[++tmpIndex];
        buffer[pos++] = l1 & 0xFFFFFFL;
        buffer[pos++] = (l1 >>> 24) & 0xFFFFFFL;
        buffer[pos++] = (l1 >>> 48) & 0XFFFFFFL | ((l2 & 0xFFL) << 16);
        buffer[pos++] = (l2 >>> 8) & 0xFFFFFFL;
        buffer[pos++] = (l2 >>> 32) & 0xFFFFFFL;
        buffer[pos++] = (l2 >>> 56) & 0xFFFFFFL | ((l3 & 0xFFFFL) << 8);
        buffer[pos++] = (l3 >>> 16) & 0xFFFFFFL;
        buffer[pos++] = (l3 >>> 40) & 0xFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader28 extends ForwardWarmUpDirectReader {
    static final int BPV = 28;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        buffer[pos++] = tmp[++tmpIndex] & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 28) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 56) & 0XFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFL) << 8);
        buffer[pos++] = (tmp[tmpIndex] >>> 20) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 48) & 0xFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFL) << 16);
        buffer[pos++] = (tmp[tmpIndex] >>> 12) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 40) & 0xFFFFFFFL | ((tmp[++tmpIndex] & 0xFL) << 24);
        buffer[pos++] = (tmp[tmpIndex] >>> 4) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 32) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 60) & 0xFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFL) << 4);
        buffer[pos++] = (tmp[tmpIndex] >>> 24) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 52) & 0xFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFL) << 12);
        buffer[pos++] = (tmp[tmpIndex] >>> 16) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 44) & 0xFFFFFFFL | ((tmp[++tmpIndex] & 0xFFL) << 20);
        buffer[pos++] = (tmp[tmpIndex] >>> 8) & 0xFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 36) & 0xFFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader32 extends ForwardWarmUpDirectReader {
    static final int BPV = 32;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        final long l = tmp[++tmpIndex];
        buffer[pos++] = l & 0xFFFFFFFFL;
        buffer[pos++] = (l >>> 32) & 0xFFFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader40 extends ForwardWarmUpDirectReader {
    static final int BPV = 40;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        buffer[pos++] = tmp[++tmpIndex] & 0xFFFFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 40) & 0xFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFL) << 24);
        buffer[pos++] = (tmp[tmpIndex] >>> 16) & 0xFFFFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 56) & 0xFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFFFL) << 8);
        buffer[pos++] = (tmp[tmpIndex] >>> 32) & 0xFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFL) << 32);
        buffer[pos++] = (tmp[tmpIndex] >>> 8) & 0xFFFFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 48) & 0xFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFL) << 16);
        buffer[pos++] = (tmp[tmpIndex] >>> 24) & 0xFFFFFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader48 extends ForwardWarmUpDirectReader {
    static final int BPV = 48;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        buffer[pos++] = tmp[++tmpIndex] & 0xFFFFFFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 48) & 0xFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFFFL) << 16);
        buffer[pos++] = (tmp[tmpIndex] >>> 32) & 0xFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFL) << 32);
        buffer[pos++] = (tmp[tmpIndex] >>> 16) & 0xFFFFFFFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader56 extends ForwardWarmUpDirectReader {
    static final int BPV = 56;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;
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
      int pos = 0, tmpIndex = -1;
      while (pos < BLOCK_SIZE) {
        buffer[pos++] = tmp[++tmpIndex] & 0xFFFFFFFFFFFFFFL;
        buffer[pos++] = (tmp[tmpIndex] >>> 56) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFFFFFFFL) << 8);
        buffer[pos++] = (tmp[tmpIndex] >>> 48) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFFFFFL) << 16);
        buffer[pos++] = (tmp[tmpIndex] >>> 40) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFFFL) << 24);
        buffer[pos++] = (tmp[tmpIndex] >>> 32) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFFFL) << 32);
        buffer[pos++] = (tmp[tmpIndex] >>> 24) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFFFL) << 40);
        buffer[pos++] = (tmp[tmpIndex] >>> 16) & 0xFFFFFFFFFFFFFFL | ((tmp[++tmpIndex] & 0xFFL) << 48);
        buffer[pos++] = (tmp[tmpIndex] >>> 8) & 0xFFFFFFFFFFFFFFL;
      }
    }
  }

  static final class DirectForwardReader64 extends ForwardWarmUpDirectReader {
    static final int BPV = 64;
    static final int BLOCK_BYTES = BLOCK_SIZE * BPV / Byte.SIZE ;

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
