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

import java.io.IOException;
import java.io.UncheckedIOException;
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
public class DirectReader {

  static final int MERGE_BUFFER_SHIFT = 7;
  private static final int MERGE_BUFFER_SIZE = 1 << MERGE_BUFFER_SHIFT;
  private static final int MERGE_BUFFER_MASK = MERGE_BUFFER_SIZE - 1;

  /**
   * Retrieves an instance from the specified slice written decoding {@code bitsPerValue} for each
   * value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue) {
    return getInstance(slice, bitsPerValue, 0);
  }

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset) {
    switch (bitsPerValue) {
      case 1:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          public long doGet(long index) throws IOException {
            int shift = (int) (index & 7);
            return (in.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
          }
        };
      case 2:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          public long doGet(long index) throws IOException {
            int shift = ((int) (index & 3)) << 1;
            return (in.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
          }
        };
      case 4:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          public long doGet(long index) throws IOException {
            int shift = (int) (index & 1) << 2;
            return (in.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
          }
        };
      case 8:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          public long doGet(long index) throws IOException {
            return in.readByte(offset + index) & 0xFF;
          }
        };
      case 12:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            long offset = (index * 12) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (in.readShort(this.offset + offset) >>> shift) & 0xFFF;
          }
        };
      case 16:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readShort(offset + (index << 1)) & 0xFFFF;
          }
        };
      case 20:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            long offset = (index * 20) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFF;
          }
        };
      case 24:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readInt(this.offset + index * 3) & 0xFFFFFF;
          }
        };
      case 28:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            long offset = (index * 28) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFFFF;
          }
        };
      case 32:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readInt(this.offset + (index << 2)) & 0xFFFFFFFFL;
          }
        };
      case 40:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readLong(this.offset + index * 5) & 0xFFFFFFFFFFL;
          }
        };
      case 48:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readLong(this.offset + index * 6) & 0xFFFFFFFFFFFFL;
          }
        };
      case 56:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readLong(this.offset + index * 7) & 0xFFFFFFFFFFFFFFL;
          }
        };
      case 64:
        return new AbstractDirectPackedReader(slice, offset) {
          @Override
          protected long doGet(long index) throws IOException {
            return in.readLong(offset + (index << 3));
          }
        };
      default:
        throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  /**
   * Retrieves an instance that is specialized for merges and is typically faster at sequential
   * access but slower at random access.
   */
  public static LongValues getMergeInstance(
      RandomAccessInput slice, int bitsPerValue, long numValues) {
    return getMergeInstance(slice, bitsPerValue, 0L, numValues);
  }

  /**
   * Retrieves an instance that is specialized for merges and is typically faster at sequential
   * access.
   */
  public static LongValues getMergeInstance(
      RandomAccessInput slice, int bitsPerValue, long baseOffset, long numValues) {
    return new LongValues() {

      private final long[] buffer = new long[MERGE_BUFFER_SIZE];
      private long blockIndex = -1;

      @Override
      public long get(long index) {
        assert index < numValues;
        final long blockIndex = index >>> MERGE_BUFFER_SHIFT;
        if (this.blockIndex != blockIndex) {
          try {
            fillBuffer(blockIndex << MERGE_BUFFER_SHIFT);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          this.blockIndex = blockIndex;
        }
        return buffer[(int) (index & MERGE_BUFFER_MASK)];
      }

      private void fillBuffer(long index) throws IOException {
        // NOTE: we're not allowed to read more than 3 bytes past the last value
        if (index >= numValues - MERGE_BUFFER_SIZE) {
          // 128 values left or less
          final LongValues slowInstance = getInstance(slice, bitsPerValue, baseOffset);
          final int numValuesLastBlock = Math.toIntExact(numValues - index);
          for (int i = 0; i < numValuesLastBlock; ++i) {
            buffer[i] = slowInstance.get(index + i);
          }
        } else if ((bitsPerValue & 0x07) == 0) {
          // bitsPerValue is a multiple of 8: 8, 16, 24, 32, 30, 48, 56, 64
          final int bytesPerValue = bitsPerValue / Byte.SIZE;
          final long mask = bitsPerValue == 64 ? ~0L : (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          for (int i = 0; i < MERGE_BUFFER_SIZE; ++i) {
            if (bitsPerValue > Integer.SIZE) {
              buffer[i] = slice.readLong(offset) & mask;
            } else if (bitsPerValue > Short.SIZE) {
              buffer[i] = slice.readInt(offset) & mask;
            } else if (bitsPerValue > Byte.SIZE) {
              buffer[i] = Short.toUnsignedLong(slice.readShort(offset));
            } else {
              buffer[i] = Byte.toUnsignedLong(slice.readByte(offset));
            }
            offset += bytesPerValue;
          }
        } else if (bitsPerValue < 8) {
          // bitsPerValue is 1, 2 or 4
          final int valuesPerLong = Long.SIZE / bitsPerValue;
          final long mask = (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          int i = 0;
          for (int l = 0; l < 2 * bitsPerValue; ++l) {
            final long bits = slice.readLong(offset);
            for (int j = 0; j < valuesPerLong; ++j) {
              buffer[i++] = (bits >>> (j * bitsPerValue)) & mask;
            }
            offset += Long.BYTES;
          }
        } else {
          // bitsPerValue is 12, 20 or 28
          // Read values 2 by 2
          final int numBytesFor2Values = bitsPerValue * 2 / Byte.SIZE;
          final long mask = (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          for (int i = 0; i < MERGE_BUFFER_SIZE; i += 2) {
            final long l;
            if (numBytesFor2Values > Integer.BYTES) {
              l = slice.readLong(offset);
            } else {
              l = slice.readInt(offset);
            }
            buffer[i] = l & mask;
            buffer[i + 1] = (l >>> bitsPerValue) & mask;
            offset += numBytesFor2Values;
          }
        }
      }
    };
  }

  private abstract static class AbstractDirectPackedReader extends LongValues {
    protected final RandomAccessInput in;
    protected final long offset;

    protected AbstractDirectPackedReader(final RandomAccessInput in, final long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return doGet(index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected abstract long doGet(long index) throws IOException;
  }
}
