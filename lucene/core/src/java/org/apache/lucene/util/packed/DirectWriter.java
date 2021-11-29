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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;

/**
 * Class for writing packed integers to be directly read from Directory. Integers can be read
 * on-the-fly via {@link DirectReader}.
 *
 * <p>Unlike PackedInts, it optimizes for read i/o operations and supports &gt; 2B values. Example
 * usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = DirectWriter.bitsRequired(100); // values up to and including 100
 *   IndexOutput output = dir.createOutput("packed", IOContext.DEFAULT);
 *   DirectWriter writer = DirectWriter.getInstance(output, numberOfValues, bitsPerValue);
 *   for (int i = 0; i &lt; numberOfValues; i++) {
 *     writer.add(value);
 *   }
 *   writer.finish();
 *   output.close();
 * </pre>
 *
 * @see DirectReader
 */
public final class DirectWriter {
  final int bitsPerValue;
  final long numValues;
  final DataOutput output;

  long count;
  boolean finished;

  // for now, just use the existing writer under the hood
  int off;
  final byte[] nextBlocks;
  final long[] nextValues;

  DirectWriter(DataOutput output, long numValues, int bitsPerValue) {
    this.output = output;
    this.numValues = numValues;
    this.bitsPerValue = bitsPerValue;

    final int memoryBudgetInBits = Math.multiplyExact(Byte.SIZE, PackedInts.DEFAULT_BUFFER_SIZE);
    // For every value we need 64 bits for the value and bitsPerValue for the encoded value
    int bufferSize = memoryBudgetInBits / (Long.SIZE + bitsPerValue);
    assert bufferSize > 0;
    // Round to the next multiple of 64
    bufferSize = Math.toIntExact(bufferSize + 63) & 0xFFFFFFC0;
    nextValues = new long[bufferSize];
    // add 7 bytes in the end so that any value could be written as a long
    nextBlocks = new byte[bufferSize * bitsPerValue / Byte.SIZE + Long.BYTES - 1];
  }

  /** Adds a value to this writer */
  public void add(long l) throws IOException {
    assert bitsPerValue == 64 || (l >= 0 && l <= PackedInts.maxValue(bitsPerValue)) : bitsPerValue;
    assert !finished;
    if (count >= numValues) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = l;
    if (off == nextValues.length) {
      flush();
    }
    count++;
  }

  private void flush() throws IOException {
    if (off == 0) {
      return;
    }
    // Avoid writing bits from values that are outside of the range we need to encode
    Arrays.fill(nextValues, off, nextValues.length, 0L);
    encode(nextValues, off, nextBlocks, bitsPerValue);
    final int blockCount =
        (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    output.writeBytes(nextBlocks, blockCount);
    off = 0;
  }

  private static void encode(long[] nextValues, int upTo, byte[] nextBlocks, int bitsPerValue) {
    if ((bitsPerValue & 7) == 0) {
      // bitsPerValue is a multiple of 8: 8, 16, 24, 32, 30, 48, 56, 64
      final int bytesPerValue = bitsPerValue / Byte.SIZE;
      for (int i = 0, o = 0; i < upTo; ++i, o += bytesPerValue) {
        final long l = nextValues[i];
        if (bitsPerValue > Integer.SIZE) {
          BitUtil.VH_LE_LONG.set(nextBlocks, o, l);
        } else if (bitsPerValue > Short.SIZE) {
          BitUtil.VH_LE_INT.set(nextBlocks, o, (int) l);
        } else if (bitsPerValue > Byte.SIZE) {
          BitUtil.VH_LE_SHORT.set(nextBlocks, o, (short) l);
        } else {
          nextBlocks[o] = (byte) l;
        }
      }
    } else if (bitsPerValue < 8) {
      // bitsPerValue is 1, 2 or 4
      final int valuesPerLong = Long.SIZE / bitsPerValue;
      for (int i = 0, o = 0; i < upTo; i += valuesPerLong, o += Long.BYTES) {
        long v = 0;
        for (int j = 0; j < valuesPerLong; ++j) {
          v |= nextValues[i + j] << (bitsPerValue * j);
        }
        BitUtil.VH_LE_LONG.set(nextBlocks, o, v);
      }
    } else {
      // bitsPerValue is 12, 20 or 28
      // Write values 2 by 2
      final int numBytesFor2Values = bitsPerValue * 2 / Byte.SIZE;
      for (int i = 0, o = 0; i < upTo; i += 2, o += numBytesFor2Values) {
        final long l1 = nextValues[i];
        final long l2 = nextValues[i + 1];
        final long merged = l1 | (l2 << bitsPerValue);
        if (bitsPerValue <= Integer.SIZE / 2) {
          BitUtil.VH_LE_INT.set(nextBlocks, o, (int) merged);
        } else {
          BitUtil.VH_LE_LONG.set(nextBlocks, o, merged);
        }
      }
    }
  }

  /** finishes writing */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException(
          "Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    assert !finished;
    flush();
    // add padding bytes for fast io
    // for every number of bits per value, we want to be able to read the entire value in a single
    // read e.g. for 20 bits per value, we want to be able to read values using ints so we need
    // 32 - 20 = 12 bits of padding
    int paddingBitsNeeded;
    if (bitsPerValue > Integer.SIZE) {
      paddingBitsNeeded = Long.SIZE - bitsPerValue;
    } else if (bitsPerValue > Short.SIZE) {
      paddingBitsNeeded = Integer.SIZE - bitsPerValue;
    } else if (bitsPerValue > Byte.SIZE) {
      paddingBitsNeeded = Short.SIZE - bitsPerValue;
    } else {
      paddingBitsNeeded = 0;
    }
    assert paddingBitsNeeded >= 0;
    final int paddingBytesNeeded = (paddingBitsNeeded + Byte.SIZE - 1) / Byte.SIZE;
    assert paddingBytesNeeded <= 3;

    for (int i = 0; i < paddingBytesNeeded; i++) {
      output.writeByte((byte) 0);
    }
    finished = true;
  }

  /** Returns an instance suitable for encoding {@code numValues} using {@code bitsPerValue} */
  public static DirectWriter getInstance(DataOutput output, long numValues, int bitsPerValue) {
    if (Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) < 0) {
      throw new IllegalArgumentException(
          "Unsupported bitsPerValue " + bitsPerValue + ". Did you use bitsRequired?");
    }
    return new DirectWriter(output, numValues, bitsPerValue);
  }

  /**
   * Round a number of bits per value to the next amount of bits per value that is supported by this
   * writer.
   *
   * @param bitsRequired the amount of bits required
   * @return the next number of bits per value that is gte the provided value and supported by this
   *     writer
   */
  private static int roundBits(int bitsRequired) {
    int index = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
    if (index < 0) {
      return SUPPORTED_BITS_PER_VALUE[-index - 1];
    } else {
      return bitsRequired;
    }
  }

  /**
   * Returns how many bits are required to hold values up to and including maxValue
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#bitsRequired(long)
   */
  public static int bitsRequired(long maxValue) {
    return roundBits(PackedInts.bitsRequired(maxValue));
  }

  /**
   * Returns how many bits are required to hold values up to and including maxValue, interpreted as
   * an unsigned value.
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#unsignedBitsRequired(long)
   */
  public static int unsignedBitsRequired(long maxValue) {
    return roundBits(PackedInts.unsignedBitsRequired(maxValue));
  }

  static final int[] SUPPORTED_BITS_PER_VALUE =
      new int[] {1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64};
}
