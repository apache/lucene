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

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * This class contains utility methods and constants for group varint
 *
 * @lucene.internal
 */
public final class GroupVIntUtil {
  // the maximum length of a single group-varint is 4 integers + 1 byte flag.
  public static final int MAX_LENGTH_PER_GROUP = 17;

  // we use long array instead of int array to make negative integer to be read as positive long.
  private static final long[] MASKS = new long[] {0xFFL, 0xFFFFL, 0xFFFFFFL, 0xFFFFFFFFL};

  /**
   * Default implementation of read single group, for optimal performance, you should use {@link
   * DataInput#readGroupVInts(long[], int)} instead.
   *
   * @param dst the array to read ints into.
   * @param offset the offset in the array to start storing ints.
   */
  public static void readGroupVInt(DataInput in, long[] dst, int offset) throws IOException {
    final int flag = in.readByte() & 0xFF;

    final int n1Minus1 = flag >> 6;
    final int n2Minus1 = (flag >> 4) & 0x03;
    final int n3Minus1 = (flag >> 2) & 0x03;
    final int n4Minus1 = flag & 0x03;

    dst[offset] = readLongInGroup(in, n1Minus1);
    dst[offset + 1] = readLongInGroup(in, n2Minus1);
    dst[offset + 2] = readLongInGroup(in, n3Minus1);
    dst[offset + 3] = readLongInGroup(in, n4Minus1);
  }

  private static long readLongInGroup(DataInput in, int numBytesMinus1) throws IOException {
    switch (numBytesMinus1) {
      case 0:
        return in.readByte() & 0xFFL;
      case 1:
        return in.readShort() & 0xFFFFL;
      case 2:
        return (in.readShort() & 0xFFFFL) | ((in.readByte() & 0xFFL) << 16);
      default:
        return in.readInt() & 0xFFFFFFFFL;
    }
  }

  /**
   * Provides an abstraction for read int values, so that decoding logic can be reused in different
   * DataInput.
   */
  @FunctionalInterface
  public static interface IntReader {
    int read(long v);
  }

  /**
   * Faster implementation of read single group, It read values from the buffer that would not cross
   * boundaries.
   *
   * @param in the input to use to read data.
   * @param remaining the number of remaining bytes allowed to read for current block/segment.
   * @param reader the supplier of read int.
   * @param pos the start pos to read from the reader.
   * @param dst the array to read ints into.
   * @param offset the offset in the array to start storing ints.
   * @return the number of bytes read excluding the flag. this indicates the number of positions
   *     should to be increased for caller, it is 0 or positive number and less than {@link
   *     #MAX_LENGTH_PER_GROUP}
   */
  public static int readGroupVInt(
      DataInput in, long remaining, IntReader reader, long pos, long[] dst, int offset)
      throws IOException {
    if (remaining < MAX_LENGTH_PER_GROUP) {
      readGroupVInt(in, dst, offset);
      return 0;
    }
    final int flag = in.readByte() & 0xFF;
    final long posStart = ++pos; // exclude the flag bytes, the position has updated via readByte().
    final int n1Minus1 = flag >> 6;
    final int n2Minus1 = (flag >> 4) & 0x03;
    final int n3Minus1 = (flag >> 2) & 0x03;
    final int n4Minus1 = flag & 0x03;

    // This code path has fewer conditionals and tends to be significantly faster in benchmarks
    dst[offset] = reader.read(pos) & MASKS[n1Minus1];
    pos += 1 + n1Minus1;
    dst[offset + 1] = reader.read(pos) & MASKS[n2Minus1];
    pos += 1 + n2Minus1;
    dst[offset + 2] = reader.read(pos) & MASKS[n3Minus1];
    pos += 1 + n3Minus1;
    dst[offset + 3] = reader.read(pos) & MASKS[n4Minus1];
    pos += 1 + n4Minus1;
    return (int) (pos - posStart);
  }

  private static int numBytes(int v) {
    // | 1 to return 1 when v = 0
    return Integer.BYTES - (Integer.numberOfLeadingZeros(v | 1) >> 3);
  }

  private static int toInt(long value) {
    if ((Long.compareUnsigned(value, 0xFFFFFFFFL) > 0)) {
      throw new ArithmeticException("integer overflow");
    }
    return (int) value;
  }

  /**
   * The implementation for group-varint encoding, It uses a maximum of {@link
   * #MAX_LENGTH_PER_GROUP} bytes scratch buffer.
   */
  public static void writeGroupVInts(DataOutput out, byte[] scratch, long[] values, int limit)
      throws IOException {
    int readPos = 0;

    // encode each group
    while ((limit - readPos) >= 4) {
      int writePos = 0;
      final int n1Minus1 = numBytes(toInt(values[readPos])) - 1;
      final int n2Minus1 = numBytes(toInt(values[readPos + 1])) - 1;
      final int n3Minus1 = numBytes(toInt(values[readPos + 2])) - 1;
      final int n4Minus1 = numBytes(toInt(values[readPos + 3])) - 1;
      int flag = (n1Minus1 << 6) | (n2Minus1 << 4) | (n3Minus1 << 2) | (n4Minus1);
      scratch[writePos++] = (byte) flag;
      BitUtil.VH_LE_INT.set(scratch, writePos, (int) (values[readPos++]));
      writePos += n1Minus1 + 1;
      BitUtil.VH_LE_INT.set(scratch, writePos, (int) (values[readPos++]));
      writePos += n2Minus1 + 1;
      BitUtil.VH_LE_INT.set(scratch, writePos, (int) (values[readPos++]));
      writePos += n3Minus1 + 1;
      BitUtil.VH_LE_INT.set(scratch, writePos, (int) (values[readPos++]));
      writePos += n4Minus1 + 1;

      out.writeBytes(scratch, writePos);
    }

    // tail vints
    for (; readPos < limit; readPos++) {
      out.writeVInt(toInt(values[readPos]));
    }
  }
}
