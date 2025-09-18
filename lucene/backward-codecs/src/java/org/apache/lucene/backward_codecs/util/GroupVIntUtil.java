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
package org.apache.lucene.backward_codecs.util;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;

/**
 * This class contains utility methods for group varint encoding/decoding of long[] arrays. These
 * methods are only used by backward codecs and have been moved here from the main GroupVIntUtil
 * class.
 *
 * @lucene.internal
 */
public final class GroupVIntUtil {
  /** The maximum length of a single group-varint is 1 byte flag and 4 integers. */
  public static final int MAX_LENGTH_PER_GROUP = Byte.BYTES + 4 * Integer.BYTES;

  private GroupVIntUtil() {} // no instance

  /**
   * Read all the group varints, including the tail vints to a long[].
   *
   * @param dst the array to read ints into.
   * @param limit the number of int values to read.
   * @lucene.experimental
   */
  public static void readGroupVInts(DataInput in, long[] dst, int limit) throws IOException {
    int i;
    for (i = 0; i <= limit - 4; i += 4) {
      readGroupVInt(in, dst, i);
    }
    for (; i < limit; ++i) {
      dst[i] = in.readVInt() & 0xFFFFFFFFL;
    }
  }

  /**
   * Default implementation of read single group, for optimal performance, you should use {@link
   * GroupVIntUtil#readGroupVInts(DataInput, long[], int)} instead.
   *
   * @param in the input to use to read data.
   * @param dst the array to read ints into.
   * @param offset the offset in the array to start storing ints.
   */
  public static void readGroupVInt(DataInput in, long[] dst, int offset) throws IOException {
    final int flag = in.readByte() & 0xFF;

    final int n1Minus1 = flag >> 6;
    final int n2Minus1 = (flag >> 4) & 0x03;
    final int n3Minus1 = (flag >> 2) & 0x03;
    final int n4Minus1 = flag & 0x03;

    dst[offset] = readIntInGroup(in, n1Minus1) & 0xFFFFFFFFL;
    dst[offset + 1] = readIntInGroup(in, n2Minus1) & 0xFFFFFFFFL;
    dst[offset + 2] = readIntInGroup(in, n3Minus1) & 0xFFFFFFFFL;
    dst[offset + 3] = readIntInGroup(in, n4Minus1) & 0xFFFFFFFFL;
  }

  private static int readIntInGroup(DataInput in, int numBytesMinus1) throws IOException {
    switch (numBytesMinus1) {
      case 0:
        return in.readByte() & 0xFF;
      case 1:
        return in.readShort() & 0xFFFF;
      case 2:
        return (in.readShort() & 0xFFFF) | ((in.readByte() & 0xFF) << 16);
      default:
        return in.readInt();
    }
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
