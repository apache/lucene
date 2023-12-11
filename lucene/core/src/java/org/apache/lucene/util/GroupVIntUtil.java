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

/**
 * This class contains utility methods and constants for group varint
 *
 * @lucene.internal
 */
public final class GroupVIntUtil {
  // the maximum length of a single group-varint is 4 integers + 1 byte flag.
  public static final int MAX_LENGTH_PER_GROUP = 17;
  private static final int[] MASKS = new int[] {0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF};

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
   * Faster implementation of read groups, the specialized method can only be used only when the
   * input implementation is base on random access buffer, it also means skipBytes an efficient
   * implementation. (only changes file pointer, does not read bytes.)
   *
   * @param in the input to use to read data.
   * @param remaining the number of remaining bytes allowed to read for current block/segment. it
   *     read values from the buffer that would not cross boundaries.
   * @param reader the supplier of read int.
   * @param pos the start pos to read from the reader.
   * @param dst the array to read ints into.
   * @param limit the number of int values to read.
   */
  public static void readGroupVInts(
      DataInput in, long remaining, IntReader reader, long pos, long[] dst, int limit)
      throws IOException {
    int i;
    for (i = 0; i <= limit - 4; i += 4) {
      if (remaining < MAX_LENGTH_PER_GROUP) {
        readGroupVInt(in, dst, i);
      } else {
        final int len = readGroupVInt(in, reader, pos, dst, i);
        pos += len + 1;
        remaining -= len + 1;
        in.skipBytes(len);
      }
    }

    for (; i < limit; ++i) {
      dst[i] = in.readVInt();
    }
  }

  /**
   * Read a group base on a random-access reader, the caller must be make sure the remaining byte
   * greater than {@link #MAX_LENGTH_PER_GROUP}
   *
   * @param in the input to use to read data.
   * @param reader the supplier of read int.
   * @param pos the start pos to read from the reader.
   * @param dst the array to read ints into.
   * @param offset the offset in the array to start storing ints.
   * @return the number of bytes read excluding the flag. this indicates the number of positions
   *     should to be increased for caller, it is a positive number and less than {@link
   *     #MAX_LENGTH_PER_GROUP}
   */
  private static int readGroupVInt(DataInput in, IntReader reader, long pos, long[] dst, int offset)
      throws IOException {
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
}
