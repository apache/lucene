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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.packed.PackedInts;

/** Utility class to encode sequences of 128 small positive integers. */
final class PForUtil {

  private static final int MAX_EXCEPTIONS = 7;

  // IDENTITY_PLUS_ONE[i] == i + 1
  private static final long[] IDENTITY_PLUS_ONE = new long[ForUtil.BLOCK_SIZE];

  static {
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      IDENTITY_PLUS_ONE[i] = i + 1;
    }
  }

  static boolean allEqual(long[] l) {
    for (int i = 1; i < ForUtil.BLOCK_SIZE; ++i) {
      if (l[i] != l[0]) {
        return false;
      }
    }
    return true;
  }

  private final ForUtil forUtil;

  PForUtil(ForUtil forUtil) {
    assert ForUtil.BLOCK_SIZE <= 256 : "blocksize must fit in one byte. got " + ForUtil.BLOCK_SIZE;
    this.forUtil = forUtil;
  }

  /** Encode 128 integers from {@code longs} into {@code out}. */
  void encode(long[] longs, DataOutput out) throws IOException {
    // Determine the top MAX_EXCEPTIONS + 1 values
    final LongHeap top = new LongHeap(MAX_EXCEPTIONS + 1);
    for (int i = 0; i <= MAX_EXCEPTIONS; ++i) {
      top.push(longs[i]);
    }
    long topValue = top.top();
    for (int i = MAX_EXCEPTIONS + 1; i < ForUtil.BLOCK_SIZE; ++i) {
      if (longs[i] > topValue) {
        topValue = top.updateTop(longs[i]);
      }
    }

    long max = 0L;
    for (int i = 1; i <= top.size(); ++i) {
      max = Math.max(max, top.get(i));
    }

    final int maxBitsRequired = PackedInts.bitsRequired(max);
    // We store the patch on a byte, so we can't decrease the number of bits required by more than 8
    final int patchedBitsRequired =
        Math.max(PackedInts.bitsRequired(topValue), maxBitsRequired - 8);
    int numExceptions = 0;
    final long maxUnpatchedValue = (1L << patchedBitsRequired) - 1;
    for (int i = 2; i <= top.size(); ++i) {
      if (top.get(i) > maxUnpatchedValue) {
        numExceptions++;
      }
    }
    final byte[] exceptions = new byte[numExceptions * 2];
    if (numExceptions > 0) {
      int exceptionCount = 0;
      for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
        if (longs[i] > maxUnpatchedValue) {
          exceptions[exceptionCount * 2] = (byte) i;
          exceptions[exceptionCount * 2 + 1] = (byte) (longs[i] >>> patchedBitsRequired);
          longs[i] &= maxUnpatchedValue;
          exceptionCount++;
        }
      }
      assert exceptionCount == numExceptions : exceptionCount + " " + numExceptions;
    }

    if (allEqual(longs) && maxBitsRequired <= 8) {
      for (int i = 0; i < numExceptions; ++i) {
        exceptions[2 * i + 1] =
            (byte) (Byte.toUnsignedLong(exceptions[2 * i + 1]) << patchedBitsRequired);
      }
      out.writeByte((byte) (numExceptions << 5));
      out.writeVLong(longs[0]);
    } else {
      final int token = (numExceptions << 5) | patchedBitsRequired;
      out.writeByte((byte) token);
      forUtil.encode(longs, patchedBitsRequired, out);
    }
    out.writeBytes(exceptions, exceptions.length);
  }

  /** Decode 128 integers into {@code ints}. */
  void decode(DataInput in, long[] longs) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (bitsPerValue == 0) {
      Arrays.fill(longs, 0, ForUtil.BLOCK_SIZE, in.readVLong());
    } else {
      forUtil.decode(bitsPerValue, in, longs);
    }
    for (int i = 0; i < numExceptions; ++i) {
      longs[Byte.toUnsignedInt(in.readByte())] |=
          Byte.toUnsignedLong(in.readByte()) << bitsPerValue;
    }
  }

  /** Skip 128 integers. */
  void skip(DataInput in) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (bitsPerValue == 0) {
      in.readVLong();
      in.skipBytes((numExceptions << 1));
    } else {
      in.skipBytes(forUtil.numBytes(bitsPerValue) + (numExceptions << 1));
    }
  }
}
