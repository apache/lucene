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
package org.apache.lucene.codecs.lucene90;

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
  // buffer for reading exception data; each exception uses two bytes (pos + high-order bits of the
  // exception)
  private final byte[] exceptionBuff = new byte[MAX_EXCEPTIONS * 2];

  PForUtil(ForUtil forUtil) {
    assert ForUtil.BLOCK_SIZE <= 256 : "blocksize must fit in one byte. got " + ForUtil.BLOCK_SIZE;
    this.forUtil = forUtil;
  }

  /** Encode 128 integers from {@code longs} into {@code out}. */
  void encode(long[] longs, DataOutput out) throws IOException {
    // Determine the top MAX_EXCEPTIONS + 1 values
    final LongHeap top = LongHeap.create(LongHeap.Order.MIN, MAX_EXCEPTIONS + 1);
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

  /** Decode deltas, compute the prefix sum and add {@code base} to all decoded longs. */
  void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (numExceptions == 0) {
      // when there are no exceptions to apply, we can be a bit more efficient with our decoding
      if (bitsPerValue == 0) {
        // a bpv of zero indicates all delta values are the same
        long val = in.readVLong();
        if (val == 1) {
          // this will often be the common case when working with doc IDs, so we special-case it to
          // be slightly more efficient
          prefixSumOfOnes(longs, base);
        } else {
          prefixSumOf(longs, base, val);
        }
      } else {
        // decode the deltas then apply the prefix sum logic
        forUtil.decodeTo32(bitsPerValue, in, longs);
        prefixSum32(longs, base);
      }
    } else {
      // pack two values per long so we can apply prefixes two-at-a-time
      if (bitsPerValue == 0) {
        fillSameValue32(longs, in.readVLong());
      } else {
        forUtil.decodeTo32(bitsPerValue, in, longs);
      }
      applyExceptions32(bitsPerValue, numExceptions, in, longs);
      prefixSum32(longs, base);
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

  /**
   * Fill {@code longs} with the final values for the case of all deltas being 1. Note this assumes
   * there are no exceptions to apply.
   */
  private static void prefixSumOfOnes(long[] longs, long base) {
    System.arraycopy(IDENTITY_PLUS_ONE, 0, longs, 0, ForUtil.BLOCK_SIZE);
    // This loop gets auto-vectorized
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      longs[i] += base;
    }
  }

  /**
   * Fill {@code longs} with the final values for the case of all deltas being {@code val}. Note
   * this assumes there are no exceptions to apply.
   */
  private static void prefixSumOf(long[] longs, long base, long val) {
    for (int i = 0; i < ForUtil.BLOCK_SIZE; i++) {
      longs[i] = (i + 1) * val + base;
    }
  }

  /**
   * Fills the {@code longs} with the provided {@code val}, packed two values per long (using 32
   * bits per value).
   */
  private static void fillSameValue32(long[] longs, long val) {
    final long token = val << 32 | val;
    Arrays.fill(longs, 0, ForUtil.BLOCK_SIZE_DIV_2, token);
  }

  /** Apply the exceptions where the values are packed two-per-long in {@code longs}. */
  private void applyExceptions32(int bitsPerValue, int numExceptions, DataInput in, long[] longs)
      throws IOException {
    in.readBytes(exceptionBuff, 0, numExceptions * 2);
    for (int i = 0; i < numExceptions; ++i) {
      final int exceptionPos = Byte.toUnsignedInt(exceptionBuff[i * 2]);
      final long exception = Byte.toUnsignedLong(exceptionBuff[i * 2 + 1]);
      // note that we pack two values per long, so the index is [0..63] for 128 values
      final int idx = exceptionPos & (ForUtil.BLOCK_SIZE_DIV_2_MASK); // mod 64
      // we need to shift by 1) the bpv, and 2) 32 for positions [0..63] (and no 32 shift for
      // [64..127])
      final int shift = bitsPerValue + ((1 ^ (exceptionPos >>> (ForUtil.BLOCK_SIZE_LOG2_MIN_1))) << 5);
      longs[idx] |= exception << shift;
    }
  }

  /** Apply prefix sum logic where the values are packed two-per-long in {@code longs}. */
  private static void prefixSum32(long[] longs, long base) {
    longs[0] += base << 32;
    ForUtil.innerPrefixSum32(longs);
    expand32(longs);
    final long l = longs[ForUtil.BLOCK_SIZE_DIV_2_MASK];
    for (int i = ForUtil.BLOCK_SIZE_DIV_2; i < ForUtil.BLOCK_SIZE; ++i) {
      longs[i] += l;
    }
  }

  /**
   * Expand the values packed two-per-long in {@code longs} into 128 individual long values stored
   * back into {@code longs}.
   */
  private static void expand32(long[] longs) {
    for (int i = 0; i < ForUtil.BLOCK_SIZE_DIV_2; ++i) {
      final long l = longs[i];
      longs[i] = l >>> 32;
      longs[ForUtil.BLOCK_SIZE_DIV_2 + i] = l & 0xFFFFFFFFL;
    }
  }
}
