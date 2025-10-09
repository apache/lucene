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
package org.apache.lucene.backward_codecs.lucene103;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/** Utility class to encode sequences of 128 small positive integers. */
final class PForUtil {

  private static final int MAX_EXCEPTIONS = 7;

  static boolean allEqual(int[] l) {
    for (int i = 1; i < ForUtil.BLOCK_SIZE; ++i) {
      if (l[i] != l[0]) {
        return false;
      }
    }
    return true;
  }

  private final ForUtil forUtil = new ForUtil();

  static {
    assert ForUtil.BLOCK_SIZE <= 256 : "blocksize must fit in one byte. got " + ForUtil.BLOCK_SIZE;
  }

  /** Encode 128 integers from {@code ints} into {@code out}. */
  void encode(int[] ints, DataOutput out) throws IOException {
    // histogram of bit widths
    final int[] histogram = new int[32];
    int maxBitsRequired = 0;
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      final int v = ints[i];
      final int bits = PackedInts.bitsRequired(v);
      histogram[bits]++;
      maxBitsRequired = Math.max(maxBitsRequired, bits);
    }

    // We store patch on a byte, so we can't decrease bits by more than 8
    final int minBits = Math.max(0, maxBitsRequired - 8);
    int cumulativeExceptions = 0;
    int patchedBitsRequired = maxBitsRequired;
    int numExceptions = 0;

    for (int b = maxBitsRequired; b >= minBits; --b) {
      if (cumulativeExceptions > MAX_EXCEPTIONS) {
        break;
      }
      patchedBitsRequired = b;
      numExceptions = cumulativeExceptions;
      cumulativeExceptions += histogram[b];
    }

    final int maxUnpatchedValue = (1 << patchedBitsRequired) - 1;
    final byte[] exceptions = new byte[numExceptions * 2];
    if (numExceptions > 0) {
      int exceptionCount = 0;
      for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
        if (ints[i] > maxUnpatchedValue) {
          exceptions[exceptionCount * 2] = (byte) i;
          exceptions[exceptionCount * 2 + 1] = (byte) (ints[i] >>> patchedBitsRequired);
          ints[i] &= maxUnpatchedValue;
          exceptionCount++;
        }
      }
      assert exceptionCount == numExceptions : exceptionCount + " " + numExceptions;
    }

    if (allEqual(ints) && maxBitsRequired <= 8) {
      for (int i = 0; i < numExceptions; ++i) {
        exceptions[2 * i + 1] =
            (byte) (Byte.toUnsignedInt(exceptions[2 * i + 1]) << patchedBitsRequired);
      }
      out.writeByte((byte) (numExceptions << 5));
      out.writeVInt(ints[0]);
    } else {
      final int token = (numExceptions << 5) | patchedBitsRequired;
      out.writeByte((byte) token);
      forUtil.encode(ints, patchedBitsRequired, out);
    }
    out.writeBytes(exceptions, exceptions.length);
  }

  /** Decode 128 integers into {@code ints}. */
  void decode(PostingDecodingUtil pdu, int[] ints) throws IOException {
    var in = pdu.in;
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    if (bitsPerValue == 0) {
      Arrays.fill(ints, 0, ForUtil.BLOCK_SIZE, in.readVInt());
    } else {
      forUtil.decode(bitsPerValue, pdu, ints);
    }
    final int numExceptions = token >>> 5;
    for (int i = 0; i < numExceptions; ++i) {
      ints[Byte.toUnsignedInt(in.readByte())] |= Byte.toUnsignedInt(in.readByte()) << bitsPerValue;
    }
  }

  /** Skip 128 integers. */
  static void skip(DataInput in) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (bitsPerValue == 0) {
      in.readVLong();
      in.skipBytes((numExceptions << 1));
    } else {
      in.skipBytes(ForUtil.numBytes(bitsPerValue) + (numExceptions << 1));
    }
  }
}
