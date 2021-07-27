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

final class DocValuesForUtil {

  static final int BLOCK_SIZE = Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;

  private final long[] tmp = new long[BLOCK_SIZE];

  void encode(long[] in, int bitsPerValue, DataOutput out) throws IOException {
    final int numValuesPerLong = Long.SIZE / bitsPerValue;
    final int numLongs = 1 + (BLOCK_SIZE - 1) / numValuesPerLong;
    Arrays.fill(tmp, 0, numLongs, 0L);
    int inOffset = 0;
    for (int shift = 0; shift + bitsPerValue <= Long.SIZE; shift += bitsPerValue) {
      for (int i = 0; i < numLongs && inOffset + i < BLOCK_SIZE; ++i) {
        assert bitsPerValue == Long.SIZE || in[inOffset + i] <= ((1L << bitsPerValue) - 1L);
        tmp[i] |= in[inOffset + i] << shift;
      }
      inOffset += numLongs;
    }
    for (int i = 0; i < numLongs; ++i) {
      out.writeLong(tmp[i]);
    }
  }

  void decode(int bitsPerValue, DataInput in, long[] out) throws IOException {
    if (bitsPerValue > 32) {
      in.readLongs(out, 0, BLOCK_SIZE);
      return;
    }
    final int numValuesPerLong = Long.SIZE / bitsPerValue;
    final int numLongs = 1 + (BLOCK_SIZE - 1) / numValuesPerLong;
    in.readLongs(tmp, 0, numLongs);
    final long mask = (1L << bitsPerValue) - 1L;
    int outOffset = 0;
    for (int shift = 0; shift + bitsPerValue <= Long.SIZE; shift += bitsPerValue) {
      shiftLongs(tmp, Math.min(numLongs, BLOCK_SIZE - outOffset), out, outOffset, shift, mask);
      outOffset += numLongs;
    }
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2 compiler, which
   * generates SIMD instructions for it in order to shift multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi + i] = (a[i] >>> shift) & mask;
    }
  }
}
