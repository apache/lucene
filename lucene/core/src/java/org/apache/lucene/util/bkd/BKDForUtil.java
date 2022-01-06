// This file has been automatically generated, DO NOT EDIT
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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
final class BKDForUtil {

  static final int BLOCK_SIZE = 512;
  private static final long MASK32_8 = mask32(8);
  private static final long MASK32_24 = mask32(24);

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static void expand16(long[] arr) {
    for (int i = 0; i < 128; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[128 + i] = (l >>> 32) & 0xFFFFL;
      arr[256 + i] = (l >>> 16) & 0xFFFFL;
      arr[384 + i] = l & 0xFFFFL;
    }
  }

  private static void collapse16(long[] arr) {
    for (int i = 0; i < 128; ++i) {
      arr[i] = (arr[i] << 48) | (arr[128 + i] << 32) | (arr[256 + i] << 16) | arr[384 + i];
    }
  }

  private static void expand32(long[] arr) {
    for (int i = 0; i < 256; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[256 + i] = l & 0xFFFFFFFFL;
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 256; ++i) {
      arr[i] = (arr[i] << 32) | arr[256 + i];
    }
  }

  private final long[] tmp = new long[256];

  void encode16(long[] longs, DataOutput out) throws IOException {
    collapse16(longs);
    for (int i = 0; i < 128; i++) {
      out.writeLong(longs[i]);
    }
  }

  void encode32(long[] longs, DataOutput out) throws IOException {
    collapse32(longs);
    for (int i = 0; i < 256; i++) {
      out.writeLong(longs[i]);
    }
  }

  void encode24(long[] longs, DataOutput out) throws IOException {
    collapse32(longs);
    for (int i = 0; i < 192; ++i) {
      tmp[i] = longs[i] << 8;
    }
    int tmpIdx = 0;
    for (int i = 192; i < 256; i++) {
      tmp[tmpIdx++] |= (longs[i] >>> 16) & MASK32_8;
      tmp[tmpIdx++] |= (longs[i] >>> 8) & MASK32_8;
      tmp[tmpIdx++] |= longs[i] & MASK32_8;
    }
    for (int i = 0; i < 192; ++i) {
      out.writeLong(tmp[i]);
    }
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2 compiler, which
   * generates SIMD instructions for it in order to shift multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[i] = (a[i] >>> shift) & mask;
    }
  }

  void decode16(DataInput in, long[] longs) throws IOException {
    in.readLongs(longs, 0, 128);
    expand16(longs);
  }

  void decode24(DataInput in, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 192);
    shiftLongs(tmp, 192, longs, 8, MASK32_24);
    shiftLongs(tmp, 192, tmp, 0, MASK32_8);
    for (int iter = 0, tmpIdx = 0, longsIdx = 192; iter < 64; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = tmp[tmpIdx] << 16;
      l0 |= tmp[tmpIdx + 1] << 8;
      l0 |= tmp[tmpIdx + 2];
      longs[longsIdx] = l0;
    }
    expand32(longs);
  }

  void decode32(DataInput in, long[] longs) throws IOException {
    in.readLongs(longs, 0, 256);
    expand32(longs);
  }
}
