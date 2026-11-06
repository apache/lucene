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
package org.apache.lucene.codecs.fuse;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Allocation-free 64-bit term hash for binary fuse filters.
 *
 * <p>Returns {@code h[0] ^ Long.rotateLeft(h[1], 32)} where {@code h} is {@link
 * org.apache.lucene.util.StringHelper#murmurhash3_x64_128(BytesRef)}, but without allocating the
 * {@code long[2]} result: the filter is probed on every {@code seekExact}, once per segment, and
 * that allocation is not reliably removed by escape analysis.
 */
final class FuseHash {
  private static final long SEED = 104729L; // same seed as StringHelper.murmurhash3_x64_128
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  private FuseHash() {}

  static long hash(BytesRef term) {
    return hash(term.bytes, term.offset, term.length);
  }

  @SuppressWarnings("fallthrough")
  static long hash(byte[] data, int offset, int length) {
    long h1 = SEED;
    long h2 = SEED;
    final int nblocks = length >> 4;
    for (int i = 0; i < nblocks; i++) {
      final int index = offset + (i << 4);
      long k1 = (long) BitUtil.VH_LE_LONG.get(data, index);
      long k2 = (long) BitUtil.VH_LE_LONG.get(data, index + 8);
      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;
      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }
    long k1 = 0;
    long k2 = 0;
    final int index = offset + (nblocks << 4);
    switch (length & 0x0F) {
      case 15:
        k2 ^= ((long) data[index + 14] & 0xff) << 48;
      case 14:
        k2 ^= ((long) data[index + 13] & 0xff) << 40;
      case 13:
        k2 ^= ((long) data[index + 12] & 0xff) << 32;
      case 12:
        k2 ^= ((long) data[index + 11] & 0xff) << 24;
      case 11:
        k2 ^= ((long) data[index + 10] & 0xff) << 16;
      case 10:
        k2 ^= ((long) data[index + 9] & 0xff) << 8;
      case 9:
        k2 ^= data[index + 8] & 0xff;
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= C1;
        h2 ^= k2;
      case 8:
        k1 ^= ((long) data[index + 7] & 0xff) << 56;
      case 7:
        k1 ^= ((long) data[index + 6] & 0xff) << 48;
      case 6:
        k1 ^= ((long) data[index + 5] & 0xff) << 40;
      case 5:
        k1 ^= ((long) data[index + 4] & 0xff) << 32;
      case 4:
        k1 ^= ((long) data[index + 3] & 0xff) << 24;
      case 3:
        k1 ^= ((long) data[index + 2] & 0xff) << 16;
      case 2:
        k1 ^= ((long) data[index + 1] & 0xff) << 8;
      case 1:
        k1 ^= data[index] & 0xff;
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= C2;
        h1 ^= k1;
    }
    h1 ^= length;
    h2 ^= length;
    h1 += h2;
    h2 += h1;
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 += h2;
    h2 += h1;
    return h1 ^ Long.rotateLeft(h2, 32);
  }

  private static long fmix64(long h) {
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }
}
