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
package org.apache.lucene.codecs.bloom;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based lookup. See
 * http://murmurhash.googlepages.com/ for more details.
 *
 * <p>The code from Apache Commons was adapted in the form here to work with BytesRefs with offsets
 * and lengths rather than raw byte arrays.
 */
public class MurmurHash64 extends HashFunction {
  private static final long M64 = 0xc6a4a7935bd1e995L;
  private static final int R64 = 47;
  public static final HashFunction INSTANCE = new MurmurHash64();

  /**
   * Generates a 64-bit hash from byte array of the given length and seed.
   *
   * @param data The input byte array
   * @param seed The initial seed value
   * @param length The length of the array
   * @return The 64-bit hash of the given array
   */
  public static long hash64(byte[] data, int seed, int offset, int length) {
    long h = (seed & 0xffffffffL) ^ (length * M64);

    final int nblocks = length >> 3;

    // body
    for (int i = 0; i < nblocks; i++) {

      long k = (long) BitUtil.VH_LE_LONG.get(data, offset);
      k *= M64;
      k ^= k >>> R64;
      k *= M64;

      h ^= k;
      h *= M64;

      offset += Long.BYTES;
    }

    int remaining = length & 0x07;
    if (0 < remaining) {
      for (int i = 0; i < remaining; i++) {
        h ^= ((long) data[offset + i] & 0xff) << (Byte.SIZE * i);
      }
      h *= M64;
    }

    h ^= h >>> R64;
    h *= M64;
    h ^= h >>> R64;

    return h;
  }

  @Override
  public final long hash(BytesRef br) {
    return hash64(br.bytes, 0xe17a1465, br.offset, br.length);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
