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

package org.apache.lucene.codecs.dedup;

import org.apache.lucene.util.StringHelper;

/**
 * 64-bit byte-array hash used by {@link DedupFlatVectorsWriter}'s dedup hash table.
 *
 * <p>Implementation: takes the first {@code long} of {@link
 * StringHelper#murmurhash3_x64_128(byte[], int, int, int)} for speed and quality. {@link #EMPTY} (=
 * 0) is reserved as a "slot empty" sentinel by the open-addressed table — when the hash
 * legitimately equals 0 we substitute it with {@link #SUBSTITUTE_FOR_EMPTY}.
 */
final class DedupHash {

  /** Slot-empty sentinel used by the hash table. */
  static final long EMPTY = 0L;

  /** Substitute hash returned in place of {@link #EMPTY} so callers never see a zero hash. */
  private static final long SUBSTITUTE_FOR_EMPTY = 0xDEDDEADBEEFCAFE0L;

  private DedupHash() {}

  /**
   * Compute a 64-bit hash over {@code bytes[0..len)}. Always returns a value distinct from {@link
   * #EMPTY}.
   */
  static long hash(byte[] bytes, int len) {
    long[] h128 = StringHelper.murmurhash3_x64_128(bytes, 0, len, 0);
    long h = h128[0];
    if (h == EMPTY) {
      // Vanishingly unlikely (1 in 2^64); but if it happens we don't want it confused with
      // "slot empty". Use a fixed substitute that itself can never match a legitimate murmur
      // output for a different vector with non-trivial probability.
      return SUBSTITUTE_FOR_EMPTY;
    }
    return h;
  }
}
