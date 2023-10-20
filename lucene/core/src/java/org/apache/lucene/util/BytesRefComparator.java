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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Specialized {@link BytesRef} comparator that {@link StringSorter} has optimizations for.
 *
 * @lucene.internal
 */
public abstract class BytesRefComparator implements Comparator<BytesRef> {

  /** Comparing ByteRefs in natual order. */
  public static final BytesRefComparator NATURAL =
      new BytesRefComparator(Integer.MAX_VALUE) {
        @Override
        protected int byteAt(BytesRef ref, int i) {
          if (ref.length <= i) {
            return -1;
          }
          return ref.bytes[ref.offset + i] & 0xFF;
        }

        @Override
        public int compare(BytesRef o1, BytesRef o2, int k) {
          return Arrays.compareUnsigned(
              o1.bytes,
              o1.offset + k,
              o1.offset + o1.length,
              o2.bytes,
              o2.offset + k,
              o2.offset + o2.length);
        }
      };

  final int comparedBytesCount;

  /**
   * Sole constructor.
   *
   * @param comparedBytesCount the maximum number of bytes to compare.
   */
  protected BytesRefComparator(int comparedBytesCount) {
    this.comparedBytesCount = comparedBytesCount;
  }

  /**
   * Return the unsigned byte to use for comparison at index {@code i}, or {@code -1} if all bytes
   * that are useful for comparisons are exhausted. This may only be called with a value of {@code
   * i} between {@code 0} included and {@code comparedBytesCount} excluded.
   */
  protected abstract int byteAt(BytesRef ref, int i);

  @Override
  public final int compare(BytesRef o1, BytesRef o2) {
    return compare(o1, o2, 0);
  }

  /** Compare two bytes refs that first k bytes are already guaranteed to be equal. */
  public int compare(BytesRef o1, BytesRef o2, int k) {
    for (int i = k; i < comparedBytesCount; ++i) {
      final int b1 = byteAt(o1, i);
      final int b2 = byteAt(o2, i);
      if (b1 != b2) {
        return b1 - b2;
      } else if (b1 == -1) {
        break;
      }
    }
    return 0;
  }
}
