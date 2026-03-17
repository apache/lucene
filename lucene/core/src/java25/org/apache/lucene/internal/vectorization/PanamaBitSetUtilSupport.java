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
package org.apache.lucene.internal.vectorization;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/** {@link BitSetUtilSupport} implementation using the Panama Vector API. */
final class PanamaBitSetUtilSupport implements BitSetUtilSupport {

  private static final VectorSpecies<Long> LONG_SPECIES =
      VectorSpecies.of(
          long.class, VectorShape.forBitSize(PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE));
  private static final int VL = LONG_SPECIES.length();

  /**
   * Minimum number of longs (words) before using SIMD; below this the dispatch overhead isn't worth
   * it.
   */
  private static final int MIN_WORDS = 64; // 4096 bits

  @Override
  public long popCount(long[] a, int start, int len) {
    if (len < MIN_WORDS) {
      long count = 0;
      for (int i = 0; i < len; i++) {
        count += Long.bitCount(a[start + i]);
      }
      return count;
    }
    int i = 0;
    final int loopBound = LONG_SPECIES.loopBound(len);
    LongVector acc = LongVector.zero(LONG_SPECIES);
    for (; i < loopBound; i += VL) {
      acc =
          acc.add(
              LongVector.fromArray(LONG_SPECIES, a, start + i).lanewise(VectorOperators.BIT_COUNT));
    }
    long count = acc.reduceLanes(VectorOperators.ADD);
    for (; i < len; i++) {
      count += Long.bitCount(a[start + i]);
    }
    return count;
  }
}
