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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;

/** Panama Vector API implementation of {@link DocValuesRangeSupport}. */
final class PanamaDocValuesRangeSupport implements DocValuesRangeSupport {

  static final PanamaDocValuesRangeSupport INSTANCE = new PanamaDocValuesRangeSupport();

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

  private PanamaDocValuesRangeSupport() {}

  @Override
  public void rangeIntoBitSet(
      LongValues values,
      int fromDoc,
      int toDoc,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset) {
    final int vectorLen = LONG_SPECIES.length();

    // Scratch buffer for loading values before SIMD comparison
    final long[] scratch = new long[vectorLen];
    final int loopBound = fromDoc + LONG_SPECIES.loopBound(toDoc - fromDoc);

    // SIMD loop: load vectorLen values into scratch, compare all at once
    for (int d = fromDoc; d < loopBound; d += vectorLen) {
      for (int i = 0; i < vectorLen; i++) {
        scratch[i] = values.get(d + i);
      }
      LongVector v = LongVector.fromArray(LONG_SPECIES, scratch, 0);
      VectorMask<Long> inRange =
          v.compare(VectorOperators.GE, minValue).and(v.compare(VectorOperators.LE, maxValue));
      long maskBits = inRange.toLong();
      if (maskBits != 0) {
        bitSet.orMask(d - offset, maskBits, vectorLen);
      }
    }

    // Scalar tail for remaining docs
    for (int d = loopBound; d < toDoc; d++) {
      long v = values.get(d);
      if (v >= minValue && v <= maxValue) {
        bitSet.set(d - offset);
      }
    }
  }
}
