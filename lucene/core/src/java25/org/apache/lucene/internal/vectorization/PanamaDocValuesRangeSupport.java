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

  @Override
  public void sortedNumericRangeIntoBitSet(
      LongValues values,
      int fromDoc,
      int toDoc,
      int cardinality,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset) {
    assert cardinality > 0 : "cardinality must be positive: " + cardinality;
    final int vectorLen = LONG_SPECIES.length();
    final int docsPerVector = vectorLen / cardinality;
    if (docsPerVector == 0 || vectorLen % cardinality != 0) {
      DocValuesRangeSupport.super.sortedNumericRangeIntoBitSet(
          values, fromDoc, toDoc, cardinality, minValue, maxValue, bitSet, offset);
      return;
    }

    final long[] scratch = new long[vectorLen];
    final int vectorDocEnd = fromDoc + (toDoc - fromDoc) / docsPerVector * docsPerVector;
    int doc = fromDoc;
    for (; doc < vectorDocEnd; doc += docsPerVector) {
      long valueOffset = (long) doc * cardinality;
      for (int lane = 0; lane < vectorLen; lane++) {
        scratch[lane] = values.get(valueOffset + lane);
      }
      LongVector vector = LongVector.fromArray(LONG_SPECIES, scratch, 0);
      // Flat range check on all lanes. Equivalent to the scalar early-break on sorted values:
      // a value outside [min,max] that is >= min must be > max, so its lane stays unset, and
      // collapseToDocMask OR-reduces lanes per doc to match "any value in range" semantics.
      VectorMask<Long> inRange =
          vector
              .compare(VectorOperators.GE, minValue)
              .and(vector.compare(VectorOperators.LE, maxValue));
      long docMask = collapseToDocMask(inRange.toLong(), cardinality, docsPerVector);
      if (docMask != 0) {
        bitSet.orMask(doc - offset, docMask, docsPerVector);
      }
    }

    DocValuesRangeSupport.super.sortedNumericRangeIntoBitSet(
        values, doc, toDoc, cardinality, minValue, maxValue, bitSet, offset);
  }

  private static long collapseToDocMask(long valueMask, int cardinality, int docsPerVector) {
    final long valueMaskPerDoc = (1L << cardinality) - 1L;
    long docMask = 0L;
    for (int d = 0; d < docsPerVector; d++) {
      long perDoc = (valueMask >>> (d * cardinality)) & valueMaskPerDoc;
      if (perDoc != 0) {
        docMask |= 1L << d;
      }
    }
    return docMask;
  }
}
