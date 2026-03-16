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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Vector-API implementation of {@link VectorizedBitSetOps.VectorHelper}.
 *
 * <p>This class is located in Lucene's Java 25+ sources so the base classes do not link against
 * incubator Vector API types unless the runtime supports them.
 */
final class VectorizedBitSetOpsHelper implements VectorizedBitSetOps.VectorHelper {

  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int VL = SPECIES.length();

  @Override
  public boolean isVectorized() {
    return true;
  }

  @Override
  public long popCount(long[] a, int start, int len) {
    int i = 0;
    final int loopBound = SPECIES.loopBound(len);
    LongVector acc = LongVector.zero(SPECIES);
    for (; i < loopBound; i += VL) {
      acc = acc.add(LongVector.fromArray(SPECIES, a, start + i).lanewise(VectorOperators.BIT_COUNT));
    }
    long count = acc.reduceLanes(VectorOperators.ADD);
    for (; i < len; i++) {
      count += Long.bitCount(a[start + i]);
    }
    return count;
  }

  @Override
  public long intersectionCount(long[] a, long[] b, int len) {
    int i = 0;
    final int loopBound = SPECIES.loopBound(len);
    LongVector acc = LongVector.zero(SPECIES);
    for (; i < loopBound; i += VL) {
      LongVector va = LongVector.fromArray(SPECIES, a, i);
      LongVector vb = LongVector.fromArray(SPECIES, b, i);
      acc = acc.add(va.and(vb).lanewise(VectorOperators.BIT_COUNT));
    }
    long count = acc.reduceLanes(VectorOperators.ADD);
    for (; i < len; i++) {
      count += Long.bitCount(a[i] & b[i]);
    }
    return count;
  }

  @Override
  public long unionCount(long[] a, long[] b, int len) {
    int i = 0;
    final int loopBound = SPECIES.loopBound(len);
    LongVector acc = LongVector.zero(SPECIES);
    for (; i < loopBound; i += VL) {
      LongVector va = LongVector.fromArray(SPECIES, a, i);
      LongVector vb = LongVector.fromArray(SPECIES, b, i);
      acc = acc.add(va.or(vb).lanewise(VectorOperators.BIT_COUNT));
    }
    long count = acc.reduceLanes(VectorOperators.ADD);
    for (; i < len; i++) {
      count += Long.bitCount(a[i] | b[i]);
    }
    return count;
  }

  @Override
  public long andNotCount(long[] a, long[] b, int len) {
    int i = 0;
    final int loopBound = SPECIES.loopBound(len);
    LongVector acc = LongVector.zero(SPECIES);
    for (; i < loopBound; i += VL) {
      LongVector va = LongVector.fromArray(SPECIES, a, i);
      LongVector vb = LongVector.fromArray(SPECIES, b, i);
      acc = acc.add(va.and(vb.not()).lanewise(VectorOperators.BIT_COUNT));
    }
    long count = acc.reduceLanes(VectorOperators.ADD);
    for (; i < len; i++) {
      count += Long.bitCount(a[i] & ~b[i]);
    }
    return count;
  }
}

