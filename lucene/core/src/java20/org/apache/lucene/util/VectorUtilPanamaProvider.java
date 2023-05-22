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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/** A VectorUtil provider implementation that leverages the Panama Vector API. */
final class VectorUtilPanamaProvider implements VectorUtilProvider {

  static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  VectorUtilPanamaProvider() {}

  @Override
  public float dotProduct(float[] a, float[] b) {
    int i = 0;
    float res = 0;
    // if the array size is large (> 2x platform vector size), its worth the overhead to vectorize
    if (a.length > 2 * SPECIES.length()) {
      // vector loop is unrolled 4x (4 accumulators in parallel)
      FloatVector acc1 = FloatVector.zero(SPECIES);
      FloatVector acc2 = FloatVector.zero(SPECIES);
      FloatVector acc3 = FloatVector.zero(SPECIES);
      FloatVector acc4 = FloatVector.zero(SPECIES);
      int upperBound = SPECIES.loopBound(a.length - 3 * SPECIES.length());
      for (; i < upperBound; i += 4 * SPECIES.length()) {
        FloatVector va = FloatVector.fromArray(SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
        acc1 = acc1.add(va.mul(vb));
        FloatVector vc = FloatVector.fromArray(SPECIES, a, i + SPECIES.length());
        FloatVector vd = FloatVector.fromArray(SPECIES, b, i + SPECIES.length());
        acc2 = acc2.add(vc.mul(vd));
        FloatVector ve = FloatVector.fromArray(SPECIES, a, i + 2 * SPECIES.length());
        FloatVector vf = FloatVector.fromArray(SPECIES, b, i + 2 * SPECIES.length());
        acc3 = acc3.add(ve.mul(vf));
        FloatVector vg = FloatVector.fromArray(SPECIES, a, i + 3 * SPECIES.length());
        FloatVector vh = FloatVector.fromArray(SPECIES, b, i + 3 * SPECIES.length());
        acc4 = acc4.add(vg.mul(vh));
      }
      // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
      upperBound = SPECIES.loopBound(a.length);
      for (; i < upperBound; i += SPECIES.length()) {
        FloatVector va = FloatVector.fromArray(SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
        acc1 = acc1.add(va.mul(vb));
      }
      // reduce
      FloatVector res1 = acc1.add(acc2);
      FloatVector res2 = acc3.add(acc4);
      res += res1.add(res2).reduceLanes(VectorOperators.ADD);
    }

    for (; i < a.length; i++) {
      res += b[i] * a[i];
    }
    return res;
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    int i = 0;
    int res = 0;
    final int vectorSize = IntVector.SPECIES_PREFERRED.vectorBitSize();
    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors
    if (a.length >= ByteVector.SPECIES_64.length() && vectorSize >= 128) {
      // compute vectorized dot product consistent with VPDPBUSD instruction, acts like:
      // int sum = 0;
      // for (...) {
      //   short product = (short) (x[i] * y[i]);
      //   sum += product;
      // }
      if (vectorSize >= 256) {
        // optimized 256 bit implementation, processes 8 bytes at a time
        int upperBound = ByteVector.SPECIES_64.loopBound(a.length);
        IntVector acc = IntVector.zero(IntVector.SPECIES_256);
        for (; i < upperBound; i += ByteVector.SPECIES_64.length()) {
          ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
          ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);
          Vector<Short> va16 = va8.convertShape(VectorOperators.B2S, ShortVector.SPECIES_128, 0);
          Vector<Short> vb16 = vb8.convertShape(VectorOperators.B2S, ShortVector.SPECIES_128, 0);
          Vector<Short> prod16 = va16.mul(vb16);
          Vector<Integer> prod32 =
              prod16.convertShape(VectorOperators.S2I, IntVector.SPECIES_256, 0);
          acc = acc.add(prod32);
        }
        // reduce
        res += acc.reduceLanes(VectorOperators.ADD);
      } else {
        // 128-bit implementation
        // generic implementation, which must "split up" vectors due to widening conversions
        int upperBound = ByteVector.SPECIES_64.loopBound(a.length);
        IntVector acc1 = IntVector.zero(IntVector.SPECIES_PREFERRED);
        IntVector acc2 = IntVector.zero(IntVector.SPECIES_PREFERRED);
        for (; i < upperBound; i += ByteVector.SPECIES_64.length()) {
          ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
          ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);
          // split each byte vector into two short vectors and multiply
          Vector<Short> va16 = va8.convertShape(VectorOperators.B2S, ShortVector.SPECIES_128, 0);
          Vector<Short> vb16 = vb8.convertShape(VectorOperators.B2S, ShortVector.SPECIES_128, 0);
          Vector<Short> prod16 = va16.mul(vb16);
          // split each short vector into two int vectors and add
          Vector<Integer> prod32_1 =
              prod16.convertShape(VectorOperators.S2I, IntVector.SPECIES_128, 0);
          Vector<Integer> prod32_2 =
              prod16.convertShape(VectorOperators.S2I, IntVector.SPECIES_128, 1);
          acc1 = acc1.add(prod32_1);
          acc2 = acc2.add(prod32_2);
        }
        // reduce
        res += acc1.add(acc2).reduceLanes(VectorOperators.ADD);
      }
    }

    for (; i < a.length; i++) {
      res += b[i] * a[i];
    }
    return res;
  }
}
