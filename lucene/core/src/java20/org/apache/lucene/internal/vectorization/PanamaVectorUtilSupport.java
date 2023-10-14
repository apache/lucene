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

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.S2I;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

final class PanamaVectorUtilSupport implements VectorUtilSupport {

  private static final int INT_SPECIES_PREF_BIT_SIZE = IntVector.SPECIES_PREFERRED.vectorBitSize();

  // we always use the platform's maximum floating point vector size
  private static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;

  // for integer methods, it is more complicated due to conversions
  private static final VectorSpecies<Byte> BYTE_SPECIES;
  private static final VectorSpecies<Short> SHORT_SPECIES;

  // compute BYTE/SHORT sizes relative to preferred integer vector size
  static {
    if (INT_SPECIES_PREF_BIT_SIZE >= 256) {
      BYTE_SPECIES =
          ByteVector.SPECIES_MAX.withShape(
              VectorShape.forBitSize(IntVector.SPECIES_PREFERRED.vectorBitSize() >> 2));
      SHORT_SPECIES =
          ShortVector.SPECIES_MAX.withShape(
              VectorShape.forBitSize(IntVector.SPECIES_PREFERRED.vectorBitSize() >> 1));
    } else {
      BYTE_SPECIES = null;
      SHORT_SPECIES = null;
    }
  }

  private final boolean useIntegerVectors;

  PanamaVectorUtilSupport(boolean useIntegerVectors) {
    this.useIntegerVectors = useIntegerVectors;
  }

  @Override
  public float dotProduct(float[] a, float[] b) {
    int i = 0;
    float res = 0;

    // if the array size is large (> 2x platform vector size), its worth the overhead to vectorize
    if (a.length > 2 * FLOAT_SPECIES.length()) {
      i += FLOAT_SPECIES.loopBound(a.length);
      res += dotProductBody(a, b, i);
    }

    // scalar tail
    for (; i < a.length; i++) {
      res += b[i] * a[i];
    }
    return res;
  }

  /** vectorized float dot product body */
  private float dotProductBody(float[] a, float[] b, int limit) {
    int i = 0;
    // vector loop is unrolled 4x (4 accumulators in parallel)
    // we don't know how many the cpu can do at once, some can do 2, some 4
    FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc4 = FloatVector.zero(FLOAT_SPECIES);
    int unrolledLimit = limit - 3 * FLOAT_SPECIES.length();
    for (; i < unrolledLimit; i += 4 * FLOAT_SPECIES.length()) {
      // one
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      acc1 = acc1.add(va.mul(vb));

      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
      acc2 = acc2.add(vc.mul(vd));

      // three
      FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
      FloatVector vf = FloatVector.fromArray(FLOAT_SPECIES, b, i + 2 * FLOAT_SPECIES.length());
      acc3 = acc3.add(ve.mul(vf));

      // four
      FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
      FloatVector vh = FloatVector.fromArray(FLOAT_SPECIES, b, i + 3 * FLOAT_SPECIES.length());
      acc4 = acc4.add(vg.mul(vh));
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      acc1 = acc1.add(va.mul(vb));
    }
    // reduce
    FloatVector res1 = acc1.add(acc2);
    FloatVector res2 = acc3.add(acc4);
    return res1.add(res2).reduceLanes(ADD);
  }

  @Override
  public float cosine(float[] a, float[] b) {
    int i = 0;
    float sum = 0;
    float norm1 = 0;
    float norm2 = 0;
    // if the array size is large (> 2x platform vector size), its worth the overhead to vectorize
    if (a.length > 2 * FLOAT_SPECIES.length()) {
      // vector loop is unrolled 4x (4 accumulators in parallel)
      // we don't know how many the cpu can do at once, some can do 2, some 4
      FloatVector sum1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sum2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sum3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector sum4 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm1_1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm1_2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm1_3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm1_4 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm2_1 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm2_2 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm2_3 = FloatVector.zero(FLOAT_SPECIES);
      FloatVector norm2_4 = FloatVector.zero(FLOAT_SPECIES);
      int upperBound = FLOAT_SPECIES.loopBound(a.length - 3 * FLOAT_SPECIES.length());
      for (; i < upperBound; i += 4 * FLOAT_SPECIES.length()) {
        // one
        FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
        sum1 = sum1.add(va.mul(vb));
        norm1_1 = norm1_1.add(va.mul(va));
        norm2_1 = norm2_1.add(vb.mul(vb));

        // two
        FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
        FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
        sum2 = sum2.add(vc.mul(vd));
        norm1_2 = norm1_2.add(vc.mul(vc));
        norm2_2 = norm2_2.add(vd.mul(vd));

        // three
        FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
        FloatVector vf = FloatVector.fromArray(FLOAT_SPECIES, b, i + 2 * FLOAT_SPECIES.length());
        sum3 = sum3.add(ve.mul(vf));
        norm1_3 = norm1_3.add(ve.mul(ve));
        norm2_3 = norm2_3.add(vf.mul(vf));

        // four
        FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
        FloatVector vh = FloatVector.fromArray(FLOAT_SPECIES, b, i + 3 * FLOAT_SPECIES.length());
        sum4 = sum4.add(vg.mul(vh));
        norm1_4 = norm1_4.add(vg.mul(vg));
        norm2_4 = norm2_4.add(vh.mul(vh));
      }
      // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
      upperBound = FLOAT_SPECIES.loopBound(a.length);
      for (; i < upperBound; i += FLOAT_SPECIES.length()) {
        FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
        sum1 = sum1.add(va.mul(vb));
        norm1_1 = norm1_1.add(va.mul(va));
        norm2_1 = norm2_1.add(vb.mul(vb));
      }
      // reduce
      FloatVector sumres1 = sum1.add(sum2);
      FloatVector sumres2 = sum3.add(sum4);
      FloatVector norm1res1 = norm1_1.add(norm1_2);
      FloatVector norm1res2 = norm1_3.add(norm1_4);
      FloatVector norm2res1 = norm2_1.add(norm2_2);
      FloatVector norm2res2 = norm2_3.add(norm2_4);
      sum += sumres1.add(sumres2).reduceLanes(ADD);
      norm1 += norm1res1.add(norm1res2).reduceLanes(ADD);
      norm2 += norm2res1.add(norm2res2).reduceLanes(ADD);
    }

    // scalar tail
    for (; i < a.length; i++) {
      float elem1 = a[i];
      float elem2 = b[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    int i = 0;
    float res = 0;

    // if the array size is large (> 2x platform vector size), its worth the overhead to vectorize
    if (a.length > 2 * FLOAT_SPECIES.length()) {
      i += FLOAT_SPECIES.loopBound(a.length);
      res += squareDistanceBody(a, b, i);
    }

    // scalar tail
    for (; i < a.length; i++) {
      float diff = a[i] - b[i];
      res += diff * diff;
    }
    return res;
  }

  /** vectorized square distance body */
  private float squareDistanceBody(float[] a, float[] b, int limit) {
    int i = 0;
    // vector loop is unrolled 4x (4 accumulators in parallel)
    // we don't know how many the cpu can do at once, some can do 2, some 4
    FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc4 = FloatVector.zero(FLOAT_SPECIES);
    int unrolledLimit = limit - 3 * FLOAT_SPECIES.length();
    for (; i < unrolledLimit; i += 4 * FLOAT_SPECIES.length()) {
      // one
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      FloatVector diff1 = va.sub(vb);
      acc1 = acc1.add(diff1.mul(diff1));

      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
      FloatVector diff2 = vc.sub(vd);
      acc2 = acc2.add(diff2.mul(diff2));

      // three
      FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
      FloatVector vf = FloatVector.fromArray(FLOAT_SPECIES, b, i + 2 * FLOAT_SPECIES.length());
      FloatVector diff3 = ve.sub(vf);
      acc3 = acc3.add(diff3.mul(diff3));

      // four
      FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
      FloatVector vh = FloatVector.fromArray(FLOAT_SPECIES, b, i + 3 * FLOAT_SPECIES.length());
      FloatVector diff4 = vg.sub(vh);
      acc4 = acc4.add(diff4.mul(diff4));
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      FloatVector diff = va.sub(vb);
      acc1 = acc1.add(diff.mul(diff));
    }
    // reduce
    FloatVector res1 = acc1.add(acc2);
    FloatVector res2 = acc3.add(acc4);
    return res1.add(res2).reduceLanes(ADD);
  }

  // Binary functions, these all follow a general pattern like this:
  //
  //   short intermediate = a * b;
  //   int accumulator = (int)accumulator + (int)intermediate;
  //
  // 256 or 512 bit vectors can process 64 or 128 bits at a time, respectively
  // intermediate results use 128 or 256 bit vectors, respectively
  // final accumulator uses 256 or 512 bit vectors, respectively
  //
  // We also support 128 bit vectors, going 32 bits at a time.
  // This is slower but still faster than not vectorizing at all.

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    int i = 0;
    int res = 0;

    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors (256-bit on intel to dodge performance landmines)
    if (a.length >= 16 && useIntegerVectors) {
      // compute vectorized dot product consistent with VPDPBUSD instruction
      if (INT_SPECIES_PREF_BIT_SIZE >= 256) {
        i += BYTE_SPECIES.loopBound(a.length);
        res += dotProductBody256(a, b, i);
      } else {
        // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
        i += ByteVector.SPECIES_64.loopBound(a.length - ByteVector.SPECIES_64.length());
        res += dotProductBody128(a, b, i);
      }
    }

    // scalar tail
    for (; i < a.length; i++) {
      res += b[i] * a[i];
    }
    return res;
  }

  /** vectorized dot product body (256+ bit vectors) */
  private int dotProductBody256(byte[] a, byte[] b, int limit) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_PREFERRED);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
      ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

      // 16-bit multiply
      Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> prod16 = va16.mul(vb16);

      // 32-bit add
      Vector<Integer> prod32 = prod16.convertShape(S2I, IntVector.SPECIES_PREFERRED, 0);
      acc = acc.add(prod32);
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized dot product body (128 bit vectors) */
  private int dotProductBody128(byte[] a, byte[] b, int limit) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_128);
    // 4 bytes at a time (re-loading half the vector each time!)
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
      // load 8 bytes
      ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
      ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);

      // process first "half" only: 16-bit multiply
      Vector<Short> va16 = va8.convert(B2S, 0);
      Vector<Short> vb16 = vb8.convert(B2S, 0);
      Vector<Short> prod16 = va16.mul(vb16);

      // 32-bit add
      acc = acc.add(prod16.convertShape(S2I, IntVector.SPECIES_128, 0));
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    int i = 0;
    int sum = 0;
    int norm1 = 0;
    int norm2 = 0;
    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors (256-bit on intel to dodge performance landmines)
    if (a.length >= 16 && useIntegerVectors) {
      if (INT_SPECIES_PREF_BIT_SIZE >= 256) {
        // optimized 256/512 bit implementation, processes 8/16 bytes at a time
        int upperBound = BYTE_SPECIES.loopBound(a.length);
        IntVector accSum = IntVector.zero(IntVector.SPECIES_PREFERRED);
        IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_PREFERRED);
        IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_PREFERRED);
        for (; i < upperBound; i += BYTE_SPECIES.length()) {
          ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
          ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

          // 16-bit multiply
          Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES, 0);
          Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES, 0);
          Vector<Short> prod16 = va16.mul(vb16);
          Vector<Short> norm1_16 = va16.mul(va16);
          Vector<Short> norm2_16 = vb16.mul(vb16);

          // sum into accumulators: 32-bit add
          Vector<Integer> prod32 = prod16.convertShape(S2I, IntVector.SPECIES_PREFERRED, 0);
          Vector<Integer> norm1_32 = norm1_16.convertShape(S2I, IntVector.SPECIES_PREFERRED, 0);
          Vector<Integer> norm2_32 = norm2_16.convertShape(S2I, IntVector.SPECIES_PREFERRED, 0);
          accSum = accSum.add(prod32);
          accNorm1 = accNorm1.add(norm1_32);
          accNorm2 = accNorm2.add(norm2_32);
        }
        // reduce
        sum += accSum.reduceLanes(ADD);
        norm1 += accNorm1.reduceLanes(ADD);
        norm2 += accNorm2.reduceLanes(ADD);
      } else {
        // 128-bit impl, which is tricky since we don't have SPECIES_32, it does "overlapping read"
        int upperBound = ByteVector.SPECIES_64.loopBound(a.length - ByteVector.SPECIES_64.length());
        IntVector accSum = IntVector.zero(IntVector.SPECIES_128);
        IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_128);
        IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_128);
        for (; i < upperBound; i += ByteVector.SPECIES_64.length() >> 1) {
          ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
          ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);

          // process first half only: 16-bit multiply
          Vector<Short> va16 = va8.convert(B2S, 0);
          Vector<Short> vb16 = vb8.convert(B2S, 0);
          Vector<Short> norm1_16 = va16.mul(va16);
          Vector<Short> norm2_16 = vb16.mul(vb16);
          Vector<Short> prod16 = va16.mul(vb16);

          // sum into accumulators: 32-bit add
          accNorm1 = accNorm1.add(norm1_16.convertShape(S2I, IntVector.SPECIES_128, 0));
          accNorm2 = accNorm2.add(norm2_16.convertShape(S2I, IntVector.SPECIES_128, 0));
          accSum = accSum.add(prod16.convertShape(S2I, IntVector.SPECIES_128, 0));
        }
        // reduce
        sum += accSum.reduceLanes(ADD);
        norm1 += accNorm1.reduceLanes(ADD);
        norm2 += accNorm2.reduceLanes(ADD);
      }
    }

    // scalar tail
    for (; i < a.length; i++) {
      byte elem1 = a[i];
      byte elem2 = b[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    int i = 0;
    int res = 0;

    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors (256-bit on intel to dodge performance landmines)
    if (a.length >= 16 && useIntegerVectors) {
      if (INT_SPECIES_PREF_BIT_SIZE >= 256) {
        i += BYTE_SPECIES.loopBound(a.length);
        res += squareDistanceBody256(a, b, i);
      } else {
        i += ByteVector.SPECIES_64.loopBound(a.length);
        res += squareDistanceBody128(a, b, i);
      }
    }

    // scalar tail
    for (; i < a.length; i++) {
      int diff = a[i] - b[i];
      res += diff * diff;
    }
    return res;
  }

  /** vectorized square distance body (256+ bit vectors) */
  private int squareDistanceBody256(byte[] a, byte[] b, int limit) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_PREFERRED);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
      ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

      // 16-bit sub
      Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> diff16 = va16.sub(vb16);

      // 32-bit multiply and add into accumulators
      Vector<Integer> diff32 = diff16.convertShape(S2I, IntVector.SPECIES_PREFERRED, 0);
      acc = acc.add(diff32.mul(diff32));
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized square distance body (128 bit vectors) */
  private int squareDistanceBody128(byte[] a, byte[] b, int limit) {
    // 128-bit implementation, which must "split up" vectors due to widening conversions
    // it doesn't help to do the overlapping read trick, due to 32-bit multiply in the formula
    IntVector acc1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc2 = IntVector.zero(IntVector.SPECIES_128);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
      ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);

      // 16-bit sub
      Vector<Short> va16 = va8.convertShape(B2S, ShortVector.SPECIES_128, 0);
      Vector<Short> vb16 = vb8.convertShape(B2S, ShortVector.SPECIES_128, 0);
      Vector<Short> diff16 = va16.sub(vb16);

      // 32-bit multiply and add into accumulators
      Vector<Integer> diff32_1 = diff16.convertShape(S2I, IntVector.SPECIES_128, 0);
      Vector<Integer> diff32_2 = diff16.convertShape(S2I, IntVector.SPECIES_128, 1);
      acc1 = acc1.add(diff32_1.mul(diff32_1));
      acc2 = acc2.add(diff32_2.mul(diff32_2));
    }
    // reduce
    return acc1.add(acc2).reduceLanes(ADD);
  }
}
