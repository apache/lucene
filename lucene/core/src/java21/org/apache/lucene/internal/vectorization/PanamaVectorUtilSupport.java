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
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.S2I;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2S;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/**
 * VectorUtil methods implemented with Panama incubating vector API.
 *
 * <p>Supports two system properties for correctness testing purposes only:
 *
 * <ul>
 *   <li>tests.vectorsize (int)
 *   <li>tests.forceintegervectors (boolean)
 * </ul>
 *
 * Setting these properties will make this code run EXTREMELY slow!
 */
final class PanamaVectorUtilSupport implements VectorUtilSupport {

  // preferred vector sizes, which can be altered for testing
  private static final VectorSpecies<Float> FLOAT_SPECIES;
  private static final VectorSpecies<Integer> INT_SPECIES;
  private static final VectorSpecies<Byte> BYTE_SPECIES;
  private static final VectorSpecies<Short> SHORT_SPECIES;

  static final int VECTOR_BITSIZE;
  static final boolean HAS_FAST_INTEGER_VECTORS;

  static {
    // default to platform supported bitsize
    int vectorBitSize = VectorShape.preferredShape().vectorBitSize();
    // but allow easy overriding for testing
    vectorBitSize = VectorizationProvider.TESTS_VECTOR_SIZE.orElse(vectorBitSize);
    INT_SPECIES = VectorSpecies.of(int.class, VectorShape.forBitSize(vectorBitSize));
    VECTOR_BITSIZE = INT_SPECIES.vectorBitSize();
    FLOAT_SPECIES = INT_SPECIES.withLanes(float.class);
    // compute BYTE/SHORT sizes relative to preferred integer vector size
    if (VECTOR_BITSIZE >= 256) {
      BYTE_SPECIES = ByteVector.SPECIES_MAX.withShape(VectorShape.forBitSize(VECTOR_BITSIZE >> 2));
      SHORT_SPECIES =
          ShortVector.SPECIES_MAX.withShape(VectorShape.forBitSize(VECTOR_BITSIZE >> 1));
    } else {
      BYTE_SPECIES = null;
      SHORT_SPECIES = null;
    }
    // hotspot misses some SSE intrinsics, workaround it
    // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
    boolean isAMD64withoutAVX2 = Constants.OS_ARCH.equals("amd64") && VECTOR_BITSIZE < 256;
    HAS_FAST_INTEGER_VECTORS =
        VectorizationProvider.TESTS_FORCE_INTEGER_VECTORS || (isAMD64withoutAVX2 == false);
  }

  // the way FMA should work! if available use it, otherwise fall back to mul/add
  private static FloatVector fma(FloatVector a, FloatVector b, FloatVector c) {
    if (Constants.HAS_FAST_VECTOR_FMA) {
      return a.fma(b, c);
    } else {
      return a.mul(b).add(c);
    }
  }

  @SuppressForbidden(reason = "Uses FMA only where fast and carefully contained")
  private static float fma(float a, float b, float c) {
    if (Constants.HAS_FAST_SCALAR_FMA) {
      return Math.fma(a, b, c);
    } else {
      return a * b + c;
    }
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
      res = fma(a[i], b[i], res);
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
      acc1 = fma(va, vb, acc1);

      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
      acc2 = fma(vc, vd, acc2);

      // three
      FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
      FloatVector vf = FloatVector.fromArray(FLOAT_SPECIES, b, i + 2 * FLOAT_SPECIES.length());
      acc3 = fma(ve, vf, acc3);

      // four
      FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
      FloatVector vh = FloatVector.fromArray(FLOAT_SPECIES, b, i + 3 * FLOAT_SPECIES.length());
      acc4 = fma(vg, vh, acc4);
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      acc1 = fma(va, vb, acc1);
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
      i += FLOAT_SPECIES.loopBound(a.length);
      float[] ret = cosineBody(a, b, i);
      sum += ret[0];
      norm1 += ret[1];
      norm2 += ret[2];
    }

    // scalar tail
    for (; i < a.length; i++) {
      sum = fma(a[i], b[i], sum);
      norm1 = fma(a[i], a[i], norm1);
      norm2 = fma(b[i], b[i], norm2);
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  /** vectorized cosine body */
  private float[] cosineBody(float[] a, float[] b, int limit) {
    int i = 0;
    // vector loop is unrolled 2x (2 accumulators in parallel)
    // each iteration has 3 FMAs, so its a lot already, no need to unroll more
    FloatVector sum1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector sum2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector norm1_1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector norm1_2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector norm2_1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector norm2_2 = FloatVector.zero(FLOAT_SPECIES);
    int unrolledLimit = limit - FLOAT_SPECIES.length();
    for (; i < unrolledLimit; i += 2 * FLOAT_SPECIES.length()) {
      // one
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      sum1 = fma(va, vb, sum1);
      norm1_1 = fma(va, va, norm1_1);
      norm2_1 = fma(vb, vb, norm2_1);

      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
      sum2 = fma(vc, vd, sum2);
      norm1_2 = fma(vc, vc, norm1_2);
      norm2_2 = fma(vd, vd, norm2_2);
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      sum1 = fma(va, vb, sum1);
      norm1_1 = fma(va, va, norm1_1);
      norm2_1 = fma(vb, vb, norm2_1);
    }
    return new float[] {
      sum1.add(sum2).reduceLanes(ADD),
      norm1_1.add(norm1_2).reduceLanes(ADD),
      norm2_1.add(norm2_2).reduceLanes(ADD)
    };
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
      res = fma(diff, diff, res);
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
      acc1 = fma(diff1, diff1, acc1);

      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd = FloatVector.fromArray(FLOAT_SPECIES, b, i + FLOAT_SPECIES.length());
      FloatVector diff2 = vc.sub(vd);
      acc2 = fma(diff2, diff2, acc2);

      // three
      FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
      FloatVector vf = FloatVector.fromArray(FLOAT_SPECIES, b, i + 2 * FLOAT_SPECIES.length());
      FloatVector diff3 = ve.sub(vf);
      acc3 = fma(diff3, diff3, acc3);

      // four
      FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
      FloatVector vh = FloatVector.fromArray(FLOAT_SPECIES, b, i + 3 * FLOAT_SPECIES.length());
      FloatVector diff4 = vg.sub(vh);
      acc4 = fma(diff4, diff4, acc4);
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, b, i);
      FloatVector diff = va.sub(vb);
      acc1 = fma(diff, diff, acc1);
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
    if (a.length >= 16 && HAS_FAST_INTEGER_VECTORS) {
      // compute vectorized dot product consistent with VPDPBUSD instruction
      if (VECTOR_BITSIZE >= 512) {
        i += BYTE_SPECIES.loopBound(a.length);
        res += dotProductBody512(a, b, i);
      } else if (VECTOR_BITSIZE == 256) {
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

  /** vectorized dot product body (512 bit vectors) */
  private int dotProductBody512(byte[] a, byte[] b, int limit) {
    IntVector acc = IntVector.zero(INT_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
      ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

      // 16-bit multiply: avoid AVX-512 heavy multiply on zmm
      Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> prod16 = va16.mul(vb16);

      // 32-bit add
      Vector<Integer> prod32 = prod16.convertShape(S2I, INT_SPECIES, 0);
      acc = acc.add(prod32);
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized dot product body (256 bit vectors) */
  private int dotProductBody256(byte[] a, byte[] b, int limit) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_256);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
      ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);

      // 32-bit multiply and add into accumulator
      Vector<Integer> va32 = va8.convertShape(B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> vb32 = vb8.convertShape(B2I, IntVector.SPECIES_256, 0);
      acc = acc.add(va32.mul(vb32));
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
  public int int4DotProduct(byte[] a, boolean apacked, byte[] b, boolean bpacked) {
    assert (apacked && bpacked) == false;
    int i = 0;
    int res = 0;
    if (apacked || bpacked) {
      byte[] packed = apacked ? a : b;
      byte[] unpacked = apacked ? b : a;
      if (packed.length >= 32) {
        if (VECTOR_BITSIZE >= 512) {
          i += ByteVector.SPECIES_256.loopBound(packed.length);
          res += dotProductBody512Int4Packed(unpacked, packed, i);
        } else if (VECTOR_BITSIZE == 256) {
          i += ByteVector.SPECIES_128.loopBound(packed.length);
          res += dotProductBody256Int4Packed(unpacked, packed, i);
        } else if (HAS_FAST_INTEGER_VECTORS) {
          i += ByteVector.SPECIES_64.loopBound(packed.length);
          res += dotProductBody128Int4Packed(unpacked, packed, i);
        }
      }
      // scalar tail
      for (; i < packed.length; i++) {
        byte packedByte = packed[i];
        byte unpacked1 = unpacked[i];
        byte unpacked2 = unpacked[i + packed.length];
        res += (packedByte & 0x0F) * unpacked2;
        res += ((packedByte & 0xFF) >> 4) * unpacked1;
      }
    } else {
      if (VECTOR_BITSIZE >= 512 || VECTOR_BITSIZE == 256) {
        return dotProduct(a, b);
      } else if (a.length >= 32 && HAS_FAST_INTEGER_VECTORS) {
        i += ByteVector.SPECIES_128.loopBound(a.length);
        res += int4DotProductBody128(a, b, i);
      }
      // scalar tail
      for (; i < a.length; i++) {
        res += b[i] * a[i];
      }
    }

    return res;
  }

  private int dotProductBody512Int4Packed(byte[] unpacked, byte[] packed, int limit) {
    int sum = 0;
    // iterate in chunks of 1024 items to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += 4096) {
      ShortVector acc0 = ShortVector.zero(ShortVector.SPECIES_512);
      ShortVector acc1 = ShortVector.zero(ShortVector.SPECIES_512);
      int innerLimit = Math.min(limit - i, 4096);
      for (int j = 0; j < innerLimit; j += ByteVector.SPECIES_256.length()) {
        // packed
        var vb8 = ByteVector.fromArray(ByteVector.SPECIES_256, packed, i + j);
        // unpacked
        var va8 = ByteVector.fromArray(ByteVector.SPECIES_256, unpacked, i + j + packed.length);

        // upper
        ByteVector prod8 = vb8.and((byte) 0x0F).mul(va8);
        Vector<Short> prod16 = prod8.convertShape(ZERO_EXTEND_B2S, ShortVector.SPECIES_512, 0);
        acc0 = acc0.add(prod16);

        // lower
        ByteVector vc8 = ByteVector.fromArray(ByteVector.SPECIES_256, unpacked, i + j);
        ByteVector prod8a = vb8.lanewise(LSHR, 4).mul(vc8);
        Vector<Short> prod16a = prod8a.convertShape(ZERO_EXTEND_B2S, ShortVector.SPECIES_512, 0);
        acc1 = acc1.add(prod16a);
      }
      IntVector intAcc0 = acc0.convertShape(S2I, IntVector.SPECIES_512, 0).reinterpretAsInts();
      IntVector intAcc1 = acc0.convertShape(S2I, IntVector.SPECIES_512, 1).reinterpretAsInts();
      IntVector intAcc2 = acc1.convertShape(S2I, IntVector.SPECIES_512, 0).reinterpretAsInts();
      IntVector intAcc3 = acc1.convertShape(S2I, IntVector.SPECIES_512, 1).reinterpretAsInts();
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reduceLanes(ADD);
    }
    return sum;
  }

  private int dotProductBody256Int4Packed(byte[] unpacked, byte[] packed, int limit) {
    int sum = 0;
    // iterate in chunks of 1024 items to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += 2048) {
      ShortVector acc0 = ShortVector.zero(ShortVector.SPECIES_256);
      ShortVector acc1 = ShortVector.zero(ShortVector.SPECIES_256);
      int innerLimit = Math.min(limit - i, 2048);
      for (int j = 0; j < innerLimit; j += ByteVector.SPECIES_128.length()) {
        // packed
        var vb8 = ByteVector.fromArray(ByteVector.SPECIES_128, packed, i + j);
        // unpacked
        var va8 = ByteVector.fromArray(ByteVector.SPECIES_128, unpacked, i + j + packed.length);

        // upper
        ByteVector prod8 = vb8.and((byte) 0x0F).mul(va8);
        Vector<Short> prod16 = prod8.convertShape(ZERO_EXTEND_B2S, ShortVector.SPECIES_256, 0);
        acc0 = acc0.add(prod16);

        // lower
        ByteVector vc8 = ByteVector.fromArray(ByteVector.SPECIES_128, unpacked, i + j);
        ByteVector prod8a = vb8.lanewise(LSHR, 4).mul(vc8);
        Vector<Short> prod16a = prod8a.convertShape(ZERO_EXTEND_B2S, ShortVector.SPECIES_256, 0);
        acc1 = acc1.add(prod16a);
      }
      IntVector intAcc0 = acc0.convertShape(S2I, IntVector.SPECIES_256, 0).reinterpretAsInts();
      IntVector intAcc1 = acc0.convertShape(S2I, IntVector.SPECIES_256, 1).reinterpretAsInts();
      IntVector intAcc2 = acc1.convertShape(S2I, IntVector.SPECIES_256, 0).reinterpretAsInts();
      IntVector intAcc3 = acc1.convertShape(S2I, IntVector.SPECIES_256, 1).reinterpretAsInts();
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reduceLanes(ADD);
    }
    return sum;
  }

  /** vectorized dot product body (128 bit vectors) */
  private int dotProductBody128Int4Packed(byte[] unpacked, byte[] packed, int limit) {
    int sum = 0;
    // iterate in chunks of 1024 items to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += 1024) {
      ShortVector acc0 = ShortVector.zero(ShortVector.SPECIES_128);
      ShortVector acc1 = ShortVector.zero(ShortVector.SPECIES_128);
      int innerLimit = Math.min(limit - i, 1024);
      for (int j = 0; j < innerLimit; j += ByteVector.SPECIES_64.length()) {
        // packed
        ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, packed, i + j);
        // unpacked
        ByteVector va8 =
            ByteVector.fromArray(ByteVector.SPECIES_64, unpacked, i + j + packed.length);

        // upper
        ByteVector prod8 = vb8.and((byte) 0x0F).mul(va8);
        ShortVector prod16 =
            prod8.convertShape(B2S, ShortVector.SPECIES_128, 0).reinterpretAsShorts();
        acc0 = acc0.add(prod16.and((short) 0xFF));

        // lower
        va8 = ByteVector.fromArray(ByteVector.SPECIES_64, unpacked, i + j);
        prod8 = vb8.lanewise(LSHR, 4).mul(va8);
        prod16 = prod8.convertShape(B2S, ShortVector.SPECIES_128, 0).reinterpretAsShorts();
        acc1 = acc1.add(prod16.and((short) 0xFF));
      }
      IntVector intAcc0 = acc0.convertShape(S2I, IntVector.SPECIES_128, 0).reinterpretAsInts();
      IntVector intAcc1 = acc0.convertShape(S2I, IntVector.SPECIES_128, 1).reinterpretAsInts();
      IntVector intAcc2 = acc1.convertShape(S2I, IntVector.SPECIES_128, 0).reinterpretAsInts();
      IntVector intAcc3 = acc1.convertShape(S2I, IntVector.SPECIES_128, 1).reinterpretAsInts();
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reduceLanes(ADD);
    }
    return sum;
  }

  private int int4DotProductBody128(byte[] a, byte[] b, int limit) {
    int sum = 0;
    // iterate in chunks of 1024 items to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += 1024) {
      ShortVector acc0 = ShortVector.zero(ShortVector.SPECIES_128);
      ShortVector acc1 = ShortVector.zero(ShortVector.SPECIES_128);
      int innerLimit = Math.min(limit - i, 1024);
      for (int j = 0; j < innerLimit; j += ByteVector.SPECIES_128.length()) {
        ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i + j);
        ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i + j);
        ByteVector prod8 = va8.mul(vb8);
        ShortVector prod16 =
            prod8.convertShape(B2S, ShortVector.SPECIES_128, 0).reinterpretAsShorts();
        acc0 = acc0.add(prod16.and((short) 0xFF));

        va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i + j + 8);
        vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i + j + 8);
        prod8 = va8.mul(vb8);
        prod16 = prod8.convertShape(B2S, ShortVector.SPECIES_128, 0).reinterpretAsShorts();
        acc1 = acc1.add(prod16.and((short) 0xFF));
      }
      IntVector intAcc0 = acc0.convertShape(S2I, IntVector.SPECIES_128, 0).reinterpretAsInts();
      IntVector intAcc1 = acc0.convertShape(S2I, IntVector.SPECIES_128, 1).reinterpretAsInts();
      IntVector intAcc2 = acc1.convertShape(S2I, IntVector.SPECIES_128, 0).reinterpretAsInts();
      IntVector intAcc3 = acc1.convertShape(S2I, IntVector.SPECIES_128, 1).reinterpretAsInts();
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    int i = 0;
    int sum = 0;
    int norm1 = 0;
    int norm2 = 0;

    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors (256-bit on intel to dodge performance landmines)
    if (a.length >= 16 && HAS_FAST_INTEGER_VECTORS) {
      final float[] ret;
      if (VECTOR_BITSIZE >= 512) {
        i += BYTE_SPECIES.loopBound(a.length);
        ret = cosineBody512(a, b, i);
      } else if (VECTOR_BITSIZE == 256) {
        i += BYTE_SPECIES.loopBound(a.length);
        ret = cosineBody256(a, b, i);
      } else {
        // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
        i += ByteVector.SPECIES_64.loopBound(a.length - ByteVector.SPECIES_64.length());
        ret = cosineBody128(a, b, i);
      }
      sum += ret[0];
      norm1 += ret[1];
      norm2 += ret[2];
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

  /** vectorized cosine body (512 bit vectors) */
  private float[] cosineBody512(byte[] a, byte[] b, int limit) {
    IntVector accSum = IntVector.zero(INT_SPECIES);
    IntVector accNorm1 = IntVector.zero(INT_SPECIES);
    IntVector accNorm2 = IntVector.zero(INT_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
      ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

      // 16-bit multiply: avoid AVX-512 heavy multiply on zmm
      Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES, 0);
      Vector<Short> norm1_16 = va16.mul(va16);
      Vector<Short> norm2_16 = vb16.mul(vb16);
      Vector<Short> prod16 = va16.mul(vb16);

      // sum into accumulators: 32-bit add
      Vector<Integer> norm1_32 = norm1_16.convertShape(S2I, INT_SPECIES, 0);
      Vector<Integer> norm2_32 = norm2_16.convertShape(S2I, INT_SPECIES, 0);
      Vector<Integer> prod32 = prod16.convertShape(S2I, INT_SPECIES, 0);
      accNorm1 = accNorm1.add(norm1_32);
      accNorm2 = accNorm2.add(norm2_32);
      accSum = accSum.add(prod32);
    }
    // reduce
    return new float[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  /** vectorized cosine body (256 bit vectors) */
  private float[] cosineBody256(byte[] a, byte[] b, int limit) {
    IntVector accSum = IntVector.zero(IntVector.SPECIES_256);
    IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_256);
    IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_256);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = ByteVector.fromArray(ByteVector.SPECIES_64, a, i);
      ByteVector vb8 = ByteVector.fromArray(ByteVector.SPECIES_64, b, i);

      // 16-bit multiply, and add into accumulators
      Vector<Integer> va32 = va8.convertShape(B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> vb32 = vb8.convertShape(B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> norm1_32 = va32.mul(va32);
      Vector<Integer> norm2_32 = vb32.mul(vb32);
      Vector<Integer> prod32 = va32.mul(vb32);
      accNorm1 = accNorm1.add(norm1_32);
      accNorm2 = accNorm2.add(norm2_32);
      accSum = accSum.add(prod32);
    }
    // reduce
    return new float[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  /** vectorized cosine body (128 bit vectors) */
  private float[] cosineBody128(byte[] a, byte[] b, int limit) {
    IntVector accSum = IntVector.zero(IntVector.SPECIES_128);
    IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_128);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
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
    return new float[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    int i = 0;
    int res = 0;

    // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
    // vectors (256-bit on intel to dodge performance landmines)
    if (a.length >= 16 && HAS_FAST_INTEGER_VECTORS) {
      if (VECTOR_BITSIZE >= 256) {
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
    IntVector acc = IntVector.zero(INT_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES, a, i);
      ByteVector vb8 = ByteVector.fromArray(BYTE_SPECIES, b, i);

      // 32-bit sub, multiply, and add into accumulators
      // TODO: uses AVX-512 heavy multiply on zmm, should we just use 256-bit vectors on AVX-512?
      Vector<Integer> va32 = va8.convertShape(B2I, INT_SPECIES, 0);
      Vector<Integer> vb32 = vb8.convertShape(B2I, INT_SPECIES, 0);
      Vector<Integer> diff32 = va32.sub(vb32);
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
