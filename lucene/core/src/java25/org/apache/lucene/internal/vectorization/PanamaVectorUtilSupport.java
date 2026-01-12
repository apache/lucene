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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.S2I;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2I;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2S;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_S2I;
import static org.apache.lucene.util.VectorUtil.EPSILON;

import java.lang.foreign.MemorySegment;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
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
 * </ul>
 *
 * Setting these properties will make this code run EXTREMELY slow!
 */
final class PanamaVectorUtilSupport implements VectorUtilSupport {

  // preferred vector sizes, which can be altered for testing
  private static final VectorSpecies<Float> FLOAT_SPECIES;
  private static final VectorSpecies<Double> DOUBLE_SPECIES =
      PanamaVectorConstants.PREFERRED_DOUBLE_SPECIES;
  // This create a vector species which we make sure have exact half bits of DOUBLE_SPECIES
  private static final VectorSpecies<Integer> INT_FOR_DOUBLE_SPECIES =
      VectorSpecies.of(int.class, VectorShape.forBitSize(DOUBLE_SPECIES.vectorBitSize() / 2));
  private static final VectorSpecies<Integer> INT_SPECIES =
      PanamaVectorConstants.PRERERRED_INT_SPECIES;
  private static final VectorSpecies<Byte> BYTE_SPECIES;
  private static final VectorSpecies<Short> SHORT_SPECIES;
  private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
  private static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;

  static final int VECTOR_BITSIZE;

  static {
    VECTOR_BITSIZE = PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE;
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
  }

  // the way FMA should work! if available use it, otherwise fall back to mul/add
  @SuppressForbidden(reason = "Uses FMA only where fast and carefully contained")
  static FloatVector fma(FloatVector a, FloatVector b, FloatVector c) {
    if (Constants.HAS_FAST_VECTOR_FMA) {
      return a.fma(b, c);
    } else {
      return a.mul(b).add(c);
    }
  }

  @SuppressForbidden(reason = "Uses FMA only where fast and carefully contained")
  static float fma(float a, float b, float c) {
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

    // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
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

    // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
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

    // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
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

  private interface ByteVectorLoader {
    int length();

    ByteVector load(VectorSpecies<Byte> species, int index);

    byte tail(int index);
  }

  private record ArrayLoader(byte[] arr) implements ByteVectorLoader {
    @Override
    public int length() {
      return arr.length;
    }

    @Override
    public ByteVector load(VectorSpecies<Byte> species, int index) {
      assert index + species.length() <= length();
      return ByteVector.fromArray(species, arr, index);
    }

    @Override
    public byte tail(int index) {
      assert index <= length();
      return arr[index];
    }
  }

  private record MemorySegmentLoader(MemorySegment segment) implements ByteVectorLoader {
    @Override
    public int length() {
      return Math.toIntExact(segment.byteSize());
    }

    @Override
    public ByteVector load(VectorSpecies<Byte> species, int index) {
      assert index + species.length() <= length();
      return ByteVector.fromMemorySegment(species, segment, index, LITTLE_ENDIAN);
    }

    @Override
    public byte tail(int index) {
      assert index <= length();
      return segment.get(JAVA_BYTE, index);
    }
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    return dotProductBody(new ArrayLoader(a), new ArrayLoader(b), true);
  }

  @Override
  public int uint8DotProduct(byte[] a, byte[] b) {
    return dotProductBody(new ArrayLoader(a), new ArrayLoader(b), false);
  }

  public static int dotProduct(byte[] a, MemorySegment b) {
    return dotProductBody(new ArrayLoader(a), new MemorySegmentLoader(b), true);
  }

  public static int dotProduct(MemorySegment a, MemorySegment b) {
    return dotProductBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b), true);
  }

  public static int uint8DotProduct(byte[] a, MemorySegment b) {
    return dotProductBody(new ArrayLoader(a), new MemorySegmentLoader(b), false);
  }

  public static int uint8DotProduct(MemorySegment a, MemorySegment b) {
    return dotProductBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b), false);
  }

  private static int dotProductBody(ByteVectorLoader a, ByteVectorLoader b, boolean signed) {
    assert a.length() == b.length();
    int i = 0;
    int res = 0;

    // only vectorize if we'll at least enter the loop a single time
    if (a.length() >= 16) {
      // compute vectorized dot product consistent with VPDPBUSD instruction
      if (VECTOR_BITSIZE >= 512) {
        i += BYTE_SPECIES.loopBound(a.length());
        res += dotProductBody512(a, b, i, signed);
      } else if (VECTOR_BITSIZE == 256) {
        i += BYTE_SPECIES.loopBound(a.length());
        res += dotProductBody256(a, b, i, signed);
      } else {
        // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
        i += ByteVector.SPECIES_64.loopBound(a.length() - ByteVector.SPECIES_64.length());
        res += dotProductBody128(a, b, i, signed);
      }
    }

    // scalar tail
    if (signed) {
      for (; i < a.length(); i++) {
        res += a.tail(i) * b.tail(i);
      }
    } else {
      for (; i < a.length(); i++) {
        res += Byte.toUnsignedInt(a.tail(i)) * Byte.toUnsignedInt(b.tail(i));
      }
    }
    return res;
  }

  /** vectorized dot product body (512 bit vectors) */
  private static int dotProductBody512(
      ByteVectorLoader a, ByteVectorLoader b, int limit, boolean signed) {
    IntVector acc = IntVector.zero(INT_SPECIES);
    var conversion_short = signed ? B2S : ZERO_EXTEND_B2S;
    var conversion_int = signed ? S2I : ZERO_EXTEND_S2I;
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = a.load(BYTE_SPECIES, i);
      ByteVector vb8 = b.load(BYTE_SPECIES, i);

      // 16-bit multiply: avoid AVX-512 heavy multiply on zmm
      Vector<Short> va16 = va8.convertShape(conversion_short, SHORT_SPECIES, 0);
      Vector<Short> vb16 = vb8.convertShape(conversion_short, SHORT_SPECIES, 0);
      Vector<Short> prod16 = va16.mul(vb16);

      // 32-bit add
      Vector<Integer> prod32 = prod16.convertShape(conversion_int, INT_SPECIES, 0);
      acc = acc.add(prod32);
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized dot product body (256 bit vectors) */
  private static int dotProductBody256(
      ByteVectorLoader a, ByteVectorLoader b, int limit, boolean signed) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_256);
    var conversion = signed ? B2I : ZERO_EXTEND_B2I;
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = a.load(ByteVector.SPECIES_64, i);
      ByteVector vb8 = b.load(ByteVector.SPECIES_64, i);

      // 32-bit multiply and add into accumulator
      Vector<Integer> va32 = va8.convertShape(conversion, IntVector.SPECIES_256, 0);
      Vector<Integer> vb32 = vb8.convertShape(conversion, IntVector.SPECIES_256, 0);
      acc = acc.add(va32.mul(vb32));
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized dot product body (128 bit vectors) */
  private static int dotProductBody128(
      ByteVectorLoader a, ByteVectorLoader b, int limit, boolean signed) {
    IntVector acc = IntVector.zero(IntVector.SPECIES_128);
    var conversion_short = signed ? B2S : ZERO_EXTEND_B2S;
    var conversion_int = signed ? S2I : ZERO_EXTEND_S2I;
    // 4 bytes at a time (re-loading half the vector each time!)
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
      // load 8 bytes
      ByteVector va8 = a.load(ByteVector.SPECIES_64, i);
      ByteVector vb8 = b.load(ByteVector.SPECIES_64, i);

      // process first "half" only: 16-bit multiply
      Vector<Short> va16 = va8.convert(conversion_short, 0);
      Vector<Short> vb16 = vb8.convert(conversion_short, 0);
      Vector<Short> prod16 = va16.mul(vb16);

      // 32-bit add
      acc = acc.add(prod16.convertShape(conversion_int, IntVector.SPECIES_128, 0));
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  private static class Int4Constants {
    static final VectorSpecies<Byte> BYTE_SPECIES;
    static final VectorSpecies<Short> SHORT_SPECIES;
    static final int CHUNK;

    static {
      if (VECTOR_BITSIZE >= 512) {
        BYTE_SPECIES = ByteVector.SPECIES_256;
        SHORT_SPECIES = ShortVector.SPECIES_512;
        CHUNK = 4096;
      } else if (VECTOR_BITSIZE == 256) {
        BYTE_SPECIES = ByteVector.SPECIES_128;
        SHORT_SPECIES = ShortVector.SPECIES_256;
        CHUNK = 2048;
      } else {
        BYTE_SPECIES = ByteVector.SPECIES_64;
        SHORT_SPECIES = ShortVector.SPECIES_128;
        CHUNK = 1024;
      }
    }
  }

  @Override
  public int int4DotProduct(byte[] a, byte[] b) {
    return int4DotProductBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static int int4DotProduct(byte[] a, MemorySegment b) {
    return int4DotProductBody(new ArrayLoader(a), new MemorySegmentLoader(b));
  }

  public static int int4DotProduct(MemorySegment a, MemorySegment b) {
    return int4DotProductBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b));
  }

  private static int int4DotProductBody(ByteVectorLoader a, ByteVectorLoader b) {
    int i = 0;
    int res = 0;
    if (a.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(a.length());
      res += int4DotProductBody(a, b, i);
    }
    // scalar tail
    for (; i < a.length(); i++) {
      res += a.tail(i) * b.tail(i);
    }
    return res;
  }

  private static int int4DotProductBody(ByteVectorLoader a, ByteVectorLoader b, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // unpacked
        ByteVector vb8 = b.load(Int4Constants.BYTE_SPECIES, i + j);
        Vector<Short> vb16 = vb8.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);

        // unpacked
        ByteVector va8 = a.load(Int4Constants.BYTE_SPECIES, i + j);
        Vector<Short> va16 = va8.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);

        acc = acc.add(vb16.mul(va16));
      }
      Vector<Integer> intAcc0 = acc.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public int int4DotProductSinglePacked(byte[] unpacked, byte[] packed) {
    return int4DotProductSinglePackedBody(new ArrayLoader(unpacked), new ArrayLoader(packed));
  }

  public static int int4DotProductSinglePacked(byte[] unpacked, MemorySegment packed) {
    return int4DotProductSinglePackedBody(
        new ArrayLoader(unpacked), new MemorySegmentLoader(packed));
  }

  private static int int4DotProductSinglePackedBody(
      ByteVectorLoader unpacked, ByteVectorLoader packed) {
    int i = 0;
    int res = 0;
    if (packed.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(packed.length());
      res += int4DotProductSinglePackedBody(unpacked, packed, i);
    }
    // scalar tail
    for (; i < packed.length(); i++) {
      byte packedByte = packed.tail(i);
      byte unpacked1 = unpacked.tail(i);
      byte unpacked2 = unpacked.tail(i + packed.length());
      res += (packedByte & 0x0F) * unpacked2;
      res += ((packedByte & 0xFF) >> 4) * unpacked1;
    }
    return res;
  }

  private static int int4DotProductSinglePackedBody(
      ByteVectorLoader unpacked, ByteVectorLoader packed, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc0 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      ShortVector acc1 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // packed
        ByteVector vb8 = packed.load(Int4Constants.BYTE_SPECIES, i + j);

        // upper
        ByteVector va8 = unpacked.load(Int4Constants.BYTE_SPECIES, i + j + packed.length());
        ByteVector prod8 = vb8.and((byte) 0x0F).mul(va8);
        Vector<Short> prod16 = prod8.convertShape(ZERO_EXTEND_B2S, Int4Constants.SHORT_SPECIES, 0);
        acc0 = acc0.add(prod16);

        // lower
        ByteVector vc8 = unpacked.load(Int4Constants.BYTE_SPECIES, i + j);
        ByteVector prod8a = vb8.lanewise(LSHR, 4).mul(vc8);
        Vector<Short> prod16a =
            prod8a.convertShape(ZERO_EXTEND_B2S, Int4Constants.SHORT_SPECIES, 0);
        acc1 = acc1.add(prod16a);
      }
      Vector<Integer> intAcc0 = acc0.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc0.convert(S2I, 1);
      Vector<Integer> intAcc2 = acc1.convert(S2I, 0);
      Vector<Integer> intAcc3 = acc1.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public int int4DotProductBothPacked(byte[] a, byte[] b) {
    return int4DotProductBothPackedBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static int int4DotProductBothPacked(MemorySegment a, MemorySegment b) {
    return int4DotProductBothPackedBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b));
  }

  private static int int4DotProductBothPackedBody(ByteVectorLoader a, ByteVectorLoader b) {
    int i = 0;
    int res = 0;
    if (a.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(a.length());
      res += int4DotProductBothPackedBody(a, b, i);
    }
    // scalar tail
    for (; i < a.length(); i++) {
      byte aByte = a.tail(i);
      byte bByte = b.tail(i);
      res += (aByte & 0x0F) * (bByte & 0x0F);
      res += ((aByte & 0xFF) >> 4) * ((bByte & 0xFF) >> 4);
    }
    return res;
  }

  private static int int4DotProductBothPackedBody(
      ByteVectorLoader a, ByteVectorLoader b, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc0 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      ShortVector acc1 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // packed
        var vb8 = b.load(Int4Constants.BYTE_SPECIES, i + j);
        // packed
        var va8 = a.load(Int4Constants.BYTE_SPECIES, i + j);

        // upper
        ByteVector prod8 = vb8.and((byte) 0x0F).mul(va8.and((byte) 0x0F));
        Vector<Short> prod16 = prod8.convertShape(ZERO_EXTEND_B2S, Int4Constants.SHORT_SPECIES, 0);
        acc0 = acc0.add(prod16);

        // lower
        ByteVector prod8a = vb8.lanewise(LSHR, 4).mul(va8.lanewise(LSHR, 4));
        Vector<Short> prod16a =
            prod8a.convertShape(ZERO_EXTEND_B2S, Int4Constants.SHORT_SPECIES, 0);
        acc1 = acc1.add(prod16a);
      }
      Vector<Integer> intAcc0 = acc0.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc0.convert(S2I, 1);
      Vector<Integer> intAcc2 = acc1.convert(S2I, 0);
      Vector<Integer> intAcc3 = acc1.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    return cosineBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static float cosine(MemorySegment a, MemorySegment b) {
    return cosineBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b));
  }

  public static float cosine(byte[] a, MemorySegment b) {
    return cosineBody(new ArrayLoader(a), new MemorySegmentLoader(b));
  }

  private static float cosineBody(ByteVectorLoader a, ByteVectorLoader b) {
    int i = 0;
    int sum = 0;
    int norm1 = 0;
    int norm2 = 0;

    // only vectorize if we'll at least enter the loop a single time
    if (a.length() >= 16) {
      final int[] ret;
      if (VECTOR_BITSIZE >= 512) {
        i += BYTE_SPECIES.loopBound(a.length());
        ret = cosineBody512(a, b, i);
      } else if (VECTOR_BITSIZE == 256) {
        i += BYTE_SPECIES.loopBound(a.length());
        ret = cosineBody256(a, b, i);
      } else {
        // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
        i += ByteVector.SPECIES_64.loopBound(a.length() - ByteVector.SPECIES_64.length());
        ret = cosineBody128(a, b, i);
      }
      sum += ret[0];
      norm1 += ret[1];
      norm2 += ret[2];
    }

    // scalar tail
    for (; i < a.length(); i++) {
      byte elem1 = a.tail(i);
      byte elem2 = b.tail(i);
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  /** vectorized cosine body (512 bit vectors) */
  private static int[] cosineBody512(ByteVectorLoader a, ByteVectorLoader b, int limit) {
    IntVector accSum = IntVector.zero(INT_SPECIES);
    IntVector accNorm1 = IntVector.zero(INT_SPECIES);
    IntVector accNorm2 = IntVector.zero(INT_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = a.load(BYTE_SPECIES, i);
      ByteVector vb8 = b.load(BYTE_SPECIES, i);

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
    return new int[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  /** vectorized cosine body (256 bit vectors) */
  private static int[] cosineBody256(ByteVectorLoader a, ByteVectorLoader b, int limit) {
    IntVector accSum = IntVector.zero(IntVector.SPECIES_256);
    IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_256);
    IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_256);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = a.load(ByteVector.SPECIES_64, i);
      ByteVector vb8 = b.load(ByteVector.SPECIES_64, i);

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
    return new int[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  /** vectorized cosine body (128 bit vectors) */
  private static int[] cosineBody128(ByteVectorLoader a, ByteVectorLoader b, int limit) {
    IntVector accSum = IntVector.zero(IntVector.SPECIES_128);
    IntVector accNorm1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector accNorm2 = IntVector.zero(IntVector.SPECIES_128);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
      ByteVector va8 = a.load(ByteVector.SPECIES_64, i);
      ByteVector vb8 = b.load(ByteVector.SPECIES_64, i);

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
    return new int[] {
      accSum.reduceLanes(ADD), accNorm1.reduceLanes(ADD), accNorm2.reduceLanes(ADD)
    };
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    return squareDistanceBody(new ArrayLoader(a), new ArrayLoader(b), true);
  }

  @Override
  public int uint8SquareDistance(byte[] a, byte[] b) {
    return squareDistanceBody(new ArrayLoader(a), new ArrayLoader(b), false);
  }

  public static int squareDistance(MemorySegment a, MemorySegment b) {
    return squareDistanceBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b), true);
  }

  public static int squareDistance(byte[] a, MemorySegment b) {
    return squareDistanceBody(new ArrayLoader(a), new MemorySegmentLoader(b), true);
  }

  public static int uint8SquareDistance(MemorySegment a, MemorySegment b) {
    return squareDistanceBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b), false);
  }

  public static int uint8SquareDistance(byte[] a, MemorySegment b) {
    return squareDistanceBody(new ArrayLoader(a), new MemorySegmentLoader(b), false);
  }

  private static int squareDistanceBody(ByteVectorLoader a, ByteVectorLoader b, boolean signed) {
    assert a.length() == b.length();
    int i = 0;
    int res = 0;

    // only vectorize if we'll at least enter the loop a single time
    if (a.length() >= 16) {
      if (VECTOR_BITSIZE >= 256) {
        i += BYTE_SPECIES.loopBound(a.length());
        res += squareDistanceBody256(a, b, i, signed);
      } else {
        i += ByteVector.SPECIES_64.loopBound(a.length());
        res += squareDistanceBody128(a, b, i, signed);
      }
    }

    // scalar tail
    if (signed) {
      for (; i < a.length(); i++) {
        int diff = a.tail(i) - b.tail(i);
        res += diff * diff;
      }
    } else {
      for (; i < a.length(); i++) {
        int diff = Byte.toUnsignedInt(a.tail(i)) - Byte.toUnsignedInt(b.tail(i));
        res += diff * diff;
      }
    }
    return res;
  }

  /** vectorized square distance body (256+ bit vectors) */
  private static int squareDistanceBody256(
      ByteVectorLoader a, ByteVectorLoader b, int limit, boolean signed) {
    IntVector acc = IntVector.zero(INT_SPECIES);
    var conversion = signed ? B2I : ZERO_EXTEND_B2I;
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      ByteVector va8 = a.load(BYTE_SPECIES, i);
      ByteVector vb8 = b.load(BYTE_SPECIES, i);

      // 32-bit sub, multiply, and add into accumulators
      // TODO: uses AVX-512 heavy multiply on zmm, should we just use 256-bit vectors on AVX-512?
      Vector<Integer> va32 = va8.convertShape(conversion, INT_SPECIES, 0);
      Vector<Integer> vb32 = vb8.convertShape(conversion, INT_SPECIES, 0);
      Vector<Integer> diff32 = va32.sub(vb32);
      acc = acc.add(diff32.mul(diff32));
    }
    // reduce
    return acc.reduceLanes(ADD);
  }

  /** vectorized square distance body (128 bit vectors) */
  private static int squareDistanceBody128(
      ByteVectorLoader a, ByteVectorLoader b, int limit, boolean signed) {
    // 128-bit implementation, which must "split up" vectors due to widening conversions
    // it doesn't help to do the overlapping read trick, due to 32-bit multiply in the formula
    IntVector acc1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc2 = IntVector.zero(IntVector.SPECIES_128);
    var conversion_short = signed ? B2S : ZERO_EXTEND_B2S;
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      ByteVector va8 = a.load(ByteVector.SPECIES_64, i);
      ByteVector vb8 = b.load(ByteVector.SPECIES_64, i);

      // 16-bit sub
      Vector<Short> va16 = va8.convertShape(conversion_short, ShortVector.SPECIES_128, 0);
      Vector<Short> vb16 = vb8.convertShape(conversion_short, ShortVector.SPECIES_128, 0);
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

  @Override
  public int int4SquareDistance(byte[] a, byte[] b) {
    return int4SquareDistanceBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static int int4SquareDistance(byte[] a, MemorySegment b) {
    return int4SquareDistanceBody(new ArrayLoader(a), new MemorySegmentLoader(b));
  }

  public static int int4SquareDistance(MemorySegment a, MemorySegment b) {
    return int4SquareDistanceBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b));
  }

  private static int int4SquareDistanceBody(ByteVectorLoader a, ByteVectorLoader b) {
    int i = 0;
    int res = 0;
    if (a.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(a.length());
      res += int4SquareDistanceBody(a, b, i);
    }
    // scalar tail
    for (; i < a.length(); i++) {
      int diff = a.tail(i) - b.tail(i);
      res += diff * diff;
    }
    return res;
  }

  private static int int4SquareDistanceBody(ByteVectorLoader a, ByteVectorLoader b, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // unpacked
        var vb8 = b.load(Int4Constants.BYTE_SPECIES, i + j);
        // unpacked
        var va8 = a.load(Int4Constants.BYTE_SPECIES, i + j);

        ByteVector diff8 = vb8.sub(va8);
        Vector<Short> diff16 = diff8.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);
        acc = acc.add(diff16.mul(diff16));
      }
      Vector<Integer> intAcc0 = acc.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public int int4SquareDistanceSinglePacked(byte[] a, byte[] b) {
    return int4SquareDistanceSinglePackedBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static int int4SquareDistanceSinglePacked(byte[] a, MemorySegment b) {
    return int4SquareDistanceSinglePackedBody(new ArrayLoader(a), new MemorySegmentLoader(b));
  }

  private static int int4SquareDistanceSinglePackedBody(
      ByteVectorLoader unpacked, ByteVectorLoader packed) {
    int i = 0;
    int res = 0;
    if (packed.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(packed.length());
      res += int4SquareDistanceSinglePackedBody(unpacked, packed, i);
    }
    // scalar tail
    for (; i < packed.length(); i++) {
      byte packedByte = packed.tail(i);
      byte unpacked1 = unpacked.tail(i);
      byte unpacked2 = unpacked.tail(i + packed.length());

      int diff1 = (packedByte & 0x0F) - unpacked2;
      int diff2 = ((packedByte & 0xFF) >> 4) - unpacked1;

      res += diff1 * diff1 + diff2 * diff2;
    }
    return res;
  }

  private static int int4SquareDistanceSinglePackedBody(
      ByteVectorLoader unpacked, ByteVectorLoader packed, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc0 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      ShortVector acc1 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // packed
        ByteVector vb8 = packed.load(Int4Constants.BYTE_SPECIES, i + j);

        // upper
        ByteVector va8 = unpacked.load(Int4Constants.BYTE_SPECIES, i + j + packed.length());
        ByteVector diff8 = vb8.and((byte) 0x0F).sub(va8);
        Vector<Short> diff16 = diff8.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);
        acc0 = acc0.add(diff16.mul(diff16));

        // lower
        ByteVector vc8 = unpacked.load(Int4Constants.BYTE_SPECIES, i + j);
        ByteVector diff8a = vb8.lanewise(LSHR, 4).sub(vc8);
        Vector<Short> diff16a = diff8a.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);
        acc1 = acc1.add(diff16a.mul(diff16a));
      }
      Vector<Integer> intAcc0 = acc0.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc0.convert(S2I, 1);
      Vector<Integer> intAcc2 = acc1.convert(S2I, 0);
      Vector<Integer> intAcc3 = acc1.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  @Override
  public int int4SquareDistanceBothPacked(byte[] a, byte[] b) {
    return int4SquareDistanceBothPackedBody(new ArrayLoader(a), new ArrayLoader(b));
  }

  public static int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b) {
    return int4SquareDistanceBothPackedBody(new MemorySegmentLoader(a), new MemorySegmentLoader(b));
  }

  private static int int4SquareDistanceBothPackedBody(ByteVectorLoader a, ByteVectorLoader b) {
    int i = 0;
    int res = 0;
    if (a.length() >= 32) {
      i += Int4Constants.BYTE_SPECIES.loopBound(a.length());
      res += int4SquareDistanceBothPackedBody(a, b, i);
    }
    // scalar tail
    for (; i < a.length(); i++) {
      byte aByte = a.tail(i);
      byte bByte = b.tail(i);

      int diff1 = (aByte & 0x0F) - (bByte & 0x0F);
      int diff2 = ((aByte & 0xFF) >> 4) - ((bByte & 0xFF) >> 4);

      res += diff1 * diff1 + diff2 * diff2;
    }
    return res;
  }

  private static int int4SquareDistanceBothPackedBody(
      ByteVectorLoader a, ByteVectorLoader b, int limit) {
    int sum = 0;
    // iterate in chunks to ensure we don't overflow the short accumulator
    for (int i = 0; i < limit; i += Int4Constants.CHUNK) {
      ShortVector acc0 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      ShortVector acc1 = ShortVector.zero(Int4Constants.SHORT_SPECIES);
      int innerLimit = Math.min(limit - i, Int4Constants.CHUNK);
      for (int j = 0; j < innerLimit; j += Int4Constants.BYTE_SPECIES.length()) {
        // packed
        var vb8 = b.load(Int4Constants.BYTE_SPECIES, i + j);
        // packed
        var va8 = a.load(Int4Constants.BYTE_SPECIES, i + j);

        // upper
        ByteVector diff8 = vb8.and((byte) 0x0F).sub(va8.and((byte) 0x0F));
        Vector<Short> diff16 = diff8.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);
        acc0 = acc0.add(diff16.mul(diff16));

        // lower
        ByteVector diff8a = vb8.lanewise(LSHR, 4).sub(va8.lanewise(LSHR, 4));
        Vector<Short> diff16a = diff8a.convertShape(B2S, Int4Constants.SHORT_SPECIES, 0);
        acc1 = acc1.add(diff16a.mul(diff16a));
      }
      Vector<Integer> intAcc0 = acc0.convert(S2I, 0);
      Vector<Integer> intAcc1 = acc0.convert(S2I, 1);
      Vector<Integer> intAcc2 = acc1.convert(S2I, 0);
      Vector<Integer> intAcc3 = acc1.convert(S2I, 1);
      sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reinterpretAsInts().reduceLanes(ADD);
    }
    return sum;
  }

  // Experiments suggest that we need at least 8 lanes so that the overhead of going with the vector
  // approach and counting trues on vector masks pays off.
  private static final boolean ENABLE_FIND_NEXT_GEQ_VECTOR_OPTO = INT_SPECIES.length() >= 8;

  @Override
  public int findNextGEQ(int[] buffer, int target, int from, int to) {
    if (ENABLE_FIND_NEXT_GEQ_VECTOR_OPTO) {
      // This effectively implements the V1 intersection algorithm from
      // D. Lemire, L. Boytsov, N. Kurz SIMD Compression and the Intersection of Sorted Integers
      // with T = INT_SPECIES.length(), ie. T=8 with AVX2 and T=16 with AVX-512
      // https://arxiv.org/pdf/1401.6399
      for (; from + INT_SPECIES.length() < to; from += INT_SPECIES.length() + 1) {
        if (buffer[from + INT_SPECIES.length()] >= target) {
          IntVector vector = IntVector.fromArray(INT_SPECIES, buffer, from);
          VectorMask<Integer> mask = vector.compare(VectorOperators.LT, target);
          return from + mask.trueCount();
        }
      }
    }
    for (int i = from; i < to; ++i) {
      if (buffer[i] >= target) {
        return i;
      }
    }
    return to;
  }

  @Override
  public long int4BitDotProduct(byte[] q, byte[] d) {
    assert q.length == d.length * 4;
    // 128 / 8 == 16
    if (d.length >= 16) {
      if (VECTOR_BITSIZE >= 256) {
        return int4BitDotProduct256(q, d);
      } else if (VECTOR_BITSIZE == 128) {
        return int4BitDotProduct128(q, d);
      }
    }
    return DefaultVectorUtilSupport.int4BitDotProductImpl(q, d);
  }

  static long int4BitDotProduct256(byte[] q, byte[] d) {
    return int4BitDotProduct256WithOffset(q, d, 0, d.length);
  }

  public static long int4BitDotProduct128(byte[] q, byte[] d) {
    return int4BitDotProduct128WithOffset(q, d, 0, d.length);
  }

  @Override
  public long int4DibitDotProduct(byte[] q, byte[] d) {
    assert q.length == d.length * 2;
    int stripeSize = d.length / 2;
    if (stripeSize >= 16) {
      if (VECTOR_BITSIZE >= 256) {
        return int4DibitDotProduct256(q, d);
      } else if (VECTOR_BITSIZE == 128) {
        return int4DibitDotProduct128(q, d);
      }
    }
    return DefaultVectorUtilSupport.int4DibitDotProductImpl(q, d);
  }

  static long int4DibitDotProduct256(byte[] q, byte[] d) {
    int stripeSize = d.length / 2;
    long ret0 = int4BitDotProduct256WithOffset(q, d, 0, stripeSize);
    long ret1 = int4BitDotProduct256WithOffset(q, d, stripeSize, stripeSize);
    return ret0 + (ret1 << 1);
  }

  static long int4DibitDotProduct128(byte[] q, byte[] d) {
    int stripeSize = d.length / 2;
    long ret0 = int4BitDotProduct128WithOffset(q, d, 0, stripeSize);
    long ret1 = int4BitDotProduct128WithOffset(q, d, stripeSize, stripeSize);
    return ret0 + (ret1 << 1);
  }

  private static long int4BitDotProduct256WithOffset(
      byte[] q, byte[] d, int dOffset, int stripeSize) {
    long subRet0 = 0;
    long subRet1 = 0;
    long subRet2 = 0;
    long subRet3 = 0;
    int i = 0;

    if (stripeSize >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
      int limit = ByteVector.SPECIES_256.loopBound(stripeSize);
      var sum0 = LongVector.zero(LongVector.SPECIES_256);
      var sum1 = LongVector.zero(LongVector.SPECIES_256);
      var sum2 = LongVector.zero(LongVector.SPECIES_256);
      var sum3 = LongVector.zero(LongVector.SPECIES_256);
      for (; i < limit; i += ByteVector.SPECIES_256.length()) {
        var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
        var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + stripeSize).reinterpretAsLongs();
        var vq2 =
            ByteVector.fromArray(BYTE_SPECIES_256, q, i + stripeSize * 2).reinterpretAsLongs();
        var vq3 =
            ByteVector.fromArray(BYTE_SPECIES_256, q, i + stripeSize * 3).reinterpretAsLongs();
        var vd = ByteVector.fromArray(BYTE_SPECIES_256, d, dOffset + i).reinterpretAsLongs();
        sum0 = sum0.add(vq0.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum1 = sum1.add(vq1.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum2 = sum2.add(vq2.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum3 = sum3.add(vq3.and(vd).lanewise(VectorOperators.BIT_COUNT));
      }
      subRet0 += sum0.reduceLanes(VectorOperators.ADD);
      subRet1 += sum1.reduceLanes(VectorOperators.ADD);
      subRet2 += sum2.reduceLanes(VectorOperators.ADD);
      subRet3 += sum3.reduceLanes(VectorOperators.ADD);
    }

    if (stripeSize - i >= ByteVector.SPECIES_128.vectorByteSize()) {
      var sum0 = LongVector.zero(LongVector.SPECIES_128);
      var sum1 = LongVector.zero(LongVector.SPECIES_128);
      var sum2 = LongVector.zero(LongVector.SPECIES_128);
      var sum3 = LongVector.zero(LongVector.SPECIES_128);
      int limit = ByteVector.SPECIES_128.loopBound(stripeSize);
      for (; i < limit; i += ByteVector.SPECIES_128.length()) {
        var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
        var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize).reinterpretAsLongs();
        var vq2 =
            ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize * 2).reinterpretAsLongs();
        var vq3 =
            ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize * 3).reinterpretAsLongs();
        var vd = ByteVector.fromArray(BYTE_SPECIES_128, d, dOffset + i).reinterpretAsLongs();
        sum0 = sum0.add(vq0.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum1 = sum1.add(vq1.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum2 = sum2.add(vq2.and(vd).lanewise(VectorOperators.BIT_COUNT));
        sum3 = sum3.add(vq3.and(vd).lanewise(VectorOperators.BIT_COUNT));
      }
      subRet0 += sum0.reduceLanes(VectorOperators.ADD);
      subRet1 += sum1.reduceLanes(VectorOperators.ADD);
      subRet2 += sum2.reduceLanes(VectorOperators.ADD);
      subRet3 += sum3.reduceLanes(VectorOperators.ADD);
    }
    // tail as bytes
    for (; i < stripeSize; i++) {
      subRet0 += Integer.bitCount((q[i] & d[dOffset + i]) & 0xFF);
      subRet1 += Integer.bitCount((q[i + stripeSize] & d[dOffset + i]) & 0xFF);
      subRet2 += Integer.bitCount((q[i + 2 * stripeSize] & d[dOffset + i]) & 0xFF);
      subRet3 += Integer.bitCount((q[i + 3 * stripeSize] & d[dOffset + i]) & 0xFF);
    }
    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
  }

  private static long int4BitDotProduct128WithOffset(
      byte[] q, byte[] d, int dOffset, int stripeSize) {
    long subRet0 = 0;
    long subRet1 = 0;
    long subRet2 = 0;
    long subRet3 = 0;
    int i = 0;

    var sum0 = IntVector.zero(IntVector.SPECIES_128);
    var sum1 = IntVector.zero(IntVector.SPECIES_128);
    var sum2 = IntVector.zero(IntVector.SPECIES_128);
    var sum3 = IntVector.zero(IntVector.SPECIES_128);
    int limit = ByteVector.SPECIES_128.loopBound(stripeSize);
    for (; i < limit; i += ByteVector.SPECIES_128.length()) {
      var vd = ByteVector.fromArray(BYTE_SPECIES_128, d, dOffset + i).reinterpretAsInts();
      var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
      var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize).reinterpretAsInts();
      var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize * 2).reinterpretAsInts();
      var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + stripeSize * 3).reinterpretAsInts();
      sum0 = sum0.add(vd.and(vq0).lanewise(VectorOperators.BIT_COUNT));
      sum1 = sum1.add(vd.and(vq1).lanewise(VectorOperators.BIT_COUNT));
      sum2 = sum2.add(vd.and(vq2).lanewise(VectorOperators.BIT_COUNT));
      sum3 = sum3.add(vd.and(vq3).lanewise(VectorOperators.BIT_COUNT));
    }
    subRet0 += sum0.reduceLanes(VectorOperators.ADD);
    subRet1 += sum1.reduceLanes(VectorOperators.ADD);
    subRet2 += sum2.reduceLanes(VectorOperators.ADD);
    subRet3 += sum3.reduceLanes(VectorOperators.ADD);
    // tail as bytes
    for (; i < stripeSize; i++) {
      int dValue = d[dOffset + i];
      subRet0 += Integer.bitCount((dValue & q[i]) & 0xFF);
      subRet1 += Integer.bitCount((dValue & q[i + stripeSize]) & 0xFF);
      subRet2 += Integer.bitCount((dValue & q[i + 2 * stripeSize]) & 0xFF);
      subRet3 += Integer.bitCount((dValue & q[i + 3 * stripeSize]) & 0xFF);
    }
    return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
  }

  @Override
  public float minMaxScalarQuantize(
      float[] vector, byte[] dest, float scale, float alpha, float minQuantile, float maxQuantile) {
    assert vector.length == dest.length;
    float correction = 0;
    int i = 0;
    // only vectorize if we have a viable BYTE_SPECIES we can use for output
    if (VECTOR_BITSIZE >= 256) {
      FloatVector sum = FloatVector.zero(FLOAT_SPECIES);

      for (; i < FLOAT_SPECIES.loopBound(vector.length); i += FLOAT_SPECIES.length()) {
        FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);

        // Make sure the value is within the quantile range, cutting off the tails
        // see first parenthesis in equation: byte = (float - minQuantile) * 127/(maxQuantile -
        // minQuantile)
        FloatVector dxc = v.min(maxQuantile).max(minQuantile).sub(minQuantile);
        // Scale the value to the range [0, 127], this is our quantized value
        // scale = 127/(maxQuantile - minQuantile)
        // Math.round rounds to positive infinity, so do the same by +0.5 then truncating to int
        Vector<Integer> roundedDxs =
            fma(dxc, dxc.broadcast(scale), dxc.broadcast(0.5f)).convert(VectorOperators.F2I, 0);
        // output this to the array
        ((ByteVector) roundedDxs.castShape(BYTE_SPECIES, 0)).intoArray(dest, i);
        // We multiply by `alpha` here to get the quantized value back into the original range
        // to aid in calculating the corrective offset
        FloatVector dxq = ((FloatVector) roundedDxs.castShape(FLOAT_SPECIES, 0)).mul(alpha);
        // Calculate the corrective offset that needs to be applied to the score
        // in addition to the `byte * minQuantile * alpha` term in the equation
        // we add the `(dx - dxq) * dxq` term to account for the fact that the quantized value
        // will be rounded to the nearest whole number and lose some accuracy
        // Additionally, we account for the global correction of `minQuantile^2` in the equation
        sum =
            fma(
                v.sub(minQuantile / 2f),
                v.broadcast(minQuantile),
                fma(v.sub(minQuantile).sub(dxq), dxq, sum));
      }

      correction = sum.reduceLanes(VectorOperators.ADD);
    }

    // complete the tail normally
    correction +=
        new DefaultVectorUtilSupport.ScalarQuantizer(alpha, scale, minQuantile, maxQuantile)
            .quantize(vector, dest, i);

    return correction;
  }

  @Override
  public float recalculateScalarQuantizationOffset(
      byte[] vector,
      float oldAlpha,
      float oldMinQuantile,
      float scale,
      float alpha,
      float minQuantile,
      float maxQuantile) {
    float correction = 0;
    int i = 0;
    // only vectorize if we have a viable BYTE_SPECIES that we can use
    if (VECTOR_BITSIZE >= 256) {
      FloatVector sum = FloatVector.zero(FLOAT_SPECIES);

      for (; i < BYTE_SPECIES.loopBound(vector.length); i += BYTE_SPECIES.length()) {
        FloatVector fv =
            (FloatVector)
                ByteVector.fromArray(BYTE_SPECIES, vector, i)
                    .convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0)
                    .castShape(FLOAT_SPECIES, 0);
        // undo the old quantization
        FloatVector v = fma(fv, fv.broadcast(oldAlpha), fv.broadcast(oldMinQuantile));

        // same operations as in quantize above
        FloatVector dxc = v.min(maxQuantile).max(minQuantile).sub(minQuantile);
        Vector<Integer> roundedDxs =
            fma(dxc, dxc.broadcast(scale), dxc.broadcast(0.5f)).convert(VectorOperators.F2I, 0);
        FloatVector dxq = ((FloatVector) roundedDxs.castShape(FLOAT_SPECIES, 0)).mul(alpha);
        sum =
            fma(
                v.sub(minQuantile / 2f),
                v.broadcast(minQuantile),
                fma(v.sub(minQuantile).sub(dxq), dxq, sum));
      }

      correction = sum.reduceLanes(VectorOperators.ADD);
    }

    // complete the tail normally
    correction +=
        new DefaultVectorUtilSupport.ScalarQuantizer(alpha, scale, minQuantile, maxQuantile)
            .recalculateOffset(vector, i, oldAlpha, oldMinQuantile);

    return correction;
  }

  @SuppressForbidden(reason = "Uses compress and cast only where fast and carefully contained")
  @Override
  public int filterByScore(
      int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo) {
    int newUpto = 0;
    int i = 0;
    if (Constants.HAS_FAST_COMPRESS_MASK_CAST) {
      for (int bound = DOUBLE_SPECIES.loopBound(upTo); i < bound; i += DOUBLE_SPECIES.length()) {
        DoubleVector scoreVector = DoubleVector.fromArray(DOUBLE_SPECIES, scoreBuffer, i);
        IntVector docVector = IntVector.fromArray(INT_FOR_DOUBLE_SPECIES, docBuffer, i);
        VectorMask<Double> mask = scoreVector.compare(VectorOperators.GE, minScoreInclusive);
        scoreVector.compress(mask).intoArray(scoreBuffer, newUpto);
        docVector.compress(mask.cast(INT_FOR_DOUBLE_SPECIES)).intoArray(docBuffer, newUpto);
        newUpto += mask.trueCount();
      }
    }

    for (; i < upTo; ++i) {
      int doc = docBuffer[i];
      double score = scoreBuffer[i];
      docBuffer[newUpto] = doc;
      scoreBuffer[newUpto] = score;
      if (score >= minScoreInclusive) {
        newUpto++;
      }
    }
    return newUpto;
  }

  @Override
  public float[] l2normalize(float[] v, boolean throwOnZero) {
    double l1norm = this.dotProduct(v, v);
    if (l1norm == 0) {
      if (throwOnZero) {
        throw new IllegalArgumentException("Cannot normalize a zero-length vector");
      } else {
        return v;
      }
    }
    if (Math.abs(l1norm - 1.0d) <= EPSILON) {
      return v;
    }

    float invNorm = 1.0f / (float) Math.sqrt(l1norm);
    int i = 0;

    // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
    if (v.length > 2 * FLOAT_SPECIES.length()) {
      i += FLOAT_SPECIES.loopBound(v.length);
      l2normalizeBody(v, invNorm, i);
    }

    for (; i < v.length; i++) {
      v[i] *= invNorm;
    }
    return v;
  }

  private void l2normalizeBody(float[] v, float invNorm, int limit) {
    FloatVector invNormVector = FloatVector.broadcast(FLOAT_SPECIES, invNorm);
    int i = 0;
    int unrolledLimit = limit - 3 * FLOAT_SPECIES.length();

    for (; i < unrolledLimit; i += 4 * FLOAT_SPECIES.length()) {
      FloatVector.fromArray(FLOAT_SPECIES, v, i).mul(invNormVector).intoArray(v, i);
      FloatVector.fromArray(FLOAT_SPECIES, v, i + FLOAT_SPECIES.length())
          .mul(invNormVector)
          .intoArray(v, i + FLOAT_SPECIES.length());
      FloatVector.fromArray(FLOAT_SPECIES, v, i + 2 * FLOAT_SPECIES.length())
          .mul(invNormVector)
          .intoArray(v, i + 2 * FLOAT_SPECIES.length());
      FloatVector.fromArray(FLOAT_SPECIES, v, i + 3 * FLOAT_SPECIES.length())
          .mul(invNormVector)
          .intoArray(v, i + 3 * FLOAT_SPECIES.length());
    }

    for (; i < limit; i += FLOAT_SPECIES.length()) {
      FloatVector.fromArray(FLOAT_SPECIES, v, i).mul(invNormVector).intoArray(v, i);
    }
  }

  private static final boolean EXPAND_8_VECTOR_OPTIMIZATION = INT_SPECIES.length() >= 4;

  @Override
  public void expand8(int[] arr) {
    // BLOCK_SIZE is 256
    if (EXPAND_8_VECTOR_OPTIMIZATION) {
      for (int i = 0; i < 64; i += INT_SPECIES.length()) {
        IntVector v = IntVector.fromArray(INT_SPECIES, arr, i);

        v.lanewise(LSHR, 24).intoArray(arr, i);
        v.lanewise(LSHR, 16).and(0xFF).intoArray(arr, 64 + i);
        v.lanewise(LSHR, 8).and(0xFF).intoArray(arr, 128 + i);
        v.and(0xFF).intoArray(arr, 192 + i);
      }
    } else {
      for (int i = 0; i < 64; ++i) {
        int l = arr[i];
        arr[i] = (l >>> 24) & 0xFF;
        arr[64 + i] = (l >>> 16) & 0xFF;
        arr[128 + i] = (l >>> 8) & 0xFF;
        arr[192 + i] = l & 0xFF;
      }
    }
  }
}
