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
import static jdk.incubator.vector.VectorOperators.BIT_COUNT;
import static jdk.incubator.vector.VectorOperators.LSHL;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2I;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2S;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_S2I;
import static org.apache.lucene.internal.vectorization.PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE;

import java.lang.foreign.MemorySegment;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/**
 * Dot products of a quantized query on heap with four quantized vectors read in place from a {@link
 * MemorySegment}. Each kernel reads the four vectors side by side, so that the CPU overlaps their
 * cache misses instead of waiting for the misses of one vector after the other, and writes the four
 * dot products to {@code results[0..3]}.
 *
 * <p>The query and vector layouts are those of {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat}; see the matching
 * single vector methods of {@link org.apache.lucene.util.VectorUtil} for each layout. The results
 * are identical to those methods. The callers pass the vector length, which they know from the
 * index, and a query of exactly the length the layout needs.
 *
 * <p>This class repeats code instead of factoring into helper methods to avoid {@link
 * jdk.incubator.vector.Vector} from being boxed. TODO: simplify after Vector API is finalized.
 */
final class Lucene104MemorySegmentQuantizedVectorOps {

  private static final int VECTOR_BITSIZE = PREFERRED_VECTOR_BITSIZE;

  // full width vectors of bytes, shorts, ints
  private static final VectorSpecies<Byte> BYTE_SPECIES =
      VectorSpecies.of(byte.class, VectorShape.forBitSize(VECTOR_BITSIZE));
  private static final VectorSpecies<Short> SHORT_SPECIES = BYTE_SPECIES.withLanes(short.class);
  private static final VectorSpecies<Integer> INT_SPECIES = BYTE_SPECIES.withLanes(int.class);

  // the 8-bit kernel at 512 bits and up widens bytes to shorts and the products to ints, so it
  // loads a quarter width of bytes, multiplies a half width of shorts and sums a full width of ints
  private static final VectorSpecies<Byte> QUARTER_BYTE_SPECIES =
      VECTOR_BITSIZE >= 512
          ? VectorSpecies.of(byte.class, VectorShape.forBitSize(VECTOR_BITSIZE >> 2))
          : null;
  private static final VectorSpecies<Short> HALF_SHORT_SPECIES =
      VECTOR_BITSIZE >= 512
          ? VectorSpecies.of(short.class, VectorShape.forBitSize(VECTOR_BITSIZE >> 1))
          : null;

  private Lucene104MemorySegmentQuantizedVectorOps() {}

  // -- unsigned bytes (8 bit and 7 bit values)

  /**
   * Dot products of the first {@code length} bytes of {@code q} with the four vectors of that
   * length at the given offsets, all treated as unsigned bytes.
   */
  static void uint8DotProductBulk(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int length, int[] results) {
    results[0] = results[1] = results[2] = results[3] = 0;
    int i = 0;
    // the same width specific kernels as PanamaVectorUtilSupport#uint8DotProduct
    if (length >= 16) {
      if (VECTOR_BITSIZE >= 512) {
        i = QUARTER_BYTE_SPECIES.loopBound(length);
        uint8DotProductBulk512(q, seg, d1, d2, d3, d4, i, results);
      } else if (VECTOR_BITSIZE == 256) {
        i = ByteVector.SPECIES_64.loopBound(length);
        uint8DotProductBulk256(q, seg, d1, d2, d3, d4, i, results);
      } else {
        // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
        i = ByteVector.SPECIES_64.loopBound(length - ByteVector.SPECIES_64.length());
        uint8DotProductBulk128(q, seg, d1, d2, d3, d4, i, results);
      }
    }
    // scalar tail
    for (; i < length; i++) {
      int qi = Byte.toUnsignedInt(q[i]);
      results[0] += qi * Byte.toUnsignedInt(seg.get(JAVA_BYTE, d1 + i));
      results[1] += qi * Byte.toUnsignedInt(seg.get(JAVA_BYTE, d2 + i));
      results[2] += qi * Byte.toUnsignedInt(seg.get(JAVA_BYTE, d3 + i));
      results[3] += qi * Byte.toUnsignedInt(seg.get(JAVA_BYTE, d4 + i));
    }
  }

  /** 512 bit vectors: 16 bit multiply, 32 bit add, like the single vector kernel. */
  private static void uint8DotProductBulk512(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int limit, int[] results) {
    IntVector acc1 = IntVector.zero(INT_SPECIES);
    IntVector acc2 = IntVector.zero(INT_SPECIES);
    IntVector acc3 = IntVector.zero(INT_SPECIES);
    IntVector acc4 = IntVector.zero(INT_SPECIES);
    for (int i = 0; i < limit; i += QUARTER_BYTE_SPECIES.length()) {
      Vector<Short> vq =
          ByteVector.fromArray(QUARTER_BYTE_SPECIES, q, i)
              .convertShape(ZERO_EXTEND_B2S, HALF_SHORT_SPECIES, 0);
      Vector<Short> v1 =
          ByteVector.fromMemorySegment(QUARTER_BYTE_SPECIES, seg, d1 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, HALF_SHORT_SPECIES, 0);
      Vector<Short> v2 =
          ByteVector.fromMemorySegment(QUARTER_BYTE_SPECIES, seg, d2 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, HALF_SHORT_SPECIES, 0);
      Vector<Short> v3 =
          ByteVector.fromMemorySegment(QUARTER_BYTE_SPECIES, seg, d3 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, HALF_SHORT_SPECIES, 0);
      Vector<Short> v4 =
          ByteVector.fromMemorySegment(QUARTER_BYTE_SPECIES, seg, d4 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, HALF_SHORT_SPECIES, 0);
      acc1 = acc1.add(vq.mul(v1).convertShape(ZERO_EXTEND_S2I, INT_SPECIES, 0));
      acc2 = acc2.add(vq.mul(v2).convertShape(ZERO_EXTEND_S2I, INT_SPECIES, 0));
      acc3 = acc3.add(vq.mul(v3).convertShape(ZERO_EXTEND_S2I, INT_SPECIES, 0));
      acc4 = acc4.add(vq.mul(v4).convertShape(ZERO_EXTEND_S2I, INT_SPECIES, 0));
    }
    results[0] += acc1.reduceLanes(ADD);
    results[1] += acc2.reduceLanes(ADD);
    results[2] += acc3.reduceLanes(ADD);
    results[3] += acc4.reduceLanes(ADD);
  }

  /** 256 bit vectors: 32 bit multiply and add, 8 bytes per step. */
  private static void uint8DotProductBulk256(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int limit, int[] results) {
    IntVector acc1 = IntVector.zero(IntVector.SPECIES_256);
    IntVector acc2 = IntVector.zero(IntVector.SPECIES_256);
    IntVector acc3 = IntVector.zero(IntVector.SPECIES_256);
    IntVector acc4 = IntVector.zero(IntVector.SPECIES_256);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length()) {
      Vector<Integer> vq =
          ByteVector.fromArray(ByteVector.SPECIES_64, q, i)
              .convertShape(ZERO_EXTEND_B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> v1 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d1 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> v2 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d2 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> v3 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d3 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2I, IntVector.SPECIES_256, 0);
      Vector<Integer> v4 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d4 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2I, IntVector.SPECIES_256, 0);
      acc1 = acc1.add(vq.mul(v1));
      acc2 = acc2.add(vq.mul(v2));
      acc3 = acc3.add(vq.mul(v3));
      acc4 = acc4.add(vq.mul(v4));
    }
    results[0] += acc1.reduceLanes(ADD);
    results[1] += acc2.reduceLanes(ADD);
    results[2] += acc3.reduceLanes(ADD);
    results[3] += acc4.reduceLanes(ADD);
  }

  /** 128 bit vectors: 4 bytes per step, loading 8 and using the first half of each load. */
  private static void uint8DotProductBulk128(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int limit, int[] results) {
    IntVector acc1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc2 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc3 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc4 = IntVector.zero(IntVector.SPECIES_128);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
      Vector<Short> vq =
          ByteVector.fromArray(ByteVector.SPECIES_64, q, i).convert(ZERO_EXTEND_B2S, 0);
      Vector<Short> v1 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d1 + i, LITTLE_ENDIAN)
              .convert(ZERO_EXTEND_B2S, 0);
      Vector<Short> v2 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d2 + i, LITTLE_ENDIAN)
              .convert(ZERO_EXTEND_B2S, 0);
      Vector<Short> v3 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d3 + i, LITTLE_ENDIAN)
              .convert(ZERO_EXTEND_B2S, 0);
      Vector<Short> v4 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_64, seg, d4 + i, LITTLE_ENDIAN)
              .convert(ZERO_EXTEND_B2S, 0);
      acc1 = acc1.add(vq.mul(v1).convertShape(ZERO_EXTEND_S2I, IntVector.SPECIES_128, 0));
      acc2 = acc2.add(vq.mul(v2).convertShape(ZERO_EXTEND_S2I, IntVector.SPECIES_128, 0));
      acc3 = acc3.add(vq.mul(v3).convertShape(ZERO_EXTEND_S2I, IntVector.SPECIES_128, 0));
      acc4 = acc4.add(vq.mul(v4).convertShape(ZERO_EXTEND_S2I, IntVector.SPECIES_128, 0));
    }
    results[0] += acc1.reduceLanes(ADD);
    results[1] += acc2.reduceLanes(ADD);
    results[2] += acc3.reduceLanes(ADD);
    results[3] += acc4.reduceLanes(ADD);
  }

  // -- 4 bit values

  /**
   * Dot products of the unpacked 4 bit query {@code q} with the four packed 4 bit vectors of {@code
   * packedLength} bytes at the given offsets. The layouts are those of {@link
   * org.apache.lucene.util.VectorUtil#int4DotProductSinglePacked}: the first {@code packedLength}
   * bytes of {@code q} multiply the high nibbles, the next the low nibbles.
   */
  static void int4DotProductSinglePackedBulk(
      byte[] q,
      MemorySegment seg,
      long d1,
      long d2,
      long d3,
      long d4,
      int packedLength,
      int[] results) {
    // Each step adds four products of at most 15 * 15 to a 16 bit lane, twice as many as the
    // single vector kernel, so a chunk is half as many steps as there before the lanes are summed.
    final int chunk = 64 * BYTE_SPECIES.length();
    final int limit = packedLength >= 32 ? BYTE_SPECIES.loopBound(packedLength) : 0;
    int sum1 = 0;
    int sum2 = 0;
    int sum3 = 0;
    int sum4 = 0;
    for (int i = 0; i < limit; i += chunk) {
      ShortVector acc1 = ShortVector.zero(SHORT_SPECIES);
      ShortVector acc2 = ShortVector.zero(SHORT_SPECIES);
      ShortVector acc3 = ShortVector.zero(SHORT_SPECIES);
      ShortVector acc4 = ShortVector.zero(SHORT_SPECIES);
      final int innerLimit = Math.min(limit - i, chunk);
      for (int j = 0; j < innerLimit; j += BYTE_SPECIES.length()) {
        ByteVector hi = ByteVector.fromArray(BYTE_SPECIES, q, i + j);
        ByteVector lo = ByteVector.fromArray(BYTE_SPECIES, q, packedLength + i + j);
        ByteVector p1 = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d1 + i + j, LITTLE_ENDIAN);
        ByteVector p2 = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d2 + i + j, LITTLE_ENDIAN);
        ByteVector p3 = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d3 + i + j, LITTLE_ENDIAN);
        ByteVector p4 = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d4 + i + j, LITTLE_ENDIAN);
        // A product of two nibbles fits in a byte, so the byte products are read as pairs in 16
        // bit lanes and each half is added separately.
        ShortVector hi1 = p1.lanewise(LSHR, 4).mul(hi).reinterpretAsShorts();
        ShortVector lo1 = p1.and((byte) 0x0F).mul(lo).reinterpretAsShorts();
        acc1 =
            acc1.add(hi1.lanewise(LSHR, 8))
                .add(hi1.and((short) 0xFF))
                .add(lo1.lanewise(LSHR, 8))
                .add(lo1.and((short) 0xFF));
        ShortVector hi2 = p2.lanewise(LSHR, 4).mul(hi).reinterpretAsShorts();
        ShortVector lo2 = p2.and((byte) 0x0F).mul(lo).reinterpretAsShorts();
        acc2 =
            acc2.add(hi2.lanewise(LSHR, 8))
                .add(hi2.and((short) 0xFF))
                .add(lo2.lanewise(LSHR, 8))
                .add(lo2.and((short) 0xFF));
        ShortVector hi3 = p3.lanewise(LSHR, 4).mul(hi).reinterpretAsShorts();
        ShortVector lo3 = p3.and((byte) 0x0F).mul(lo).reinterpretAsShorts();
        acc3 =
            acc3.add(hi3.lanewise(LSHR, 8))
                .add(hi3.and((short) 0xFF))
                .add(lo3.lanewise(LSHR, 8))
                .add(lo3.and((short) 0xFF));
        ShortVector hi4 = p4.lanewise(LSHR, 4).mul(hi).reinterpretAsShorts();
        ShortVector lo4 = p4.and((byte) 0x0F).mul(lo).reinterpretAsShorts();
        acc4 =
            acc4.add(hi4.lanewise(LSHR, 8))
                .add(hi4.and((short) 0xFF))
                .add(lo4.lanewise(LSHR, 8))
                .add(lo4.and((short) 0xFF));
      }
      // the lanes are unsigned 16 bit sums
      IntVector ints1 = acc1.reinterpretAsInts();
      sum1 += ints1.and(0xFFFF).add(ints1.lanewise(LSHR, 16)).reduceLanes(ADD);
      IntVector ints2 = acc2.reinterpretAsInts();
      sum2 += ints2.and(0xFFFF).add(ints2.lanewise(LSHR, 16)).reduceLanes(ADD);
      IntVector ints3 = acc3.reinterpretAsInts();
      sum3 += ints3.and(0xFFFF).add(ints3.lanewise(LSHR, 16)).reduceLanes(ADD);
      IntVector ints4 = acc4.reinterpretAsInts();
      sum4 += ints4.and(0xFFFF).add(ints4.lanewise(LSHR, 16)).reduceLanes(ADD);
    }
    // scalar tail
    for (int i = limit; i < packedLength; i++) {
      int hi = q[i];
      int lo = q[packedLength + i];
      sum1 += nibbleDot(seg.get(JAVA_BYTE, d1 + i), hi, lo);
      sum2 += nibbleDot(seg.get(JAVA_BYTE, d2 + i), hi, lo);
      sum3 += nibbleDot(seg.get(JAVA_BYTE, d3 + i), hi, lo);
      sum4 += nibbleDot(seg.get(JAVA_BYTE, d4 + i), hi, lo);
    }
    results[0] = sum1;
    results[1] = sum2;
    results[2] = sum3;
    results[3] = sum4;
  }

  private static int nibbleDot(byte packed, int hi, int lo) {
    return ((packed & 0xFF) >> 4) * hi + (packed & 0x0F) * lo;
  }

  // -- 1 bit and 2 bit values, with a 4 bit query

  /**
   * Dot products of a 4 bit query with the four 1 bit vectors of {@code stripe} bytes at the given
   * offsets. The layouts are those of {@link org.apache.lucene.util.VectorUtil#int4BitDotProduct}:
   * {@code q} holds the four bit planes of the query, each of {@code stripe} bytes, and a vector is
   * packed bits. A dot product is the number of bits set in both the vector and each plane,
   * weighted by the value of the plane.
   */
  static void int4BitDotProductBulk(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int stripe, int[] results) {
    results[0] = results[1] = results[2] = results[3] = 0;
    addInt4BitDotProducts(q, seg, d1, d2, d3, d4, stripe, 0, results);
  }

  /**
   * Dot products of a 4 bit query with the four 2 bit vectors at the given offsets. The layouts are
   * those of {@link org.apache.lucene.util.VectorUtil#int4DibitDotProduct}: a vector is its low bit
   * plane followed by its high bit plane, each a 1 bit vector of {@code stripe} bytes.
   */
  static void int4DibitDotProductBulk(
      byte[] q, MemorySegment seg, long d1, long d2, long d3, long d4, int stripe, int[] results) {
    results[0] = results[1] = results[2] = results[3] = 0;
    addInt4BitDotProducts(q, seg, d1, d2, d3, d4, stripe, 0, results);
    addInt4BitDotProducts(
        q, seg, d1 + stripe, d2 + stripe, d3 + stripe, d4 + stripe, stripe, 1, results);
  }

  /**
   * Adds the dot products of the 4 bit query with the four 1 bit vectors at the given offsets,
   * times {@code 2^shift}, to {@code results[0..3]}. The same shapes as the single vector kernel:
   * 256 bit vectors with 64 bit lanes where available, 128 bit vectors with 32 bit lanes otherwise,
   * and the last bytes one by one.
   */
  private static void addInt4BitDotProducts(
      byte[] q,
      MemorySegment seg,
      long d1,
      long d2,
      long d3,
      long d4,
      int stripe,
      int shift,
      int[] results) {
    int i =
        VECTOR_BITSIZE >= 256
            ? addInt4BitDotProducts256(q, seg, d1, d2, d3, d4, stripe, shift, results)
            : addInt4BitDotProducts128(q, seg, d1, d2, d3, d4, stripe, shift, results);
    for (; i < stripe; i++) {
      int q0 = q[i];
      int q1 = q[stripe + i];
      int q2 = q[2 * stripe + i];
      int q3 = q[3 * stripe + i];
      results[0] += bitDot(seg.get(JAVA_BYTE, d1 + i), q0, q1, q2, q3) << shift;
      results[1] += bitDot(seg.get(JAVA_BYTE, d2 + i), q0, q1, q2, q3) << shift;
      results[2] += bitDot(seg.get(JAVA_BYTE, d3 + i), q0, q1, q2, q3) << shift;
      results[3] += bitDot(seg.get(JAVA_BYTE, d4 + i), q0, q1, q2, q3) << shift;
    }
  }

  private static int bitDot(byte d, int q0, int q1, int q2, int q3) {
    int dv = Byte.toUnsignedInt(d);
    return Integer.bitCount(dv & q0)
        + (Integer.bitCount(dv & q1) << 1)
        + (Integer.bitCount(dv & q2) << 2)
        + (Integer.bitCount(dv & q3) << 3);
  }

  /**
   * 256 bit vectors with 64 bit lanes, then 128 bit vectors for a remainder of 16 bytes and up.
   * Returns the number of bytes done.
   */
  private static int addInt4BitDotProducts256(
      byte[] q,
      MemorySegment seg,
      long d1,
      long d2,
      long d3,
      long d4,
      int stripe,
      int shift,
      int[] results) {
    int limit = ByteVector.SPECIES_256.loopBound(stripe);
    LongVector acc1 = LongVector.zero(LongVector.SPECIES_256);
    LongVector acc2 = LongVector.zero(LongVector.SPECIES_256);
    LongVector acc3 = LongVector.zero(LongVector.SPECIES_256);
    LongVector acc4 = LongVector.zero(LongVector.SPECIES_256);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_256.length()) {
      LongVector q0 = ByteVector.fromArray(ByteVector.SPECIES_256, q, i).reinterpretAsLongs();
      LongVector q1 =
          ByteVector.fromArray(ByteVector.SPECIES_256, q, stripe + i).reinterpretAsLongs();
      LongVector q2 =
          ByteVector.fromArray(ByteVector.SPECIES_256, q, 2 * stripe + i).reinterpretAsLongs();
      LongVector q3 =
          ByteVector.fromArray(ByteVector.SPECIES_256, q, 3 * stripe + i).reinterpretAsLongs();
      LongVector v1 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_256, seg, d1 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc1 =
          acc1.add(q0.and(v1).lanewise(BIT_COUNT))
              .add(q1.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v2 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_256, seg, d2 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc2 =
          acc2.add(q0.and(v2).lanewise(BIT_COUNT))
              .add(q1.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v3 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_256, seg, d3 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc3 =
          acc3.add(q0.and(v3).lanewise(BIT_COUNT))
              .add(q1.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v4 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_256, seg, d4 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc4 =
          acc4.add(q0.and(v4).lanewise(BIT_COUNT))
              .add(q1.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 3));
    }
    if (stripe - limit >= ByteVector.SPECIES_128.length()) {
      int i = limit;
      LongVector q0 = ByteVector.fromArray(ByteVector.SPECIES_128, q, i).reinterpretAsLongs();
      LongVector q1 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, stripe + i).reinterpretAsLongs();
      LongVector q2 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 2 * stripe + i).reinterpretAsLongs();
      LongVector q3 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 3 * stripe + i).reinterpretAsLongs();
      LongVector v1 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d1 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      LongVector v2 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d2 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      LongVector v3 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d3 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      LongVector v4 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d4 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      results[0] +=
          (int)
                  q0.and(v1)
                      .lanewise(BIT_COUNT)
                      .add(q1.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 1))
                      .add(q2.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 2))
                      .add(q3.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 3))
                      .reduceLanes(ADD)
              << shift;
      results[1] +=
          (int)
                  q0.and(v2)
                      .lanewise(BIT_COUNT)
                      .add(q1.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 1))
                      .add(q2.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 2))
                      .add(q3.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 3))
                      .reduceLanes(ADD)
              << shift;
      results[2] +=
          (int)
                  q0.and(v3)
                      .lanewise(BIT_COUNT)
                      .add(q1.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 1))
                      .add(q2.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 2))
                      .add(q3.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 3))
                      .reduceLanes(ADD)
              << shift;
      results[3] +=
          (int)
                  q0.and(v4)
                      .lanewise(BIT_COUNT)
                      .add(q1.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 1))
                      .add(q2.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 2))
                      .add(q3.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 3))
                      .reduceLanes(ADD)
              << shift;
      limit += ByteVector.SPECIES_128.length();
    }
    results[0] += (int) acc1.reduceLanes(ADD) << shift;
    results[1] += (int) acc2.reduceLanes(ADD) << shift;
    results[2] += (int) acc3.reduceLanes(ADD) << shift;
    results[3] += (int) acc4.reduceLanes(ADD) << shift;
    return limit;
  }

  /** 128 bit vectors with 32 bit lanes. Returns the number of bytes done. */
  private static int addInt4BitDotProducts128(
      byte[] q,
      MemorySegment seg,
      long d1,
      long d2,
      long d3,
      long d4,
      int stripe,
      int shift,
      int[] results) {
    int limit = ByteVector.SPECIES_128.loopBound(stripe);
    IntVector acc1 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc2 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc3 = IntVector.zero(IntVector.SPECIES_128);
    IntVector acc4 = IntVector.zero(IntVector.SPECIES_128);
    for (int i = 0; i < limit; i += ByteVector.SPECIES_128.length()) {
      IntVector q0 = ByteVector.fromArray(ByteVector.SPECIES_128, q, i).reinterpretAsInts();
      IntVector q1 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, stripe + i).reinterpretAsInts();
      IntVector q2 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 2 * stripe + i).reinterpretAsInts();
      IntVector q3 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 3 * stripe + i).reinterpretAsInts();
      IntVector v1 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d1 + i, LITTLE_ENDIAN)
              .reinterpretAsInts();
      acc1 =
          acc1.add(q0.and(v1).lanewise(BIT_COUNT))
              .add(q1.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      IntVector v2 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d2 + i, LITTLE_ENDIAN)
              .reinterpretAsInts();
      acc2 =
          acc2.add(q0.and(v2).lanewise(BIT_COUNT))
              .add(q1.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      IntVector v3 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d3 + i, LITTLE_ENDIAN)
              .reinterpretAsInts();
      acc3 =
          acc3.add(q0.and(v3).lanewise(BIT_COUNT))
              .add(q1.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      IntVector v4 =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d4 + i, LITTLE_ENDIAN)
              .reinterpretAsInts();
      acc4 =
          acc4.add(q0.and(v4).lanewise(BIT_COUNT))
              .add(q1.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 3));
    }
    results[0] += acc1.reduceLanes(ADD) << shift;
    results[1] += acc2.reduceLanes(ADD) << shift;
    results[2] += acc3.reduceLanes(ADD) << shift;
    results[3] += acc4.reduceLanes(ADD) << shift;
    return limit;
  }
}
