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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.BIT_COUNT;
import static jdk.incubator.vector.VectorOperators.LSHL;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2I;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2S;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_S2I;
import static org.apache.lucene.internal.vectorization.PanamaVectorConstants.PRERERRED_INT_SPECIES;
import static org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport.BYTE_SPECIES;
import static org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport.BYTE_SPECIES_FULL;
import static org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport.SHORT_SPECIES;
import static org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport.VECTOR_BITSIZE;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
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
final class MemorySegmentQuantizedVectorOps {

  // Reads query longs in the order of the segment reads, which is the native one. (The handles in
  // BitUtil use a randomized order in tests.)
  private static final VarHandle NATIVE_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

  private static final VectorSpecies<Long> LONG_SPECIES = BYTE_SPECIES_FULL.withLanes(long.class);

  private MemorySegmentQuantizedVectorOps() {}

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
        i = BYTE_SPECIES.loopBound(length);
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
    IntVector acc1 = IntVector.zero(PRERERRED_INT_SPECIES);
    IntVector acc2 = IntVector.zero(PRERERRED_INT_SPECIES);
    IntVector acc3 = IntVector.zero(PRERERRED_INT_SPECIES);
    IntVector acc4 = IntVector.zero(PRERERRED_INT_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES.length()) {
      Vector<Short> vq =
          ByteVector.fromArray(BYTE_SPECIES, q, i).convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0);
      Vector<Short> v1 =
          ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d1 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0);
      Vector<Short> v2 =
          ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d2 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0);
      Vector<Short> v3 =
          ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d3 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0);
      Vector<Short> v4 =
          ByteVector.fromMemorySegment(BYTE_SPECIES, seg, d4 + i, LITTLE_ENDIAN)
              .convertShape(ZERO_EXTEND_B2S, SHORT_SPECIES, 0);
      acc1 = acc1.add(vq.mul(v1).convertShape(ZERO_EXTEND_S2I, PRERERRED_INT_SPECIES, 0));
      acc2 = acc2.add(vq.mul(v2).convertShape(ZERO_EXTEND_S2I, PRERERRED_INT_SPECIES, 0));
      acc3 = acc3.add(vq.mul(v3).convertShape(ZERO_EXTEND_S2I, PRERERRED_INT_SPECIES, 0));
      acc4 = acc4.add(vq.mul(v4).convertShape(ZERO_EXTEND_S2I, PRERERRED_INT_SPECIES, 0));
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
    final VectorSpecies<Byte> species = PanamaVectorUtilSupport.Int4Constants.BYTE_SPECIES;
    final VectorSpecies<Short> shortSpecies = PanamaVectorUtilSupport.Int4Constants.SHORT_SPECIES;
    // Half the chunk of the single vector kernel: each step adds four products of at most 15 * 15
    // to a 16 bit lane, twice as many as there.
    final int chunk = PanamaVectorUtilSupport.Int4Constants.CHUNK >> 1;
    final int limit = packedLength >= 32 ? species.loopBound(packedLength) : 0;
    int sum1 = 0;
    int sum2 = 0;
    int sum3 = 0;
    int sum4 = 0;
    for (int i = 0; i < limit; i += chunk) {
      ShortVector acc1 = ShortVector.zero(shortSpecies);
      ShortVector acc2 = ShortVector.zero(shortSpecies);
      ShortVector acc3 = ShortVector.zero(shortSpecies);
      ShortVector acc4 = ShortVector.zero(shortSpecies);
      final int innerLimit = Math.min(limit - i, chunk);
      for (int j = 0; j < innerLimit; j += species.length()) {
        ByteVector hi = ByteVector.fromArray(species, q, i + j);
        ByteVector lo = ByteVector.fromArray(species, q, packedLength + i + j);
        ByteVector p1 = ByteVector.fromMemorySegment(species, seg, d1 + i + j, LITTLE_ENDIAN);
        ByteVector p2 = ByteVector.fromMemorySegment(species, seg, d2 + i + j, LITTLE_ENDIAN);
        ByteVector p3 = ByteVector.fromMemorySegment(species, seg, d3 + i + j, LITTLE_ENDIAN);
        ByteVector p4 = ByteVector.fromMemorySegment(species, seg, d4 + i + j, LITTLE_ENDIAN);
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
   * times {@code 2^shift}, to {@code results[0..3]}. Bits are counted in 64 bit lanes, like the
   * single vector kernel does for 256 bit vectors and up.
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
    final int limit = BYTE_SPECIES_FULL.loopBound(stripe);
    LongVector acc1 = LongVector.zero(LONG_SPECIES);
    LongVector acc2 = LongVector.zero(LONG_SPECIES);
    LongVector acc3 = LongVector.zero(LONG_SPECIES);
    LongVector acc4 = LongVector.zero(LONG_SPECIES);
    for (int i = 0; i < limit; i += BYTE_SPECIES_FULL.length()) {
      LongVector q0 = ByteVector.fromArray(BYTE_SPECIES_FULL, q, i).reinterpretAsLongs();
      LongVector q1 = ByteVector.fromArray(BYTE_SPECIES_FULL, q, stripe + i).reinterpretAsLongs();
      LongVector q2 =
          ByteVector.fromArray(BYTE_SPECIES_FULL, q, 2 * stripe + i).reinterpretAsLongs();
      LongVector q3 =
          ByteVector.fromArray(BYTE_SPECIES_FULL, q, 3 * stripe + i).reinterpretAsLongs();
      LongVector v1 =
          ByteVector.fromMemorySegment(BYTE_SPECIES_FULL, seg, d1 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc1 =
          acc1.add(q0.and(v1).lanewise(BIT_COUNT))
              .add(q1.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v1).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v2 =
          ByteVector.fromMemorySegment(BYTE_SPECIES_FULL, seg, d2 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc2 =
          acc2.add(q0.and(v2).lanewise(BIT_COUNT))
              .add(q1.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v2).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v3 =
          ByteVector.fromMemorySegment(BYTE_SPECIES_FULL, seg, d3 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc3 =
          acc3.add(q0.and(v3).lanewise(BIT_COUNT))
              .add(q1.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v3).lanewise(BIT_COUNT).lanewise(LSHL, 3));
      LongVector v4 =
          ByteVector.fromMemorySegment(BYTE_SPECIES_FULL, seg, d4 + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      acc4 =
          acc4.add(q0.and(v4).lanewise(BIT_COUNT))
              .add(q1.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 1))
              .add(q2.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 2))
              .add(q3.and(v4).lanewise(BIT_COUNT).lanewise(LSHL, 3));
    }
    results[0] += ((int) acc1.reduceLanes(ADD) + bitDotTail(q, seg, d1, limit, stripe)) << shift;
    results[1] += ((int) acc2.reduceLanes(ADD) + bitDotTail(q, seg, d2, limit, stripe)) << shift;
    results[2] += ((int) acc3.reduceLanes(ADD) + bitDotTail(q, seg, d3, limit, stripe)) << shift;
    results[3] += ((int) acc4.reduceLanes(ADD) + bitDotTail(q, seg, d4, limit, stripe)) << shift;
  }

  /**
   * The tail of {@link #addInt4BitDotProducts}: bytes {@code from} to {@code stripe}, in 128 bit
   * steps while they fit and then in longs and bytes, like the single vector kernel.
   */
  private static int bitDotTail(byte[] q, MemorySegment seg, long d, int from, int stripe) {
    int sum = 0;
    int i = from;
    for (; i + ByteVector.SPECIES_128.length() <= stripe; i += ByteVector.SPECIES_128.length()) {
      LongVector v =
          ByteVector.fromMemorySegment(ByteVector.SPECIES_128, seg, d + i, LITTLE_ENDIAN)
              .reinterpretAsLongs();
      LongVector q0 = ByteVector.fromArray(ByteVector.SPECIES_128, q, i).reinterpretAsLongs();
      LongVector q1 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, stripe + i).reinterpretAsLongs();
      LongVector q2 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 2 * stripe + i).reinterpretAsLongs();
      LongVector q3 =
          ByteVector.fromArray(ByteVector.SPECIES_128, q, 3 * stripe + i).reinterpretAsLongs();
      sum +=
          (int)
              q0.and(v)
                  .lanewise(BIT_COUNT)
                  .add(q1.and(v).lanewise(BIT_COUNT).lanewise(LSHL, 1))
                  .add(q2.and(v).lanewise(BIT_COUNT).lanewise(LSHL, 2))
                  .add(q3.and(v).lanewise(BIT_COUNT).lanewise(LSHL, 3))
                  .reduceLanes(ADD);
    }
    for (; i + Long.BYTES <= stripe; i += Long.BYTES) {
      long dv = seg.get(JAVA_LONG_UNALIGNED, d + i);
      sum +=
          Long.bitCount(dv & (long) NATIVE_LONG.get(q, i))
              + (Long.bitCount(dv & (long) NATIVE_LONG.get(q, stripe + i)) << 1)
              + (Long.bitCount(dv & (long) NATIVE_LONG.get(q, 2 * stripe + i)) << 2)
              + (Long.bitCount(dv & (long) NATIVE_LONG.get(q, 3 * stripe + i)) << 3);
    }
    for (; i < stripe; i++) {
      int dv = Byte.toUnsignedInt(seg.get(JAVA_BYTE, d + i));
      sum +=
          Integer.bitCount(dv & q[i])
              + (Integer.bitCount(dv & q[stripe + i]) << 1)
              + (Integer.bitCount(dv & q[2 * stripe + i]) << 2)
              + (Integer.bitCount(dv & q[3 * stripe + i]) << 3);
    }
    return sum;
  }
}
