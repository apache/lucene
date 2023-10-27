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
import static jdk.incubator.vector.VectorOperators.S2I;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.MemorySegmentIndexInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

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

  static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

  static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(LE);

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
  public float dotProduct(float[] a, RandomAccessVectorValues<float[]> b, int bOrd)
      throws IOException {
    assert b.dimension() == a.length;
    if (b.getIndexInput() instanceof MemorySegmentIndexInput bIndex) {
      final int vecByteSize = b.byteSize();
      final long bOffset = (long) bOrd * vecByteSize;
      var ms = bIndex.getBackingStorageFor(bOffset, vecByteSize);
      if (ms != null) {
        return dotProduct(a, ms, bIndex.maskedPosition(bOffset));
      }
    }
    return dotProduct(a, b.vectorValue(bOrd));
  }

  private float dotProduct(float[] a, MemorySegment b, long bOffset) {
    int i = 0;
    int j = 0;
    // vector loop is unrolled 4x (4 accumulators in parallel)
    FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc4 = FloatVector.zero(FLOAT_SPECIES);
    int limit = FLOAT_SPECIES.loopBound(a.length);
    int unrolledLimit = limit - 3 * FLOAT_SPECIES.length();
    for (;
        i < unrolledLimit;
        i += 4 * FLOAT_SPECIES.length(), j += 4 * FLOAT_SPECIES.vectorByteSize()) {
      // one
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, b, bOffset + j, LE);
      acc1 = acc1.add(va.mul(vb));
      // two
      FloatVector vc = FloatVector.fromArray(FLOAT_SPECIES, a, i + FLOAT_SPECIES.length());
      FloatVector vd =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + j + FLOAT_SPECIES.vectorByteSize(), LE);
      acc2 = acc2.add(vc.mul(vd));
      // three
      FloatVector ve = FloatVector.fromArray(FLOAT_SPECIES, a, i + 2 * FLOAT_SPECIES.length());
      FloatVector vf =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + j + 2 * FLOAT_SPECIES.vectorByteSize(), LE);
      acc3 = acc3.add(ve.mul(vf));
      // four
      FloatVector vg = FloatVector.fromArray(FLOAT_SPECIES, a, i + 3 * FLOAT_SPECIES.length());
      FloatVector vh =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + j + 3 * FLOAT_SPECIES.vectorByteSize(), LE);
      acc4 = acc4.add(vg.mul(vh));
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < limit; i += FLOAT_SPECIES.length(), j += FLOAT_SPECIES.vectorByteSize()) {
      FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, a, i);
      FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, b, bOffset + j, LE);
      acc1 = acc1.add(va.mul(vb));
    }
    // reduce
    FloatVector res1 = acc1.add(acc2);
    FloatVector res2 = acc3.add(acc4);
    float res = res1.add(res2).reduceLanes(ADD);

    for (; i < a.length; i++, j += Float.BYTES) {
      res += a[i] * b.get(LAYOUT_LE_FLOAT, bOffset + j);
    }
    return res;
  }

  @Override
  public float dotProduct(
      RandomAccessVectorValues<float[]> a, int aOrd, RandomAccessVectorValues<float[]> b, int bOrd)
      throws IOException {
    assert a.dimension() == b.dimension();
    if (a.getIndexInput() instanceof MemorySegmentIndexInput aIndex // This variant is most popular?
        && b.getIndexInput() instanceof MemorySegmentIndexInput bIndex) {
      final int vecByteSize = a.byteSize();
      final long aOffset = (long) aOrd * vecByteSize;
      final long bOffset = (long) bOrd * vecByteSize;
      MemorySegment msa = aIndex.getBackingStorageFor(aOffset, vecByteSize);
      MemorySegment msb = bIndex.getBackingStorageFor(bOffset, vecByteSize);
      if (msa != null && msb != null) {
        return dotProduct(
            msa,
            aIndex.maskedPosition(aOffset),
            msb,
            bIndex.maskedPosition(bOffset),
            a.dimension());
      }
    }
    return dotProduct(a.vectorValue(aOrd), b.vectorValue(bOrd));
  }

  private float dotProduct(
      MemorySegment a, long aOffset, MemorySegment b, long bOffset, int length) {
    int i = 0;
    // vector loop is unrolled 4x (4 accumulators in parallel)
    FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES);
    FloatVector acc4 = FloatVector.zero(FLOAT_SPECIES);
    int limit = length * Float.BYTES; // limit, in bytes
    int vecLimit = FLOAT_SPECIES.loopBound(length) * Float.BYTES; // vector limit, in bytes
    int unrolledLimit = vecLimit - 3 * FLOAT_SPECIES.vectorByteSize();
    for (; i < unrolledLimit; i += 4 * FLOAT_SPECIES.vectorByteSize()) {
      // one
      FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, a, aOffset + i, LE);
      FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, b, bOffset + i, LE);
      acc1 = acc1.add(va.mul(vb));
      // two
      FloatVector vc =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, a, aOffset + i + FLOAT_SPECIES.vectorByteSize(), LE);
      FloatVector vd =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + i + FLOAT_SPECIES.vectorByteSize(), LE);
      acc2 = acc2.add(vc.mul(vd));
      // three
      FloatVector ve =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, a, aOffset + i + 2 * FLOAT_SPECIES.vectorByteSize(), LE);
      FloatVector vf =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + i + 2 * FLOAT_SPECIES.vectorByteSize(), LE);
      acc3 = acc3.add(ve.mul(vf));
      // four
      FloatVector vg =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, a, aOffset + i + 3 * FLOAT_SPECIES.vectorByteSize(), LE);
      FloatVector vh =
          FloatVector.fromMemorySegment(
              FLOAT_SPECIES, b, bOffset + i + 3 * FLOAT_SPECIES.vectorByteSize(), LE);
      acc4 = acc4.add(vg.mul(vh));
    }
    // vector tail: less scalar computations for unaligned sizes, esp with big vector sizes
    for (; i < vecLimit; i += FLOAT_SPECIES.vectorByteSize()) {
      FloatVector va = FloatVector.fromMemorySegment(FLOAT_SPECIES, a, aOffset + i, LE);
      FloatVector vb = FloatVector.fromMemorySegment(FLOAT_SPECIES, b, bOffset + i, LE);
      acc1 = acc1.add(va.mul(vb));
    }
    // reduce
    FloatVector res1 = acc1.add(acc2);
    FloatVector res2 = acc3.add(acc4);
    float res = res1.add(res2).reduceLanes(ADD);

    for (; i < limit; i += Float.BYTES) {
      res += a.get(LAYOUT_LE_FLOAT, aOffset + i) * b.get(LAYOUT_LE_FLOAT, bOffset + i);
    }
    return res;
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
      float elem1 = a[i];
      float elem2 = b[i];
      sum += elem1 * elem2;
      norm1 += elem1 * elem1;
      norm2 += elem2 * elem2;
    }
    return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
  }

  /** vectorized cosine body */
  private float[] cosineBody(float[] a, float[] b, int limit) {
    int i = 0;
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
    int unrolledLimit = limit - 3 * FLOAT_SPECIES.length();
    for (; i < unrolledLimit; i += 4 * FLOAT_SPECIES.length()) {
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
    for (; i < limit; i += FLOAT_SPECIES.length()) {
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
    return new float[] {
      sumres1.add(sumres2).reduceLanes(ADD),
      norm1res1.add(norm1res2).reduceLanes(ADD),
      norm2res1.add(norm2res2).reduceLanes(ADD)
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
