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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.VectorUtil;

/**
 * Small SIMD surface. The nested Panama implementation is only loaded when the incubator module is
 * present, so the codec also works without it.
 */
class VectorKernels {
  static final VectorKernels INSTANCE = load();

  private static VectorKernels load() {
    if (ModuleLayer.boot().findModule("jdk.incubator.vector").isEmpty()) return new VectorKernels();
    try {
      Class<?> panama = Class.forName(VectorKernels.class.getName() + "$Panama");
      return (VectorKernels) panama.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Single-pair Hamming distance; {@link VectorUtil} already provides a vectorized popcount. */
  int hamming(byte[] a, byte[] b) {
    return VectorUtil.xorBitCount(a, b);
  }

  /**
   * Hamming distances from {@code query} to each admitted row of a contiguous block of {@code
   * stride}-byte records. Rows that are not admitted are left untouched.
   */
  void hammingBulk(
      byte[] query, MemorySegment records, int stride, int rows, boolean[] admitted, int[] out) {
    for (int row = 0; row < rows; row++) {
      if (admitted[row]) out[row] = hammingTail(query, records, (long) row * stride, 0);
    }
  }

  static int hammingTail(byte[] query, MemorySegment records, long offset, int from) {
    int sum = 0;
    for (int i = from; i < query.length; i++) {
      sum += Integer.bitCount((query[i] ^ records.get(ValueLayout.JAVA_BYTE, offset + i)) & 255);
    }
    return sum;
  }

  double distance(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double d = (double) a[i] - b[i];
      sum += d * d;
    }
    return Math.sqrt(sum);
  }

  /** Preferred-width kernels with scalar tails; no per-dimension or per-layout specializations. */
  static final class Panama extends VectorKernels {
    private static final VectorSpecies<Long> LONGS = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Byte> BYTES = LONGS.withLanes(byte.class);
    private static final VectorSpecies<Double> DOUBLES = DoubleVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Float> FLOATS =
        VectorSpecies.of(float.class, VectorShape.forBitSize(DOUBLES.vectorBitSize() / 2));
    private static final int WIDTH = BYTES.length();

    // Measured at 1M: 11% faster clustering than the inherited VectorUtil.xorBitCount, which is
    // called
    // about a thousand times per routed document on 256-byte codes.
    @Override
    int hamming(byte[] a, byte[] b) {
      var acc = LongVector.zero(LONGS);
      int i = 0;
      for (; i <= a.length - WIDTH; i += WIDTH) {
        acc = acc.add(bitCount(a, i, ByteVector.fromArray(BYTES, b, i).reinterpretAsLongs()));
      }
      int sum = (int) acc.reduceLanes(VectorOperators.ADD);
      for (; i < a.length; i++) sum += Integer.bitCount((a[i] ^ b[i]) & 255);
      return sum;
    }

    // Popcounts accumulate in vector lanes and reduce once per record. Keep this loop inline and
    // single-accumulator: C2 compiles it as well as a hand-unrolled per-dimension specialization.
    @Override
    void hammingBulk(
        byte[] query, MemorySegment records, int stride, int rows, boolean[] admitted, int[] out) {
      // Long loads are fastest but need a native (mapped) segment; heap blocks load as bytes.
      boolean mapped = records.isNative();
      for (int row = 0; row < rows; row++) {
        if (admitted[row] == false) continue;
        long offset = (long) row * stride;
        var acc = LongVector.zero(LONGS);
        int i = 0;
        for (; i <= query.length - WIDTH; i += WIDTH) {
          LongVector record =
              mapped
                  ? LongVector.fromMemorySegment(
                      LONGS, records, offset + i, ByteOrder.nativeOrder())
                  : ByteVector.fromMemorySegment(
                          BYTES, records, offset + i, ByteOrder.nativeOrder())
                      .reinterpretAsLongs();
          acc = acc.add(bitCount(query, i, record));
        }
        out[row] =
            (int) acc.reduceLanes(VectorOperators.ADD) + hammingTail(query, records, offset, i);
      }
    }

    private static LongVector bitCount(byte[] query, int i, LongVector record) {
      return ByteVector.fromArray(BYTES, query, i)
          .reinterpretAsLongs()
          .lanewise(VectorOperators.XOR, record)
          .lanewise(VectorOperators.BIT_COUNT);
    }

    @Override
    double distance(float[] a, float[] b) {
      var sum = DoubleVector.zero(DOUBLES);
      int i = 0;
      for (; i < FLOATS.loopBound(a.length); i += FLOATS.length()) {
        var x =
            (DoubleVector)
                FloatVector.fromArray(FLOATS, a, i).convertShape(VectorOperators.F2D, DOUBLES, 0);
        var y =
            (DoubleVector)
                FloatVector.fromArray(FLOATS, b, i).convertShape(VectorOperators.F2D, DOUBLES, 0);
        var d = x.sub(y);
        sum = sum.add(d.mul(d));
      }
      double result = sum.reduceLanes(VectorOperators.ADD);
      for (; i < a.length; i++) {
        double d = (double) a[i] - b[i];
        result += d * d;
      }
      return Math.sqrt(result);
    }
  }
}
