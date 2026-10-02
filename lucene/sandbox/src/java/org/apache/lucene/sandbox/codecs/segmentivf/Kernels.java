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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Scalar kernels with transparent SIMD replacements for coarse scans, packing, and INT8 reranking.
 */
class Kernels {
  static final Kernels INSTANCE = load();

  private static Kernels load() {
    boolean simd = ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    try {
      var type = Class.forName(Kernels.class.getName() + (simd ? "$Panama" : ""));
      return (Kernels) type.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException | LinkageError _) {
      return new Kernels();
    }
  }

  void hamming(byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
    for (int r = 0; r < rows; r++) out[r] = hamming(q, codes, offset + (long) r * q.length);
  }

  int hamming(byte[] q, MemorySegment codes, long offset) {
    return tail(q, codes, offset, 0);
  }

  void hamming(byte[] q, byte[] codes, int offset, int rows, int[] out) {
    for (int r = 0; r < rows; r++) out[r] = hamming(q, codes, offset + r * q.length);
  }

  final void hammingAt(byte[] q, byte[] codes, int[] offsets, int count, int[] out) {
    for (int i = 0; i < count; i++) out[i] = hamming(q, codes, offsets[i]);
  }

  int hamming(byte[] q, byte[] codes, int offset) {
    return scalarHamming(q, codes, offset);
  }

  private static int scalarHamming(byte[] q, byte[] codes, int offset) {
    int sum = 0;
    for (int i = 0; i < q.length; i++) {
      sum += Integer.bitCount((q[i] ^ codes[offset + i]) & 255);
    }
    return sum;
  }

  static int tail(byte[] q, MemorySegment codes, long offset, int from) {
    int sum = 0;
    for (int i = from; i < q.length; i++)
      sum += Integer.bitCount((q[i] ^ codes.get(ValueLayout.JAVA_BYTE, offset + i)) & 255);
    return sum;
  }

  int filterAtMost(int[] distances, int from, int count, int threshold, int[] out) {
    int kept = 0;
    for (int i = 0; i < count; i++) if (distances[from + i] <= threshold) out[kept++] = i;
    return kept;
  }

  int dotProduct(byte[] query, byte[] records, int offset) {
    int sum = 0;
    for (int i = 0; i < query.length; i++) sum += query[i] * records[offset + i];
    return sum;
  }

  void dotProducts(byte[] query, byte[] records, int stride, int count, int[] out) {
    for (int i = 0; i < count; i++) out[i] = dotProduct(query, records, i * stride);
  }

  /** Dot products of {@code query} with the records starting at {@code offsets} in place. */
  void dotProducts(byte[] query, MemorySegment records, long[] offsets, int count, int[] out) {
    for (int r = 0; r < count; r++) {
      int sum = 0;
      for (int i = 0; i < query.length; i++) {
        sum += query[i] * records.get(ValueLayout.JAVA_BYTE, offsets[r] + i);
      }
      out[r] = sum;
    }
  }

  void pack2(float[] vector, int dim, byte[] dest, int offset, float low, float high) {
    int planeBytes = (dim + 7) >>> 3;
    java.util.Arrays.fill(dest, offset, offset + 2 * planeBytes, (byte) 0);
    for (int d = 0; d < dim; d++) {
      int mask = 1 << (d & 7), at = offset + (d >>> 3);
      if (vector[d] >= low) dest[at] |= (byte) mask;
      if (vector[d] >= high) dest[at + planeBytes] |= (byte) mask;
    }
  }

  static final class Panama extends Kernels {
    private static final VectorSpecies<Long> LONGS = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Byte> BYTES = LONGS.withLanes(byte.class);
    private static final VectorSpecies<Byte> DOT_BYTES = ByteVector.SPECIES_64;
    private static final VectorSpecies<Integer> DOT_INTS = IntVector.SPECIES_256;
    private static final VectorSpecies<Float> FLOATS = FloatVector.SPECIES_PREFERRED;
    private static final boolean DOT_IS_256 =
        IntVector.SPECIES_PREFERRED.vectorBitSize() == DOT_INTS.vectorBitSize();

    @Override
    void hamming(byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
      if (codes.isNative()) {
        if (q.length == BYTES.length() * 8) {
          hamming8Native(q, codes, offset, rows, out);
          return;
        }
        if (q.length == BYTES.length() * 4) {
          hamming4Native(q, codes, offset, rows, out);
          return;
        }
      }
      if (codes.isNative()) {
        // Whole vectors in the SIMD loop; any tail is added separately so the loop stays inlined.
        int prefix = q.length - q.length % BYTES.length();
        hammingRows(q, codes, offset, rows, out, prefix);
        if (prefix < q.length) {
          for (int r = 0; r < rows; r++) {
            out[r] += wordTail(q, codes, offset + (long) r * q.length, prefix);
          }
        }
        return;
      }
      super.hamming(q, codes, offset, rows, out);
    }

    private static void hamming4Native(
        byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
      int step = BYTES.length(), len = 4 * step;
      var q0 = query(q, 0);
      var q1 = query(q, step);
      var q2 = query(q, 2 * step);
      var q3 = query(q, 3 * step);
      for (int r = 0; r < rows; r++) {
        long at = offset + (long) r * len;
        var s0 = popcount(q0, code(codes, at));
        var s1 = popcount(q1, code(codes, at + step));
        var s2 = popcount(q2, code(codes, at + 2L * step));
        var s3 = popcount(q3, code(codes, at + 3L * step));
        out[r] = (int) s0.add(s1).add(s2.add(s3)).reduceLanes(VectorOperators.ADD);
      }
    }

    private static void hamming8Native(
        byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
      int step = BYTES.length(), len = 8 * step;
      var q0 = query(q, 0);
      var q1 = query(q, step);
      var q2 = query(q, 2 * step);
      var q3 = query(q, 3 * step);
      var q4 = query(q, 4 * step);
      var q5 = query(q, 5 * step);
      var q6 = query(q, 6 * step);
      var q7 = query(q, 7 * step);
      for (int r = 0; r < rows; r++) {
        long at = offset + (long) r * len;
        var s0 = popcount(q0, code(codes, at));
        var s1 = popcount(q1, code(codes, at + step));
        var s2 = popcount(q2, code(codes, at + 2L * step));
        var s3 = popcount(q3, code(codes, at + 3L * step));
        var s4 = popcount(q4, code(codes, at + 4L * step));
        var s5 = popcount(q5, code(codes, at + 5L * step));
        var s6 = popcount(q6, code(codes, at + 6L * step));
        var s7 = popcount(q7, code(codes, at + 7L * step));
        out[r] =
            (int)
                s0.add(s1)
                    .add(s2.add(s3))
                    .add(s4.add(s5).add(s6.add(s7)))
                    .reduceLanes(VectorOperators.ADD);
      }
    }

    /**
     * Scores four rows together so each query chunk is loaded once without retaining an entire
     * dimension-specific query in registers.
     */
    private static void hammingRows(
        byte[] q, MemorySegment codes, long offset, int rows, int[] out, int prefix) {
      int r = 0;
      for (; r + 4 <= rows; r += 4) {
        long row0 = offset + (long) r * q.length;
        long row1 = row0 + q.length, row2 = row1 + q.length, row3 = row2 + q.length;
        var sum0 = LongVector.zero(LONGS);
        var sum1 = LongVector.zero(LONGS);
        var sum2 = LongVector.zero(LONGS);
        var sum3 = LongVector.zero(LONGS);
        for (int i = 0; i < prefix; i += BYTES.length()) {
          var query = query(q, i);
          sum0 = sum0.add(popcount(query, code(codes, row0 + i)));
          sum1 = sum1.add(popcount(query, code(codes, row1 + i)));
          sum2 = sum2.add(popcount(query, code(codes, row2 + i)));
          sum3 = sum3.add(popcount(query, code(codes, row3 + i)));
        }
        out[r] = (int) sum0.reduceLanes(VectorOperators.ADD);
        out[r + 1] = (int) sum1.reduceLanes(VectorOperators.ADD);
        out[r + 2] = (int) sum2.reduceLanes(VectorOperators.ADD);
        out[r + 3] = (int) sum3.reduceLanes(VectorOperators.ADD);
      }
      for (; r < rows; r++) {
        long row = offset + (long) r * q.length;
        var sum = LongVector.zero(LONGS);
        for (int i = 0; i < prefix; i += BYTES.length()) {
          sum = sum.add(popcount(query(q, i), code(codes, row + i)));
        }
        out[r] = (int) sum.reduceLanes(VectorOperators.ADD);
      }
    }

    @Override
    void hamming(byte[] q, byte[] codes, int offset, int rows, int[] out) {
      if (q.length == BYTES.length() * 4 || q.length == BYTES.length() * 8) {
        for (int r = 0; r < rows; r++) {
          out[r] = hamming(q, codes, offset + r * q.length);
        }
        return;
      }
      if ((q.length & 31) == 0) {
        int r = 0;
        for (; r + 4 <= rows; r += 4) {
          int row0 = offset + r * q.length;
          int row1 = row0 + q.length, row2 = row1 + q.length, row3 = row2 + q.length;
          var sum0 = LongVector.zero(LONGS);
          var sum1 = LongVector.zero(LONGS);
          var sum2 = LongVector.zero(LONGS);
          var sum3 = LongVector.zero(LONGS);
          int i = 0;
          for (; i <= q.length - BYTES.length(); i += BYTES.length()) {
            var query = query(q, i);
            sum0 = sum0.add(popcount(query, code(codes, row0 + i)));
            sum1 = sum1.add(popcount(query, code(codes, row1 + i)));
            sum2 = sum2.add(popcount(query, code(codes, row2 + i)));
            sum3 = sum3.add(popcount(query, code(codes, row3 + i)));
          }
          int score0 = (int) sum0.reduceLanes(VectorOperators.ADD);
          int score1 = (int) sum1.reduceLanes(VectorOperators.ADD);
          int score2 = (int) sum2.reduceLanes(VectorOperators.ADD);
          int score3 = (int) sum3.reduceLanes(VectorOperators.ADD);
          for (; i < q.length; i++) {
            int query = q[i];
            score0 += Integer.bitCount((query ^ codes[row0 + i]) & 255);
            score1 += Integer.bitCount((query ^ codes[row1 + i]) & 255);
            score2 += Integer.bitCount((query ^ codes[row2 + i]) & 255);
            score3 += Integer.bitCount((query ^ codes[row3 + i]) & 255);
          }
          out[r] = score0;
          out[r + 1] = score1;
          out[r + 2] = score2;
          out[r + 3] = score3;
        }
        for (; r < rows; r++) out[r] = hamming(q, codes, offset + r * q.length);
        return;
      }
      super.hamming(q, codes, offset, rows, out);
    }

    @Override
    int hamming(byte[] q, MemorySegment codes, long offset) {
      if (codes.isNative() == false) return tail(q, codes, offset, 0);
      var sum = LongVector.zero(LONGS);
      int i = 0;
      for (; i <= q.length - BYTES.length(); i += BYTES.length()) {
        var code = LongVector.fromMemorySegment(LONGS, codes, offset + i, ByteOrder.nativeOrder());
        sum = sum.add(popcount(q, i, code));
      }
      return (int) sum.reduceLanes(VectorOperators.ADD) + tail(q, codes, offset, i);
    }

    /** Hamming distance of bytes {@code [from, q.length)}: 8-byte words, then single bytes. */
    private static int wordTail(byte[] q, MemorySegment codes, long offset, int from) {
      MemorySegment query = MemorySegment.ofArray(q);
      int sum = 0, i = from;
      for (; i + Long.BYTES <= q.length; i += Long.BYTES) {
        long word = query.get(ValueLayout.JAVA_LONG_UNALIGNED, i);
        sum += Long.bitCount(word ^ codes.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + i));
      }
      return sum + tail(q, codes, offset, i);
    }

    @Override
    int hamming(byte[] q, byte[] codes, int offset) {
      var sum = LongVector.zero(LONGS);
      int i = 0;
      for (; i <= q.length - BYTES.length(); i += BYTES.length()) {
        var code = ByteVector.fromArray(BYTES, codes, offset + i).reinterpretAsLongs();
        sum = sum.add(popcount(q, i, code));
      }
      int total = (int) sum.reduceLanes(VectorOperators.ADD);
      for (; i < q.length; i++) total += Integer.bitCount((q[i] ^ codes[offset + i]) & 255);
      return total;
    }

    private static LongVector popcount(byte[] q, int i, LongVector code) {
      return popcount(query(q, i), code);
    }

    private static LongVector query(byte[] q, int offset) {
      return ByteVector.fromArray(BYTES, q, offset).reinterpretAsLongs();
    }

    private static LongVector code(byte[] codes, int offset) {
      return ByteVector.fromArray(BYTES, codes, offset).reinterpretAsLongs();
    }

    private static LongVector code(MemorySegment codes, long offset) {
      return LongVector.fromMemorySegment(LONGS, codes, offset, ByteOrder.nativeOrder());
    }

    private static LongVector popcount(LongVector query, LongVector code) {
      return query.lanewise(VectorOperators.XOR, code).lanewise(VectorOperators.BIT_COUNT);
    }

    @Override
    int dotProduct(byte[] query, byte[] records, int offset) {
      var sum = IntVector.zero(DOT_INTS);
      int i = 0, end = DOT_BYTES.loopBound(query.length);
      for (; i < end; i += DOT_BYTES.length()) {
        var q = ByteVector.fromArray(DOT_BYTES, query, i);
        var d = ByteVector.fromArray(DOT_BYTES, records, offset + i);
        var qi = q.convertShape(VectorOperators.B2I, DOT_INTS, 0);
        var di = d.convertShape(VectorOperators.B2I, DOT_INTS, 0);
        sum = sum.add(qi.mul(di));
      }
      int total = sum.reduceLanes(VectorOperators.ADD);
      for (; i < query.length; i++) total += query[i] * records[offset + i];
      return total;
    }

    @Override
    void dotProducts(byte[] query, byte[] records, int stride, int count, int[] out) {
      if (DOT_IS_256 == false) {
        byte[] scratch = new byte[query.length];
        for (int r = 0; r < count; r++) {
          System.arraycopy(records, r * stride, scratch, 0, query.length);
          out[r] = VectorUtil.dotProduct(query, scratch);
        }
        return;
      }
      int r = 0;
      for (; r + 4 <= count; r += 4) {
        int o0 = r * stride, o1 = o0 + stride, o2 = o1 + stride, o3 = o2 + stride;
        var a0 = IntVector.zero(DOT_INTS);
        var a1 = IntVector.zero(DOT_INTS);
        var a2 = IntVector.zero(DOT_INTS);
        var a3 = IntVector.zero(DOT_INTS);
        int i = 0, end = DOT_BYTES.loopBound(query.length);
        for (; i < end; i += DOT_BYTES.length()) {
          var q =
              (IntVector)
                  ByteVector.fromArray(DOT_BYTES, query, i)
                      .convertShape(VectorOperators.B2I, DOT_INTS, 0);
          a0 = signedFma(q, records, o0 + i, a0);
          a1 = signedFma(q, records, o1 + i, a1);
          a2 = signedFma(q, records, o2 + i, a2);
          a3 = signedFma(q, records, o3 + i, a3);
        }
        int s0 = a0.reduceLanes(VectorOperators.ADD);
        int s1 = a1.reduceLanes(VectorOperators.ADD);
        int s2 = a2.reduceLanes(VectorOperators.ADD);
        int s3 = a3.reduceLanes(VectorOperators.ADD);
        for (; i < query.length; i++) {
          int q = query[i];
          s0 += q * records[o0 + i];
          s1 += q * records[o1 + i];
          s2 += q * records[o2 + i];
          s3 += q * records[o3 + i];
        }
        out[r] = s0;
        out[r + 1] = s1;
        out[r + 2] = s2;
        out[r + 3] = s3;
      }
      for (; r < count; r++) out[r] = dotProduct(query, records, r * stride);
    }

    @Override
    void dotProducts(byte[] query, MemorySegment records, long[] offsets, int count, int[] out) {
      if (DOT_IS_256 == false) {
        byte[] scratch = new byte[query.length];
        for (int r = 0; r < count; r++) {
          MemorySegment.copy(records, ValueLayout.JAVA_BYTE, offsets[r], scratch, 0, query.length);
          out[r] = VectorUtil.dotProduct(query, scratch);
        }
        return;
      }
      int r = 0, end = DOT_BYTES.loopBound(query.length);
      for (; r + 4 <= count; r += 4) {
        long o0 = offsets[r], o1 = offsets[r + 1], o2 = offsets[r + 2], o3 = offsets[r + 3];
        var a0 = IntVector.zero(DOT_INTS);
        var a1 = IntVector.zero(DOT_INTS);
        var a2 = IntVector.zero(DOT_INTS);
        var a3 = IntVector.zero(DOT_INTS);
        for (int i = 0; i < end; i += DOT_BYTES.length()) {
          var q =
              (IntVector)
                  ByteVector.fromArray(DOT_BYTES, query, i)
                      .convertShape(VectorOperators.B2I, DOT_INTS, 0);
          a0 = signedFma(q, records, o0 + i, a0);
          a1 = signedFma(q, records, o1 + i, a1);
          a2 = signedFma(q, records, o2 + i, a2);
          a3 = signedFma(q, records, o3 + i, a3);
        }
        out[r] = a0.reduceLanes(VectorOperators.ADD) + tailDot(query, records, o0, end);
        out[r + 1] = a1.reduceLanes(VectorOperators.ADD) + tailDot(query, records, o1, end);
        out[r + 2] = a2.reduceLanes(VectorOperators.ADD) + tailDot(query, records, o2, end);
        out[r + 3] = a3.reduceLanes(VectorOperators.ADD) + tailDot(query, records, o3, end);
      }
      for (; r < count; r++) {
        var a = IntVector.zero(DOT_INTS);
        for (int i = 0; i < end; i += DOT_BYTES.length()) {
          var q =
              (IntVector)
                  ByteVector.fromArray(DOT_BYTES, query, i)
                      .convertShape(VectorOperators.B2I, DOT_INTS, 0);
          a = signedFma(q, records, offsets[r] + i, a);
        }
        out[r] = a.reduceLanes(VectorOperators.ADD) + tailDot(query, records, offsets[r], end);
      }
    }

    private static int tailDot(byte[] query, MemorySegment records, long offset, int from) {
      int sum = 0;
      for (int i = from; i < query.length; i++) {
        sum += query[i] * records.get(ValueLayout.JAVA_BYTE, offset + i);
      }
      return sum;
    }

    private static IntVector signedFma(
        IntVector query, MemorySegment records, long offset, IntVector sum) {
      var doc =
          (IntVector)
              ByteVector.fromMemorySegment(DOT_BYTES, records, offset, ByteOrder.LITTLE_ENDIAN)
                  .convertShape(VectorOperators.B2I, DOT_INTS, 0);
      return sum.add(query.mul(doc));
    }

    private static IntVector signedFma(IntVector query, byte[] records, int offset, IntVector sum) {
      var doc =
          (IntVector)
              ByteVector.fromArray(DOT_BYTES, records, offset)
                  .convertShape(VectorOperators.B2I, DOT_INTS, 0);
      return sum.add(query.mul(doc));
    }

    @Override
    void pack2(float[] vector, int dim, byte[] dest, int offset, float low, float high) {
      int planeBytes = (dim + 7) >>> 3;
      int vectorized = dim & -64;
      for (int base = 0; base < vectorized; base += 64) {
        long lowBits = 0, highBits = 0;
        for (int i = 0; i < 64; i += FLOATS.length()) {
          var values = FloatVector.fromArray(FLOATS, vector, base + i);
          lowBits |= values.compare(VectorOperators.GE, low).toLong() << i;
          highBits |= values.compare(VectorOperators.GE, high).toLong() << i;
        }
        int at = offset + (base >>> 3);
        BitUtil.VH_LE_LONG.set(dest, at, lowBits);
        BitUtil.VH_LE_LONG.set(dest, at + planeBytes, highBits);
      }
      if (vectorized < dim) {
        int tail = vectorized >>> 3;
        java.util.Arrays.fill(dest, offset + tail, offset + planeBytes, (byte) 0);
        java.util.Arrays.fill(dest, offset + planeBytes + tail, offset + 2 * planeBytes, (byte) 0);
        for (int d = vectorized; d < dim; d++) {
          int mask = 1 << (d & 7), at = offset + (d >>> 3);
          if (vector[d] >= low) dest[at] |= (byte) mask;
          if (vector[d] >= high) dest[at + planeBytes] |= (byte) mask;
        }
      }
    }
  }
}
