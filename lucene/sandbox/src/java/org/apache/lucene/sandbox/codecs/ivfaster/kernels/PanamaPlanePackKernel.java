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
package org.apache.lucene.sandbox.codecs.ivfaster;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.BitUtil;

/**
 * Vectorized thermometer packing: the comparison IS the encoder.
 *
 * <p>{@code VectorMask.toLong()} returns the compare result as a packed bitmask, which is the
 * layout the plane wants, so there is no per-coordinate bit scatter, no read-modify-write, and no
 * zero-fill. Each mask is shifted into position in a 64-bit accumulator and the word is stored
 * once.
 *
 * <p>Bit order is load-bearing and matches the scalar path exactly: lane {@code i} of a vector
 * loaded at coordinate {@code d} becomes bit {@code (d + i) mod 64} of the word covering {@code d},
 * which is bit {@code (d+i) & 7} of byte {@code (d+i) >>> 3} in little-endian order. That is the
 * same bit the scalar encoder sets, and the on-disk layout is therefore identical rather than
 * merely equivalent.
 *
 * <p>This class must not be referenced from code that can load without {@code
 * jdk.incubator.vector}; it is reached only by {@code Class.forName} from {@link PlanePackKernel}.
 *
 * @lucene.experimental
 */
final class PanamaPlanePackKernel implements PlanePackKernel {

  private static final VectorSpecies<Float> F_SPECIES =
      VectorSpecies.of(float.class, VectorShape.preferredShape());

  /** Float lanes per vector: 8 at 256-bit, 16 at 512-bit. */
  private static final int LANES = F_SPECIES.length();

  PanamaPlanePackKernel() {}

  @Override
  public void pack(
      float[] vector, int dim, byte[] dest, int destOff, int planeBytes, float[] thresholds) {
    // LANES divides 64 for every species this runs on, so the inner loop never straddles a word.
    final int planes = thresholds.length;
    // Whole 64-coordinate words only; the tail is finished scalar-wise below.
    final int words = dim >>> 6;
    final int vectorized = words << 6;

    for (int w = 0; w < words; w++) {
      final int base = w << 6;
      long acc0 = 0;
      long acc1 = 0;
      long acc2 = 0;
      // Named locals rather than an array, which would spill and defeat register allocation.
      for (int i = 0; i < 64; i += LANES) {
        final FloatVector v = FloatVector.fromArray(F_SPECIES, vector, base + i);
        acc0 |= toBits(v, thresholds[0]) << i;
        if (planes > 1) {
          acc1 |= toBits(v, thresholds[1]) << i;
        }
        if (planes > 2) {
          acc2 |= toBits(v, thresholds[2]) << i;
        }
      }
      final int byteOff = destOff + (base >>> 3);
      BitUtil.VH_LE_LONG.set(dest, byteOff, acc0);
      if (planes > 1) {
        BitUtil.VH_LE_LONG.set(dest, byteOff + planeBytes, acc1);
      }
      if (planes > 2) {
        BitUtil.VH_LE_LONG.set(dest, byteOff + 2 * planeBytes, acc2);
      }
    }

    if (vectorized < dim) {
      // Tail, byte-granular so only bytes that exist are touched.
      final int tailOff = vectorized >>> 3;
      for (int t = 0; t < planes; t++) {
        java.util.Arrays.fill(
            dest, destOff + t * planeBytes + tailOff, destOff + (t + 1) * planeBytes, (byte) 0);
      }
      for (int d = vectorized; d < dim; d++) {
        final float x = vector[d];
        final int idx = d >>> 3;
        final int mask = 1 << (d & 7);
        for (int t = 0; t < planes; t++) {
          if (x >= thresholds[t]) {
            dest[destOff + t * planeBytes + idx] |= (byte) mask;
          }
        }
      }
    }
    simdEngaged.add(1);
  }

  /**
   * The compare, as a packed bitmask.
   *
   * <p>{@code GE} rather than {@code GT}: the threshold is inclusive, matching {@code v >= t} in
   * the scalar form. NaN compares false under either, which lands a NaN coordinate on level 0, the
   * same result the arithmetic form gives via {@code Math.round(NaN) == 0}.
   *
   * <p>THE WHOLE KERNEL RESTS ON {@code toLong()} BEING INTRINSIFIED, and that can degrade
   * silently: where the intrinsic does not apply, {@code toLong} falls back to a per-lane Java loop
   * that still returns the CORRECT bits, so the kernel is genuinely engaged and scalar on the
   * inside. That is the one failure mode {@link PlanePackKernel#simdEngaged} is blind to. Check it
   * by timing this pack against {@code PlanePackKernel.Scalar} on the target JDK; a ratio near 1.0
   * means confirming with {@code -XX:+PrintIntrinsics} (look for {@code VectorMask::toLong}) before
   * trusting a pack timing.
   */
  private static long toBits(FloatVector v, float threshold) {
    final VectorMask<Float> m = v.compare(VectorOperators.GE, threshold);
    return m.toLong();
  }

  @Override
  public boolean isVectorized() {
    return true;
  }
}
