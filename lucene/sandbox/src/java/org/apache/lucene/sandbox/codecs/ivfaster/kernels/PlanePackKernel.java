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

import java.util.concurrent.atomic.LongAdder;

/**
 * Packs thermometer planes: the ENCODE side of the coarse tier.
 *
 * <p>Once the encoding is expressed as one comparison per plane per coordinate (see {@code
 * Nitrox2.thresholdFor}), it vectorizes almost verbatim: {@code VectorMask.toLong()} hands back the
 * packed bitmask, so the comparison IS the encoder and there is no bit-scatter loop. At 8 float
 * lanes per 256-bit vector that is 8 compares per 64-bit word in place of 64 scalar iterations,
 * each of which also did a byte read-modify-write.
 *
 * <p>Same reflective load, scalar fallback and engagement counter as every other kernel here. The
 * counter matters because a silent scalar fallback is indistinguishable from a SIMD run in a
 * latency measurement, and a parity test comparing the scalar path against itself proves nothing.
 *
 * @lucene.experimental
 */
interface PlanePackKernel {

  /**
   * Packs {@code vector[0..dim)} into {@code planes} thermometer planes, each {@code planeBytes}
   * long, starting at {@code destOff}. Plane {@code t} gets a set bit for coordinate {@code d} when
   * {@code vector[d] >= thresholds[t]}.
   */
  void pack(float[] vector, int dim, byte[] dest, int destOff, int planeBytes, float[] thresholds);

  /** Whether this implementation uses the Vector API. */
  boolean isVectorized();

  /**
   * Vectors packed through a SIMD path, so a test can assert the fast path actually ran.
   *
   * <p>A {@link LongAdder} rather than an {@code AtomicLong}, because this ticks once per vector
   * encoded on every indexing worker at once, where a single atomic would put every thread in a CAS
   * loop on one cache line for a number only a test reads. LongAdder keeps per-thread cells and
   * sums on demand. Read with {@code sum()}.
   *
   * <p>The Hamming and bulk-dot counters stay AtomicLong: they tick once per cell scan or per
   * batch, so there is nothing to contend over.
   */
  LongAdder simdEngaged = new LongAdder();

  static PlanePackKernel get() {
    return Holder.INSTANCE;
  }

  /**
   * Lazy holder, so the reflective probe runs once.
   *
   * <p>{@code jdk.incubator.vector} is a {@code requires static} dependency, so the module may be
   * absent at runtime, and reaching the adapter reflectively keeps this class loadable when it is.
   *
   * <p>Any failure, whether a missing module, an unsupported species, or a static initializer that
   * throws, falls back to the correct scalar path rather than failing the codec. That fallback is
   * silent, which is what the engagement counter is for.
   */
  final class Holder {
    static final PlanePackKernel INSTANCE = load();

    private Holder() {}

    private static PlanePackKernel load() {
      if (Boolean.getBoolean("ivfaster.noSimdPlanePack")) {
        return new Scalar();
      }
      try {
        final Module m = PlanePackKernel.class.getModule();
        final ModuleLayer layer = m.getLayer();
        if (layer != null) {
          layer.findModule("jdk.incubator.vector").ifPresent(m::addReads);
        }
        final Class<?> impl =
            Class.forName("org.apache.lucene.sandbox.codecs.ivfaster.PanamaPlanePackKernel");
        return (PlanePackKernel) impl.getDeclaredConstructor().newInstance();
      } catch (Throwable _) {
        return new Scalar();
      }
    }
  }

  /** The reference implementation, and the fallback. */
  final class Scalar implements PlanePackKernel {
    @Override
    public void pack(
        float[] vector, int dim, byte[] dest, int destOff, int planeBytes, float[] thresholds) {
      final int planes = thresholds.length;
      java.util.Arrays.fill(dest, destOff, destOff + planes * planeBytes, (byte) 0);
      for (int d = 0; d < dim; d++) {
        final float v = vector[d];
        final int idx = d >>> 3;
        final int mask = 1 << (d & 7);
        for (int t = 0; t < planes; t++) {
          if (v >= thresholds[t]) {
            dest[destOff + t * planeBytes + idx] |= (byte) mask;
          }
        }
      }
    }

    @Override
    public boolean isVectorized() {
      return false;
    }
  }
}
