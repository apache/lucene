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

import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_B2L;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_I2L;
import static jdk.incubator.vector.VectorOperators.ZERO_EXTEND_S2L;

import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Panama Vector API implementation of {@link DocValuesBulkDecodeSupport}.
 *
 * <p>For 8/16/32 bits per value, each iteration loads one vector of narrow little-endian lanes and
 * zero-extends it into {@code PARTS_*} long vectors. The inner loop over the parts is deliberately
 * not unrolled by hand: all species and derived lane counts below are {@code static final}, so its
 * trip count is a compile-time constant and the JIT unrolls it.
 */
final class PanamaDocValuesBulkDecodeSupport implements DocValuesBulkDecodeSupport {

  static final PanamaDocValuesBulkDecodeSupport INSTANCE = new PanamaDocValuesBulkDecodeSupport();

  private static final VectorSpecies<Byte> BYTE_PREFERRED = ByteVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Long> LONG_PREFERRED = LongVector.SPECIES_PREFERRED;

  // Fixed 256-bit output shape for the widening paths (see USE_VECTORS below).
  private static final VectorSpecies<Long> L256 = LongVector.SPECIES_256;
  private static final VectorSpecies<Byte> B128 = ByteVector.SPECIES_128;
  private static final VectorSpecies<Byte> B256 = ByteVector.SPECIES_256;
  private static final VectorSpecies<Short> S256 = ShortVector.SPECIES_256;
  private static final VectorSpecies<Integer> I256 = IntVector.SPECIES_256;

  private static final int L_LANES = L256.length(); // 4
  private static final int B_LANES = B128.length(); // 16
  private static final int S_LANES = S256.length(); // 16
  private static final int I_LANES = I256.length(); // 8
  private static final int LONG_LANES = LONG_PREFERRED.length();

  /** Number of long vectors produced from one input vector. */
  private static final int PARTS_8 = B_LANES / L_LANES; // 4

  private static final int PARTS_16 = S_LANES / L_LANES; // 4
  private static final int PARTS_32 = I_LANES / L_LANES; // 2

  private static final boolean USE_VECTORS =
      ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN && BYTE_PREFERRED.vectorByteSize() >= 32;

  private PanamaDocValuesBulkDecodeSupport() {}

  @Override
  public void decodeByteAligned(
      byte[] bytes, int bytesOffset, int bitsPerValue, long[] values, int valuesOffset, int count) {

    if (USE_VECTORS == false) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, values, valuesOffset, count);
      return;
    }

    switch (bitsPerValue) {
      case Byte.SIZE -> decode8(bytes, bytesOffset, values, valuesOffset, count);
      case Short.SIZE -> decode16(bytes, bytesOffset, values, valuesOffset, count);
      case Integer.SIZE -> decode32(bytes, bytesOffset, values, valuesOffset, count);
      case Long.SIZE -> decode64(bytes, bytesOffset, values, valuesOffset, count);
      default ->
          DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
              bytes, bytesOffset, bitsPerValue, values, valuesOffset, count);
    }
  }

  private static void decode8(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final int bound = B128.loopBound(count);
    int i = 0;
    for (; i < bound; i += B_LANES) {
      ByteVector bv = ByteVector.fromArray(B128, bytes, bytesOffset + i);
      for (int part = 0; part < PARTS_8; part++) {
        ((LongVector) bv.convertShape(ZERO_EXTEND_B2L, L256, part))
            .intoArray(values, valuesOffset + i + part * L_LANES);
      }
    }
    decodeTail(bytes, bytesOffset, Byte.SIZE, values, valuesOffset, i, count);
  }

  private static void decode16(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final int bound = S256.loopBound(count);
    int i = 0;
    for (; i < bound; i += S_LANES) {
      ShortVector sv =
          ByteVector.fromArray(B256, bytes, bytesOffset + i * Short.BYTES).reinterpretAsShorts();
      for (int part = 0; part < PARTS_16; part++) {
        ((LongVector) sv.convertShape(ZERO_EXTEND_S2L, L256, part))
            .intoArray(values, valuesOffset + i + part * L_LANES);
      }
    }
    decodeTail(bytes, bytesOffset, Short.SIZE, values, valuesOffset, i, count);
  }

  private static void decode32(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final int bound = I256.loopBound(count);
    int i = 0;
    for (; i < bound; i += I_LANES) {
      IntVector iv =
          ByteVector.fromArray(B256, bytes, bytesOffset + i * Integer.BYTES).reinterpretAsInts();
      for (int part = 0; part < PARTS_32; part++) {
        ((LongVector) iv.convertShape(ZERO_EXTEND_I2L, L256, part))
            .intoArray(values, valuesOffset + i + part * L_LANES);
      }
    }
    decodeTail(bytes, bytesOffset, Integer.SIZE, values, valuesOffset, i, count);
  }

  private static void decode64(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final int bound = LONG_PREFERRED.loopBound(count);
    int i = 0;
    for (; i < bound; i += LONG_LANES) {
      ByteVector.fromArray(BYTE_PREFERRED, bytes, bytesOffset + i * Long.BYTES)
          .reinterpretAsLongs()
          .intoArray(values, valuesOffset + i);
    }
    decodeTail(bytes, bytesOffset, Long.SIZE, values, valuesOffset, i, count);
  }

  /** Scalar decode of the {@code count - done} values left over after the vectorized loop. */
  private static void decodeTail(
      byte[] bytes,
      int bytesOffset,
      int bitsPerValue,
      long[] values,
      int valuesOffset,
      int done,
      int count) {
    if (done < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes,
          bytesOffset + done * (bitsPerValue / Byte.SIZE),
          bitsPerValue,
          values,
          valuesOffset + done,
          count - done);
    }
  }
}
