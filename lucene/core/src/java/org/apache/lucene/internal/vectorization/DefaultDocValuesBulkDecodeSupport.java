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

import org.apache.lucene.util.BitUtil;

/** Scalar (non-SIMD) implementation of {@link DocValuesBulkDecodeSupport}. */
final class DefaultDocValuesBulkDecodeSupport implements DocValuesBulkDecodeSupport {

  static final DefaultDocValuesBulkDecodeSupport INSTANCE = new DefaultDocValuesBulkDecodeSupport();

  private static final int BYTES_PER_24_BIT_VALUE = 24 / Byte.SIZE;
  private static final int BYTES_PER_40_BIT_VALUE = 40 / Byte.SIZE;
  private static final int BYTES_PER_48_BIT_VALUE = 48 / Byte.SIZE;
  private static final int BYTES_PER_56_BIT_VALUE = 56 / Byte.SIZE;

  private DefaultDocValuesBulkDecodeSupport() {}

  @Override
  public void decodeByteAligned(
      byte[] bytes, int bytesOffset, int bitsPerValue, long[] values, int valuesOffset, int count) {
    switch (bitsPerValue) {
      case Byte.SIZE -> decode8(bytes, bytesOffset, values, valuesOffset, count);
      case Short.SIZE -> decode16(bytes, bytesOffset, values, valuesOffset, count);
      case 24 -> decode24(bytes, bytesOffset, values, valuesOffset, count);
      case Integer.SIZE -> decode32(bytes, bytesOffset, values, valuesOffset, count);
      case 40 -> decode40(bytes, bytesOffset, values, valuesOffset, count);
      case 48 -> decode48(bytes, bytesOffset, values, valuesOffset, count);
      case 56 -> decode56(bytes, bytesOffset, values, valuesOffset, count);
      case Long.SIZE -> decode64(bytes, bytesOffset, values, valuesOffset, count);
      default -> throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  private static void decode8(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi++) {
      values[vi] = Byte.toUnsignedLong(bytes[bi]);
    }
  }

  private static void decode16(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += Short.BYTES) {
      values[vi] = Short.toUnsignedLong((short) BitUtil.VH_LE_SHORT.get(bytes, bi));
    }
  }

  private static void decode24(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += BYTES_PER_24_BIT_VALUE) {
      values[vi] = ((int) BitUtil.VH_LE_INT.get(bytes, bi)) & 0xFFFFFFL;
    }
  }

  private static void decode32(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += Integer.BYTES) {
      values[vi] = Integer.toUnsignedLong((int) BitUtil.VH_LE_INT.get(bytes, bi));
    }
  }

  private static void decode40(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += BYTES_PER_40_BIT_VALUE) {
      values[vi] = ((long) BitUtil.VH_LE_LONG.get(bytes, bi)) & 0xFFFFFFFFFFL;
    }
  }

  private static void decode48(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += BYTES_PER_48_BIT_VALUE) {
      values[vi] = ((long) BitUtil.VH_LE_LONG.get(bytes, bi)) & 0xFFFFFFFFFFFFL;
    }
  }

  private static void decode56(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += BYTES_PER_56_BIT_VALUE) {
      values[vi] = ((long) BitUtil.VH_LE_LONG.get(bytes, bi)) & 0xFFFFFFFFFFFFFFL;
    }
  }

  private static void decode64(
      byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    for (int vi = valuesOffset, bi = bytesOffset, end = valuesOffset + count;
        vi < end;
        vi++, bi += Long.BYTES) {
      values[vi] = (long) BitUtil.VH_LE_LONG.get(bytes, bi);
    }
  }
}
