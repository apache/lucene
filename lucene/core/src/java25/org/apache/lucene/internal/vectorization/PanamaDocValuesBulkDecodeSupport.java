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

/** Panama Vector API implementation of {@link DocValuesBulkDecodeSupport}. */
final class PanamaDocValuesBulkDecodeSupport implements DocValuesBulkDecodeSupport {

  static final PanamaDocValuesBulkDecodeSupport INSTANCE = new PanamaDocValuesBulkDecodeSupport();

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

  private PanamaDocValuesBulkDecodeSupport() {}

  @Override
  public void decodeByteAligned(
      byte[] bytes, int bytesOffset, int bitsPerValue, long[] values, int valuesOffset, int count) {
    
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN || BYTE_SPECIES.vectorByteSize() < 32) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, values, valuesOffset, count);
      return;
    }

    switch (bitsPerValue) {
      case Byte.SIZE -> decode8(bytes, bytesOffset, values, valuesOffset, count);
      case Short.SIZE -> decode16(bytes, bytesOffset, values, valuesOffset, count);
      case Integer.SIZE -> decode32(bytes, bytesOffset, values, valuesOffset, count);
      case Long.SIZE -> decode64(bytes, bytesOffset, values, valuesOffset, count);
      default -> DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, values, valuesOffset, count);
    }
  }

  private static void decode8(byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_256;
    final VectorSpecies<Byte> B_SPECIES = ByteVector.SPECIES_128;
    final int L = L_SPECIES.length();
    final int byteLen = B_SPECIES.length();
    
    int i = 0;
    final int loopBound = count - (count % byteLen);
    
    if (loopBound > 0) {
      for (; i < loopBound; i += byteLen) {
        ByteVector bv = ByteVector.fromArray(B_SPECIES, bytes, bytesOffset + i);
        
        LongVector lv0 = (LongVector) bv.convertShape(ZERO_EXTEND_B2L, L_SPECIES, 0);
        LongVector lv1 = (LongVector) bv.convertShape(ZERO_EXTEND_B2L, L_SPECIES, 1);
        LongVector lv2 = (LongVector) bv.convertShape(ZERO_EXTEND_B2L, L_SPECIES, 2);
        LongVector lv3 = (LongVector) bv.convertShape(ZERO_EXTEND_B2L, L_SPECIES, 3);
        
        lv0.intoArray(values, valuesOffset + i);
        lv1.intoArray(values, valuesOffset + i + L);
        lv2.intoArray(values, valuesOffset + i + 2 * L);
        lv3.intoArray(values, valuesOffset + i + 3 * L);
      }
    }
    
    if (i < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset + i, Byte.SIZE, values, valuesOffset + i, count - i);
    }
  }

  private static void decode16(byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_256;
    final VectorSpecies<Byte> B_SPECIES = ByteVector.SPECIES_256;
    final int L = L_SPECIES.length();
    final int shortCount = B_SPECIES.length() / Short.BYTES; 
    
    int i = 0;
    final int loopBound = count - (count % shortCount);
    
    if (loopBound > 0) {
      for (; i < loopBound; i += shortCount) {
        ByteVector bv = ByteVector.fromArray(B_SPECIES, bytes, bytesOffset + i * Short.BYTES);
        ShortVector sv = bv.reinterpretAsShorts();
        
        LongVector lv0 = (LongVector) sv.convertShape(ZERO_EXTEND_S2L, L_SPECIES, 0);
        LongVector lv1 = (LongVector) sv.convertShape(ZERO_EXTEND_S2L, L_SPECIES, 1);
        LongVector lv2 = (LongVector) sv.convertShape(ZERO_EXTEND_S2L, L_SPECIES, 2);
        LongVector lv3 = (LongVector) sv.convertShape(ZERO_EXTEND_S2L, L_SPECIES, 3);
        
        lv0.intoArray(values, valuesOffset + i);
        lv1.intoArray(values, valuesOffset + i + L);
        lv2.intoArray(values, valuesOffset + i + 2 * L);
        lv3.intoArray(values, valuesOffset + i + 3 * L);
      }
    }
    
    if (i < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset + i * Short.BYTES, Short.SIZE, values, valuesOffset + i, count - i);
    }
  }

  private static void decode32(byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_256;
    final VectorSpecies<Byte> B_SPECIES = ByteVector.SPECIES_256;
    final int L = L_SPECIES.length();
    final int intCount = B_SPECIES.length() / Integer.BYTES; 
    
    int i = 0;
    final int loopBound = count - (count % intCount);
    
    if (loopBound > 0) {
      for (; i < loopBound; i += intCount) {
        ByteVector bv = ByteVector.fromArray(B_SPECIES, bytes, bytesOffset + i * Integer.BYTES);
        IntVector iv = bv.reinterpretAsInts();
        
        LongVector lv0 = (LongVector) iv.convertShape(ZERO_EXTEND_I2L, L_SPECIES, 0);
        LongVector lv1 = (LongVector) iv.convertShape(ZERO_EXTEND_I2L, L_SPECIES, 1);
        
        lv0.intoArray(values, valuesOffset + i);
        lv1.intoArray(values, valuesOffset + i + L);
      }
    }
    
    if (i < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset + i * Integer.BYTES, Integer.SIZE, values, valuesOffset + i, count - i);
    }
  }

  private static void decode64(byte[] bytes, int bytesOffset, long[] values, int valuesOffset, int count) {
    final int valuesPerVector = BYTE_SPECIES.vectorByteSize() / Long.BYTES;
    final int loopBound = count - (count % valuesPerVector);
    int i = 0;
    
    if (loopBound > 0) {
      for (; i < loopBound; i += valuesPerVector) {
        ByteVector.fromArray(BYTE_SPECIES, bytes, bytesOffset + i * Long.BYTES)
            .reinterpretAsLongs()
            .intoArray(values, valuesOffset + i);
      }
    }
    
    if (i < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset + i * Long.BYTES, Long.SIZE, values, valuesOffset + i, count - i);
    }
  }
}