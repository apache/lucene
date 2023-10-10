// This file has been automatically generated, DO NOT EDIT

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

import java.io.IOException;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

final class PanamaForUtil90 implements ForUtil90 {
  private final int[] tmp = new int[BLOCK_SIZE];

  private final int[] decoded = new int[BLOCK_SIZE];

  private static final int totalBits = 32;
  private static final VectorSpecies<Integer> SPECIES_128 = IntVector.SPECIES_128;

  /** Encode 128 integers from {@code input} into {@code out}. */
  @Override
  public void encode(int[] input, int bitsPerValue, DataOutput out) throws IOException {
    switch (bitsPerValue) {
      case 1:
        fastPack1(input, tmp);
        break;
      case 2:
        fastPack2(input, tmp);
        break;
      case 3:
        fastPack3(input, tmp);
        break;
      case 4:
        fastPack4(input, tmp);
        break;
      case 5:
        fastPack5(input, tmp);
        break;
      case 6:
        fastPack6(input, tmp);
        break;
      case 7:
        fastPack7(input, tmp);
        break;
      case 8:
        fastPack8(input, tmp);
        break;
      case 9:
        fastPack9(input, tmp);
        break;
      case 10:
        fastPack10(input, tmp);
        break;
      case 11:
        fastPack11(input, tmp);
        break;
      case 12:
        fastPack12(input, tmp);
        break;
      case 13:
        fastPack13(input, tmp);
        break;
      case 14:
        fastPack14(input, tmp);
        break;
      case 15:
        fastPack15(input, tmp);
        break;
      case 16:
        fastPack16(input, tmp);
        break;
      case 17:
        fastPack17(input, tmp);
        break;
      case 18:
        fastPack18(input, tmp);
        break;
      case 19:
        fastPack19(input, tmp);
        break;
      case 20:
        fastPack20(input, tmp);
        break;
      case 21:
        fastPack21(input, tmp);
        break;
      case 22:
        fastPack22(input, tmp);
        break;
      case 23:
        fastPack23(input, tmp);
        break;
      case 24:
        fastPack24(input, tmp);
        break;
      case 25:
        fastPack25(input, tmp);
        break;
      case 26:
        fastPack26(input, tmp);
        break;
      case 27:
        fastPack27(input, tmp);
        break;
      case 28:
        fastPack28(input, tmp);
        break;
      case 29:
        fastPack29(input, tmp);
        break;
      case 30:
        fastPack30(input, tmp);
        break;
      case 31:
        fastPack31(input, tmp);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    for (int i = 0; i < 4 * bitsPerValue; ++i) {
      out.writeInt(tmp[i]);
    }
  }

  /** Decode 128 integers into {@code output}. */
  @Override
  public void decode(int bitsPerValue, DataInput in, int[] output) throws IOException {
    in.readInts(tmp, 0, 4 * bitsPerValue);
    switch (bitsPerValue) {
      case 1:
        fastUnpack1(tmp, output);
        break;
      case 2:
        fastUnpack2(tmp, output);
        break;
      case 3:
        fastUnpack3(tmp, output);
        break;
      case 4:
        fastUnpack4(tmp, output);
        break;
      case 5:
        fastUnpack5(tmp, output);
        break;
      case 6:
        fastUnpack6(tmp, output);
        break;
      case 7:
        fastUnpack7(tmp, output);
        break;
      case 8:
        fastUnpack8(tmp, output);
        break;
      case 9:
        fastUnpack9(tmp, output);
        break;
      case 10:
        fastUnpack10(tmp, output);
        break;
      case 11:
        fastUnpack11(tmp, output);
        break;
      case 12:
        fastUnpack12(tmp, output);
        break;
      case 13:
        fastUnpack13(tmp, output);
        break;
      case 14:
        fastUnpack14(tmp, output);
        break;
      case 15:
        fastUnpack15(tmp, output);
        break;
      case 16:
        fastUnpack16(tmp, output);
        break;
      case 17:
        fastUnpack17(tmp, output);
        break;
      case 18:
        fastUnpack18(tmp, output);
        break;
      case 19:
        fastUnpack19(tmp, output);
        break;
      case 20:
        fastUnpack20(tmp, output);
        break;
      case 21:
        fastUnpack21(tmp, output);
        break;
      case 22:
        fastUnpack22(tmp, output);
        break;
      case 23:
        fastUnpack23(tmp, output);
        break;
      case 24:
        fastUnpack24(tmp, output);
        break;
      case 25:
        fastUnpack25(tmp, output);
        break;
      case 26:
        fastUnpack26(tmp, output);
        break;
      case 27:
        fastUnpack27(tmp, output);
        break;
      case 28:
        fastUnpack28(tmp, output);
        break;
      case 29:
        fastUnpack29(tmp, output);
        break;
      case 30:
        fastUnpack30(tmp, output);
        break;
      case 31:
        fastUnpack31(tmp, output);
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Decodes 128 integers into 64 {@code longs} such that each long contains two values, each
   * represented with 32 bits. Values [0..63] are encoded in the high-order bits of {@code longs}
   * [0..63], and values [64..127] are encoded in the low-order bits of {@code longs} [0..63]. This
   * representation may allow subsequent operations to be performed on two values at a time.
   */
  @Override
  public void decodeTo32(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    decode(bitsPerValue, in, decoded);
    for (int i = 0; i < 64; ++i) {
      longs[i] = (long) decoded[i] << 32 | decoded[i + 64];
    }
  }

  // SIMD_fastpackwithoutmask1_32
  private static void fastPack1(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack1_32
  private static void fastUnpack1(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 1) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask2_32
  private static void fastPack2(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack2_32
  private static void fastUnpack2(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 2) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask3_32
  private static void fastPack3(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack3_32
  private static void fastUnpack3(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 3) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask4_32
  private static void fastPack4(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack4_32
  private static void fastUnpack4(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 4) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask5_32
  private static void fastPack5(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack5_32
  private static void fastUnpack5(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 5) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask6_32
  private static void fastPack6(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack6_32
  private static void fastUnpack6(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 6) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask7_32
  private static void fastPack7(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack7_32
  private static void fastUnpack7(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 7) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask8_32
  private static void fastPack8(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack8_32
  private static void fastUnpack8(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 8) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask9_32
  private static void fastPack9(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack9_32
  private static void fastUnpack9(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 9) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask10_32
  private static void fastPack10(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack10_32
  private static void fastUnpack10(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 10) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask11_32
  private static void fastPack11(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack11_32
  private static void fastUnpack11(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 11) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask12_32
  private static void fastPack12(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack12_32
  private static void fastUnpack12(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 12) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask13_32
  private static void fastPack13(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack13_32
  private static void fastUnpack13(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 13) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask14_32
  private static void fastPack14(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack14_32
  private static void fastUnpack14(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 14) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask15_32
  private static void fastPack15(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack15_32
  private static void fastUnpack15(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 15) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask16_32
  private static void fastPack16(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack16_32
  private static void fastUnpack16(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 16) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask17_32
  private static void fastPack17(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack17_32
  private static void fastUnpack17(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 17) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask18_32
  private static void fastPack18(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack18_32
  private static void fastUnpack18(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 18) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask19_32
  private static void fastPack19(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack19_32
  private static void fastUnpack19(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 19) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask20_32
  private static void fastPack20(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack20_32
  private static void fastUnpack20(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 20) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask21_32
  private static void fastPack21(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack21_32
  private static void fastUnpack21(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 21) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask22_32
  private static void fastPack22(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack22_32
  private static void fastUnpack22(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 22) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask23_32
  private static void fastPack23(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack23_32
  private static void fastUnpack23(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 23) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 21).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask24_32
  private static void fastPack24(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack24_32
  private static void fastUnpack24(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 24) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask25_32
  private static void fastPack25(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack25_32
  private static void fastUnpack25(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 25) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 21).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 23).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask26_32
  private static void fastPack26(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack26_32
  private static void fastUnpack26(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 26) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask27_32
  private static void fastPack27(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack27_32
  private static void fastUnpack27(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 27) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 25).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 23).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 21).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 26).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask28_32
  private static void fastPack28(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack28_32
  private static void fastUnpack28(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 28) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask29_32
  private static void fastPack29(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack29_32
  private static void fastUnpack29(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 29) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 21).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 27).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 25).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 28).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 23).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 26).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask30_32
  private static void fastPack30(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack30_32
  private static void fastUnpack30(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 30) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 26).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 28).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2).and(mask);
    outVec.intoArray(output, outOff += 4);

    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 0).and(mask);
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 26).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 28).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    outVec.intoArray(output, outOff += 4);
  }

  // SIMD_fastpackwithoutmask31_32
  private static void fastPack31(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 31).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 30).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 29).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 28).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 27).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 26).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 25).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 24).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 23).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 22).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 21).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 20).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 19).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 18).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 17).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 16).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 15).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 14).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 13).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 12).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 11).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 10).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 9).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 8).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 7).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 6).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 5).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 4).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 3).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 2).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHL, 1).or(outVec);
    outVec.intoArray(output, outOff);
  }

  // __SIMD_fastunpack31_32
  private static void fastUnpack31(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << 31) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);

    outVec = inVec.lanewise(VectorOperators.LSHR, 31);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 1).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 30);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 2).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 29);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 3).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 28);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 4).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 27);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 5).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 26);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 6).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 25);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 7).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 24);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 8).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 23);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 9).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 22);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 10).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 21);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 11).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 20);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 12).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 19);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 13).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 18);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 14).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 17);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 15).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 16);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 16).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 15);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 17).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 14);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 18).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 13);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 19).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 12);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 20).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 11);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 21).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 10);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 22).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 9);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 23).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 8);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 24).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 7);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 25).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 6);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 26).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 5);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 27).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 4);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 28).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 3);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 29).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 2);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, 30).and(mask));
    outVec.intoArray(output, outOff += 4);

    outVec = inVec.lanewise(VectorOperators.LSHR, 1);
    outVec.intoArray(output, outOff += 4);
  }
}
