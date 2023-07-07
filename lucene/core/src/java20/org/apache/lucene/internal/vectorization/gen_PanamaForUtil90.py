# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Code generation for PanamaForUtil90.java"""

OUTPUT_FILE = "PanamaForUtil90.java"

HEADER = """// This file has been automatically generated, DO NOT EDIT

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
      longs[i] |= decoded[i];
      longs[i] <<= 32;
      longs[i] |= decoded[i + 64];
    }
  }
"""

total_bits = 32


def generate_pack_code(bitPerValue, f):
    function_defination = """
  // SIMD_fastpackwithoutmask%d_32
  private static void fastPack%d(int[] input, int[] output) {
    int inOff = 0;
    int outOff = 0;
    IntVector outVec;
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, inOff);
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
""" % (bitPerValue, bitPerValue)
    f.write(function_defination)

    function_end = """
    outVec = inVec.lanewise(VectorOperators.LSHL, %d).or(outVec);
    outVec.intoArray(output, outOff);
  }
"""

    prefect_fit = """
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec;
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
"""

    compress_code = """
    outVec = inVec.lanewise(VectorOperators.LSHL, %d).or(outVec);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
"""

    unprefect_fit = """
    outVec = inVec.lanewise(VectorOperators.LSHL, %d).or(outVec);
    outVec.intoArray(output, outOff);
    outOff += 4;
    outVec = inVec.lanewise(VectorOperators.LSHR, %d);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
"""

    bitsRemaining = total_bits - bitPerValue
    for i in range(0, 124, 4):
        if i == 120:
            f.write(function_end % (total_bits - bitsRemaining))
            break
        if bitsRemaining == 0:
            f.write(prefect_fit)
            bitsRemaining = total_bits - bitPerValue
        elif bitsRemaining >= bitPerValue:
            f.write(compress_code % (total_bits - bitsRemaining))
            bitsRemaining -= bitPerValue
        else:
            f.write(unprefect_fit %( total_bits - bitsRemaining, bitsRemaining))
            bitsRemaining = total_bits + (bitsRemaining - bitPerValue)


def generate_unpack_code(bitPerValue, f):
    function_defination = """
  // __SIMD_fastunpack%d_32
  private static void fastUnpack%d(int[] input, int[] output) {
    IntVector inVec = IntVector.fromArray(SPECIES_128, input, 0);
    IntVector outVec;
    int inOff = 0;
    int outOff = 0;
    final int mask = (1 << %d) - 1;

    outVec = inVec.and(mask);
    outVec.intoArray(output, outOff);
""" % (bitPerValue, bitPerValue, bitPerValue)

    decompress = """
    outVec = inVec.lanewise(VectorOperators.LSHR, %d).and(mask);
    outVec.intoArray(output, outOff += 4);
"""

    unprefect_fit = """
    outVec = inVec.lanewise(VectorOperators.LSHR, %d);
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
    outVec = outVec.or(inVec.lanewise(VectorOperators.LSHL, %d).and(mask));
    outVec.intoArray(output, outOff += 4);
"""

    prefect_fit = """
    inVec = IntVector.fromArray(SPECIES_128, input, inOff += 4);
"""

    function_end = """
    outVec = inVec.lanewise(VectorOperators.LSHR, %d);
    outVec.intoArray(output, outOff += 4);
  }
"""
    f.write(function_defination)
    bitsRemaining = total_bits - bitPerValue
    for i in range(0, 124, 4):
        if i == 120:
            f.write(function_end % (total_bits - bitPerValue))
            break
        if bitsRemaining == 0:
            f.write(prefect_fit)
            bitsRemaining = total_bits

        if bitsRemaining >= bitPerValue:
            f.write(decompress % (total_bits - bitsRemaining))
            bitsRemaining -= bitPerValue
        else:
            f.write(unprefect_fit % (total_bits - bitsRemaining, bitsRemaining))
            bitsRemaining = total_bits + (bitsRemaining - bitPerValue)


if __name__ == '__main__':
    f = open(OUTPUT_FILE, 'w')
    f.write(HEADER)

    for bits in range(1, 32):
        generate_pack_code(bits, f)
        generate_unpack_code(bits, f)

    f.write("""}\n""")