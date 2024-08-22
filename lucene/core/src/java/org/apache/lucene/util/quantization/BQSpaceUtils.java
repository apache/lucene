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
package org.apache.lucene.util.quantization;

import org.apache.lucene.util.BitUtil;

public class BQSpaceUtils {

  // FIXME: allow for a settable B_QUERY of 4 or 2
  public static final short B_QUERY = 4;

  //  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
  // private static final byte BYTE_MASK = (1 << B_QUERY) - 1;

  // FIXME: clean up this function and move to utils like "space utils"
  public static void transposeBin(byte[] q, int dimensions, byte[] quantQueryByte) {
    int byte_mask = 1;
    for (int i = 0; i < B_QUERY - 1; i++) {
      byte_mask = byte_mask << 1 | 0b00000001;
    }
    int qOffset = 0;
    final byte[] v1 = new byte[4];
    final byte[] v = new byte[32];
    for (int i = 0; i < dimensions; i += 32) {
      // for every four bytes we shift left (with remainder across those bytes)
      int shift = 8 - B_QUERY;
      for (int j = 0; j < v.length; j += 4) {
        v[j] = (byte) (q[qOffset + j] << shift | ((q[qOffset + j] >>> (8 - shift)) & byte_mask));
        v[j + 1] =
            (byte)
                (q[qOffset + j + 1] << shift | ((q[qOffset + j + 1] >>> (8 - shift)) & byte_mask));
        v[j + 2] =
            (byte)
                (q[qOffset + j + 2] << shift | ((q[qOffset + j + 2] >>> (8 - shift)) & byte_mask));
        v[j + 3] =
            (byte)
                (q[qOffset + j + 3] << shift | ((q[qOffset + j + 3] >>> (8 - shift)) & byte_mask));
      }
      for (int j = 0; j < B_QUERY; j++) {
        moveMaskEpi8Byte(v, v1);
        for (int k = 0; k < 4; k++) {
          quantQueryByte[(B_QUERY - j - 1) * (dimensions / 8) + i / 8 + k] = v1[k];
          v1[k] = 0;
        }
        for (int k = 0; k < v.length; k += 4) {
          v[k] = (byte) (v[k] + v[k]);
          v[k + 1] = (byte) (v[k + 1] + v[k + 1]);
          v[k + 2] = (byte) (v[k + 2] + v[k + 2]);
          v[k + 3] = (byte) (v[k + 3] + v[k + 3]);
        }
      }
      qOffset += 32;
    }
  }

  // FIXME: clean up this function and move to utils like "space utils" in java21 directory
  //  public static byte[] transposeBinPan(byte[] q, int D) {
  //    assert B_QUERY > 0;
  //    int B = (D + 63) / 64 * 64;
  //    byte[] quantQueryByte = new byte[B_QUERY * B / 8];
  //    int qOffset = 0;
  //
  //    final byte[] v = new byte[32];
  //    final byte[] v1b = new byte[4];
  //    for (int i = 0; i < B; i += 32) {
  //      ByteVector q0 = ByteVector.fromArray(SPECIES, q, qOffset);
  //      ByteVector q1 = ByteVector.fromArray(SPECIES, q, qOffset + 16);
  //
  //      ByteVector v0 = q0.lanewise(VectorOperators.LSHL, 8 - B_QUERY);
  //      ByteVector v1 = q1.lanewise(VectorOperators.LSHL, 8 - B_QUERY);
  //      v0 =
  //              v0.lanewise(
  //                      VectorOperators.OR, q0.lanewise(VectorOperators.LSHR,
  // B_QUERY).and(BYTE_MASK));
  //      v1 =
  //              v1.lanewise(
  //                      VectorOperators.OR, q1.lanewise(VectorOperators.LSHR,
  // B_QUERY).and(BYTE_MASK));
  //
  //      for (int j = 0; j < B_QUERY; j++) {
  //        v0.intoArray(v, 0);
  //        v1.intoArray(v, 16);
  //        moveMaskEpi8Byte(v, v1b);
  //        for (int k = 0; k < 4; k++) {
  //          quantQueryByte[(B_QUERY - j - 1) * (B / 8) + i / 8 + k] = v1b[k];
  //          v1b[k] = 0;
  //        }
  //
  //        v0 = v0.lanewise(VectorOperators.ADD, v0);
  //        v1 = v1.lanewise(VectorOperators.ADD, v1);
  //      }
  //      qOffset += 32;
  //    }
  //    return quantQueryByte;
  //  }

  // FIXME: clean up this function and move to utils like "space utils"
  private static void moveMaskEpi8Byte(byte[] v, byte[] v1b) {
    int m = 0;
    for (int k = 0; k < v.length; k++) {
      if ((v[k] & 0b10000000) == 0b10000000) {
        v1b[m] |= 0b00000001;
      }
      if (k % 8 == 7) {
        m++;
      } else {
        v1b[m] <<= 1;
      }
    }
  }
}
