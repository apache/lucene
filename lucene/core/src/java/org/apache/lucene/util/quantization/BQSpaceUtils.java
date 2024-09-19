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

/** Utility class for quantization calculations */
public class BQSpaceUtils {

  // FIXME: allow for a settable B_QUERY of 4 or 2
  public static final short B_QUERY = 4;

  /**
   * Transpose the query vector into a byte array allowing for efficient bitwise operations with the
   * index bit vectors. The idea here is to organize the query vector bits such that the first bit
   * of every dimension is in the first set dimensions bits, or (dimensions/8) bytes. The second,
   * third, and fourth bits are in the second, third, and fourth set of dimensions bits,
   * respectively. This allows for direct bitwise comparisons with the stored index vectors through
   * summing the bitwise results with the relative required bit shifts.
   *
   * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
   * @param dimensions the number of dimensions in the query vector
   * @param quantQueryByte the byte array to store the transposed query vector
   */
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
