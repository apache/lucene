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
package org.apache.lucene.sandbox.codecs.turboquant;

/**
 * Packs and unpacks b-bit quantization indices into byte arrays. Optimized paths for b=2 (4 per
 * byte), b=3 (8 indices per 3 bytes), b=4 (2 per byte / nibble), and b=8 (1 per byte / no-op).
 */
public final class TurboQuantBitPacker {

  private TurboQuantBitPacker() {}

  /** Packs b-bit indices into a byte array. */
  public static void pack(byte[] indices, int d, int b, byte[] out) {
    switch (b) {
      case 2 -> pack2(indices, d, out);
      case 3 -> pack3(indices, d, out);
      case 4 -> pack4(indices, d, out);
      case 8 -> System.arraycopy(indices, 0, out, 0, d);
      default -> throw new IllegalArgumentException("Unsupported bit-width: " + b);
    }
  }

  /** Unpacks b-bit indices from a byte array. */
  public static void unpack(byte[] packed, int b, int d, byte[] out) {
    switch (b) {
      case 2 -> unpack2(packed, d, out);
      case 3 -> unpack3(packed, d, out);
      case 4 -> unpack4(packed, d, out);
      case 8 -> System.arraycopy(packed, 0, out, 0, d);
      default -> throw new IllegalArgumentException("Unsupported bit-width: " + b);
    }
  }

  // b=2: 4 indices per byte, MSB first
  private static void pack2(byte[] indices, int d, byte[] out) {
    int outIdx = 0;
    int i = 0;
    for (; i + 3 < d; i += 4) {
      out[outIdx++] =
          (byte)
              (((indices[i] & 0x03) << 6)
                  | ((indices[i + 1] & 0x03) << 4)
                  | ((indices[i + 2] & 0x03) << 2)
                  | (indices[i + 3] & 0x03));
    }
    // Handle remainder
    if (i < d) {
      int val = 0;
      for (int shift = 6; i < d; i++, shift -= 2) {
        val |= (indices[i] & 0x03) << shift;
      }
      out[outIdx] = (byte) val;
    }
  }

  private static void unpack2(byte[] packed, int d, byte[] out) {
    int pIdx = 0;
    int i = 0;
    for (; i + 3 < d; i += 4) {
      int b = packed[pIdx++] & 0xFF;
      out[i] = (byte) ((b >> 6) & 0x03);
      out[i + 1] = (byte) ((b >> 4) & 0x03);
      out[i + 2] = (byte) ((b >> 2) & 0x03);
      out[i + 3] = (byte) (b & 0x03);
    }
    if (i < d) {
      int b = packed[pIdx] & 0xFF;
      for (int shift = 6; i < d; i++, shift -= 2) {
        out[i] = (byte) ((b >> shift) & 0x03);
      }
    }
  }

  // b=3: 8 indices per 3 bytes
  private static void pack3(byte[] indices, int d, byte[] out) {
    int outIdx = 0;
    int i = 0;
    for (; i + 7 < d; i += 8) {
      // Pack 8 3-bit values into 3 bytes (24 bits)
      int bits =
          ((indices[i] & 0x07) << 21)
              | ((indices[i + 1] & 0x07) << 18)
              | ((indices[i + 2] & 0x07) << 15)
              | ((indices[i + 3] & 0x07) << 12)
              | ((indices[i + 4] & 0x07) << 9)
              | ((indices[i + 5] & 0x07) << 6)
              | ((indices[i + 6] & 0x07) << 3)
              | (indices[i + 7] & 0x07);
      out[outIdx++] = (byte) (bits >> 16);
      out[outIdx++] = (byte) (bits >> 8);
      out[outIdx++] = (byte) bits;
    }
    // Handle remainder
    if (i < d) {
      int bits = 0;
      int shift = 21;
      for (int j = i; j < d; j++, shift -= 3) {
        bits |= (indices[j] & 0x07) << shift;
      }
      out[outIdx++] = (byte) (bits >> 16);
      if (outIdx < out.length) out[outIdx++] = (byte) (bits >> 8);
      if (outIdx < out.length) out[outIdx] = (byte) bits;
    }
  }

  private static void unpack3(byte[] packed, int d, byte[] out) {
    int pIdx = 0;
    int i = 0;
    for (; i + 7 < d; i += 8) {
      int bits =
          ((packed[pIdx] & 0xFF) << 16)
              | ((packed[pIdx + 1] & 0xFF) << 8)
              | (packed[pIdx + 2] & 0xFF);
      pIdx += 3;
      out[i] = (byte) ((bits >> 21) & 0x07);
      out[i + 1] = (byte) ((bits >> 18) & 0x07);
      out[i + 2] = (byte) ((bits >> 15) & 0x07);
      out[i + 3] = (byte) ((bits >> 12) & 0x07);
      out[i + 4] = (byte) ((bits >> 9) & 0x07);
      out[i + 5] = (byte) ((bits >> 6) & 0x07);
      out[i + 6] = (byte) ((bits >> 3) & 0x07);
      out[i + 7] = (byte) (bits & 0x07);
    }
    if (i < d) {
      int bits =
          ((pIdx < packed.length ? packed[pIdx] & 0xFF : 0) << 16)
              | ((pIdx + 1 < packed.length ? packed[pIdx + 1] & 0xFF : 0) << 8)
              | (pIdx + 2 < packed.length ? packed[pIdx + 2] & 0xFF : 0);
      for (int shift = 21; i < d; i++, shift -= 3) {
        out[i] = (byte) ((bits >> shift) & 0x07);
      }
    }
  }

  // b=4: 2 indices per byte (nibble packing)
  private static void pack4(byte[] indices, int d, byte[] out) {
    int outIdx = 0;
    int i = 0;
    for (; i + 1 < d; i += 2) {
      out[outIdx++] = (byte) (((indices[i] & 0x0F) << 4) | (indices[i + 1] & 0x0F));
    }
    if (i < d) {
      out[outIdx] = (byte) ((indices[i] & 0x0F) << 4);
    }
  }

  private static void unpack4(byte[] packed, int d, byte[] out) {
    int pIdx = 0;
    int i = 0;
    for (; i + 1 < d; i += 2) {
      int b = packed[pIdx++] & 0xFF;
      out[i] = (byte) ((b >> 4) & 0x0F);
      out[i + 1] = (byte) (b & 0x0F);
    }
    if (i < d) {
      out[i] = (byte) ((packed[pIdx] >> 4) & 0x0F);
    }
  }
}
