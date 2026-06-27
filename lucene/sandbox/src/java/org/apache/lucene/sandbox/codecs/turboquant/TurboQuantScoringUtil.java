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
 * Optimized scoring utilities for TurboQuant quantized vectors. Uses LUT-based approach where
 * centroid values are gathered via index lookup, enabling JVM auto-vectorization.
 */
public final class TurboQuantScoringUtil {

  private TurboQuantScoringUtil() {}

  /**
   * Computes dot product between a float query vector (already rotated) and a quantized document
   * vector stored as packed b-bit indices.
   *
   * @param query rotated query vector
   * @param packedIndices packed b-bit quantization indices
   * @param centroids centroid values (2^b entries, scaled by 1/√d)
   * @param b bits per coordinate (2, 3, 4, or 8)
   * @param d dimension
   * @return dot product in rotated space
   */
  public static float dotProduct(
      float[] query, byte[] packedIndices, float[] centroids, int b, int d) {
    return switch (b) {
      case 4 -> dotProduct4(query, packedIndices, centroids, d);
      case 8 -> dotProduct8(query, packedIndices, centroids, d);
      case 2 -> dotProduct2(query, packedIndices, centroids, d);
      case 3 -> dotProduct3(query, packedIndices, centroids, d);
      default -> throw new IllegalArgumentException("Unsupported bit-width: " + b);
    };
  }

  /**
   * Computes squared Euclidean distance between a float query vector and a quantized document
   * vector.
   */
  public static float squareDistance(
      float[] query, byte[] packedIndices, float[] centroids, int b, int d, float docNorm) {
    return switch (b) {
      case 4 -> squareDistance4(query, packedIndices, centroids, d, docNorm);
      case 8 -> squareDistance8(query, packedIndices, centroids, d, docNorm);
      case 2 -> squareDistance2(query, packedIndices, centroids, d, docNorm);
      case 3 -> squareDistance3(query, packedIndices, centroids, d, docNorm);
      default -> throw new IllegalArgumentException("Unsupported bit-width: " + b);
    };
  }

  // b=4: 2 indices per byte (nibble-packed), 16-entry LUT
  private static float dotProduct4(float[] query, byte[] packed, float[] centroids, int d) {
    float sum = 0;
    int qi = 0;
    for (int i = 0; i < packed.length && qi < d; i++) {
      int b = packed[i] & 0xFF;
      sum += query[qi] * centroids[(b >> 4) & 0x0F];
      qi++;
      if (qi < d) {
        sum += query[qi] * centroids[b & 0x0F];
        qi++;
      }
    }
    return sum;
  }

  private static float squareDistance4(
      float[] query, byte[] packed, float[] centroids, int d, float docNorm) {
    float sum = 0;
    int qi = 0;
    for (int i = 0; i < packed.length && qi < d; i++) {
      int b = packed[i] & 0xFF;
      float diff = query[qi] - centroids[(b >> 4) & 0x0F] * docNorm;
      sum += diff * diff;
      qi++;
      if (qi < d) {
        diff = query[qi] - centroids[b & 0x0F] * docNorm;
        sum += diff * diff;
        qi++;
      }
    }
    return sum;
  }

  // b=8: 1 index per byte, 256-entry LUT
  private static float dotProduct8(float[] query, byte[] packed, float[] centroids, int d) {
    float sum = 0;
    for (int i = 0; i < d; i++) {
      sum += query[i] * centroids[packed[i] & 0xFF];
    }
    return sum;
  }

  private static float squareDistance8(
      float[] query, byte[] packed, float[] centroids, int d, float docNorm) {
    float sum = 0;
    for (int i = 0; i < d; i++) {
      float diff = query[i] - centroids[packed[i] & 0xFF] * docNorm;
      sum += diff * diff;
    }
    return sum;
  }

  // b=2: 4 indices per byte
  private static float dotProduct2(float[] query, byte[] packed, float[] centroids, int d) {
    float sum = 0;
    int qi = 0;
    for (int i = 0; i < packed.length && qi < d; i++) {
      int b = packed[i] & 0xFF;
      sum += query[qi++] * centroids[(b >> 6) & 0x03];
      if (qi < d) sum += query[qi++] * centroids[(b >> 4) & 0x03];
      if (qi < d) sum += query[qi++] * centroids[(b >> 2) & 0x03];
      if (qi < d) sum += query[qi++] * centroids[b & 0x03];
    }
    return sum;
  }

  private static float squareDistance2(
      float[] query, byte[] packed, float[] centroids, int d, float docNorm) {
    float sum = 0;
    int qi = 0;
    for (int i = 0; i < packed.length && qi < d; i++) {
      int b = packed[i] & 0xFF;
      for (int shift = 6; shift >= 0 && qi < d; shift -= 2) {
        float diff = query[qi++] - centroids[(b >> shift) & 0x03] * docNorm;
        sum += diff * diff;
      }
    }
    return sum;
  }

  // b=3: 8 indices per 3 bytes
  private static float dotProduct3(float[] query, byte[] packed, float[] centroids, int d) {
    float sum = 0;
    int qi = 0;
    int pi = 0;
    while (qi + 7 < d && pi + 2 < packed.length) {
      int bits =
          ((packed[pi] & 0xFF) << 16) | ((packed[pi + 1] & 0xFF) << 8) | (packed[pi + 2] & 0xFF);
      pi += 3;
      sum += query[qi++] * centroids[(bits >> 21) & 0x07];
      sum += query[qi++] * centroids[(bits >> 18) & 0x07];
      sum += query[qi++] * centroids[(bits >> 15) & 0x07];
      sum += query[qi++] * centroids[(bits >> 12) & 0x07];
      sum += query[qi++] * centroids[(bits >> 9) & 0x07];
      sum += query[qi++] * centroids[(bits >> 6) & 0x07];
      sum += query[qi++] * centroids[(bits >> 3) & 0x07];
      sum += query[qi++] * centroids[bits & 0x07];
    }
    // Handle remainder
    if (qi < d && pi < packed.length) {
      int bits =
          ((pi < packed.length ? packed[pi] & 0xFF : 0) << 16)
              | ((pi + 1 < packed.length ? packed[pi + 1] & 0xFF : 0) << 8)
              | (pi + 2 < packed.length ? packed[pi + 2] & 0xFF : 0);
      for (int shift = 21; qi < d; shift -= 3) {
        sum += query[qi++] * centroids[(bits >> shift) & 0x07];
      }
    }
    return sum;
  }

  private static float squareDistance3(
      float[] query, byte[] packed, float[] centroids, int d, float docNorm) {
    // Unpack and compute — b=3 is less common, use generic path
    byte[] indices = new byte[d];
    TurboQuantBitPacker.unpack(packed, 3, d, indices);
    float sum = 0;
    for (int i = 0; i < d; i++) {
      float diff = query[i] - centroids[indices[i] & 0x07] * docNorm;
      sum += diff * diff;
    }
    return sum;
  }
}
