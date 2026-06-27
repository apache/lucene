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

import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests that LUT-based scoring matches naive unpacking for all encodings. */
public class TestTurboQuantScoringUtil extends LuceneTestCase {

  public void testDotProductMatchesNaive() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      for (int d : new int[] {32, 128, 768, 4096}) {
        verifyDotProductMatch(d, b, enc);
      }
    }
  }

  public void testSquareDistanceMatchesNaive() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      for (int d : new int[] {32, 128, 768}) {
        verifySquareDistanceMatch(d, b, enc);
      }
    }
  }

  private void verifyDotProductMatch(int d, int b, TurboQuantEncoding enc) {
    java.util.Random rng = new java.util.Random(d * 31L + b);
    float[] centroids = BetaCodebook.centroids(d, b);
    int maxVal = (1 << b) - 1;

    for (int trial = 0; trial < 10; trial++) {
      // Random query
      float[] query = new float[d];
      for (int i = 0; i < d; i++) query[i] = (float) rng.nextGaussian() / (float) Math.sqrt(d);

      // Random indices
      byte[] indices = new byte[d];
      for (int i = 0; i < d; i++) indices[i] = (byte) rng.nextInt(maxVal + 1);

      // Pack
      byte[] packed = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(indices, d, b, packed);

      // Naive dot product
      float naiveDot = 0;
      for (int i = 0; i < d; i++) {
        naiveDot += query[i] * centroids[indices[i] & 0xFF];
      }

      // LUT dot product
      float lutDot = TurboQuantScoringUtil.dotProduct(query, packed, centroids, b, d);

      assertEquals(
          "b=" + b + " d=" + d + " trial=" + trial, naiveDot, lutDot, Math.abs(naiveDot) * 1e-5f);
    }
  }

  private void verifySquareDistanceMatch(int d, int b, TurboQuantEncoding enc) {
    java.util.Random rng = new java.util.Random(d * 37L + b);
    float[] centroids = BetaCodebook.centroids(d, b);
    int maxVal = (1 << b) - 1;
    float docNorm = 1.5f;

    for (int trial = 0; trial < 10; trial++) {
      float[] query = new float[d];
      for (int i = 0; i < d; i++) query[i] = (float) rng.nextGaussian() / (float) Math.sqrt(d);

      byte[] indices = new byte[d];
      for (int i = 0; i < d; i++) indices[i] = (byte) rng.nextInt(maxVal + 1);

      byte[] packed = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(indices, d, b, packed);

      // Naive
      float naiveDist = 0;
      for (int i = 0; i < d; i++) {
        float diff = query[i] - centroids[indices[i] & 0xFF] * docNorm;
        naiveDist += diff * diff;
      }

      // LUT
      float lutDist =
          TurboQuantScoringUtil.squareDistance(query, packed, centroids, b, d, docNorm);

      assertEquals(
          "b=" + b + " d=" + d + " trial=" + trial,
          naiveDist,
          lutDist,
          Math.abs(naiveDist) * 1e-5f);
    }
  }
}
