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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Brute-force recall test that bypasses HNSW to isolate quantization quality from graph traversal.
 */
public class TestTurboQuantBruteForceRecall extends LuceneTestCase {

  public void testBruteForceRecallD768B4() {
    assertBruteForceRecall(768, 1000, 4, 0.85f);
  }

  public void testBruteForceRecallD768B8() {
    assertBruteForceRecall(768, 1000, 8, 0.95f);
  }

  public void testBruteForceRecallD128B4() {
    assertBruteForceRecall(128, 1000, 4, 0.85f);
  }

  private void assertBruteForceRecall(int d, int n, int b, float minRecall) {
    Random rng = new Random(42);
    TurboQuantEncoding enc =
        TurboQuantEncoding.fromWireNumber(
                switch (b) {
                  case 2 -> 0;
                  case 3 -> 1;
                  case 4 -> 2;
                  case 8 -> 3;
                  default -> throw new IllegalArgumentException();
                })
            .orElseThrow();

    float[] centroids = BetaCodebook.centroids(d, b);
    float[] boundaries = BetaCodebook.boundaries(d, b);
    HadamardRotation rot = HadamardRotation.create(d, 12345L);

    float[][] vecs = new float[n][];
    byte[][] packed = new byte[n][];
    for (int i = 0; i < n; i++) {
      vecs[i] = randomUnit(d, rng);
      float[] rv = new float[d];
      rot.rotate(vecs[i], rv);
      byte[] idx = new byte[d];
      for (int j = 0; j < d; j++) idx[j] = (byte) BetaCodebook.quantize(rv[j], boundaries);
      packed[i] = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(idx, d, b, packed[i]);
    }

    int k = 10;
    int nq = 50;
    float totalRecall = 0;
    for (int q = 0; q < nq; q++) {
      float[] query = randomUnit(d, rng);
      float[] rq = new float[d];
      rot.rotate(query, rq);

      // Exact top-k
      float[] exactScores = new float[n];
      for (int i = 0; i < n; i++) {
        float dot = 0;
        for (int j = 0; j < d; j++) dot += query[j] * vecs[i][j];
        exactScores[i] = dot;
      }
      Set<Integer> exactTopK = topK(exactScores, k);

      // Quantized top-k (brute force, no HNSW)
      float[] quantScores = new float[n];
      for (int i = 0; i < n; i++) {
        quantScores[i] = TurboQuantScoringUtil.dotProduct(rq, packed[i], centroids, b, d);
      }
      Set<Integer> quantTopK = topK(quantScores, k);

      int hits = 0;
      for (int idx : quantTopK) {
        if (exactTopK.contains(idx)) hits++;
      }
      totalRecall += (float) hits / k;
    }
    float avgRecall = totalRecall / nq;
    System.out.println("BruteForce d=" + d + " b=" + b + " n=" + n + " recall@" + k + " = " + avgRecall);
    assertTrue(
        "BruteForce d=" + d + " b=" + b + " recall@" + k + "=" + avgRecall + " < " + minRecall,
        avgRecall >= minRecall);
  }

  private static Set<Integer> topK(float[] scores, int k) {
    Set<Integer> result = new HashSet<>();
    for (int j = 0; j < k; j++) {
      int best = -1;
      float bestS = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < scores.length; i++) {
        if (!result.contains(i) && scores[i] > bestS) {
          bestS = scores[i];
          best = i;
        }
      }
      if (best >= 0) result.add(best);
    }
    return result;
  }

  private static float[] randomUnit(int d, Random rng) {
    float[] v = new float[d];
    float norm = 0;
    for (int i = 0; i < d; i++) {
      v[i] = (float) rng.nextGaussian();
      norm += v[i] * v[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < d; i++) v[i] /= norm;
    return v;
  }
}
