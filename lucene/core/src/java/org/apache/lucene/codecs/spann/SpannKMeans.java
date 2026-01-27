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
package org.apache.lucene.codecs.spann;

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * In-memory K-Means implementation for SPANN clustering.
 *
 * @lucene.internal
 */
public class SpannKMeans {

  /** Seed for deterministic clustering. */
  private static final long SEED = 42;

  /** Clusters the input vectors using Lloyd's algorithm. */
  public static float[][] cluster(
      float[][] vectors, int k, VectorSimilarityFunction similarityFunction, int maxIterations) {
    if (vectors.length < k) {
      return vectors.clone();
    }

    int dim = vectors[0].length;
    float[][] centroids = new float[k][dim];
    Random random = new Random(SEED);

    // If we have significantly more vectors than K (and sample limit), we should
    // sample.
    // Ideally this sampling happens before calling cluster(), but we can also
    // enforce it here
    // or provide a helper.
    // For now, we assume the caller passes the training set.

    // K-Means++ Initialization
    int firstIdx = random.nextInt(vectors.length);
    centroids[0] = vectors[firstIdx].clone();

    float[] minDistSquared = new float[vectors.length];
    Arrays.fill(minDistSquared, Float.MAX_VALUE);

    for (int c = 1; c < k; c++) {
      double totalDistSq = 0;
      for (int i = 0; i < vectors.length; i++) {
        float d = VectorUtil.squareDistance(vectors[i], centroids[c - 1]);
        if (d < minDistSquared[i]) {
          minDistSquared[i] = d;
        }
        totalDistSq += minDistSquared[i];
      }

      double r = random.nextDouble() * totalDistSq;
      double cumulative = 0;
      int selectedIdx = -1;
      for (int i = 0; i < vectors.length; i++) {
        cumulative += minDistSquared[i];
        if (cumulative >= r) {
          selectedIdx = i;
          break;
        }
      }

      if (selectedIdx == -1) {
        selectedIdx = random.nextInt(vectors.length);
      }
      centroids[c] = vectors[selectedIdx].clone();
    }

    int[] assignments = new int[vectors.length];
    Arrays.fill(assignments, -1);

    for (int iter = 0; iter < maxIterations; iter++) {
      boolean changed = false;

      for (int i = 0; i < vectors.length; i++) {
        int bestCentroid = 0;
        float bestScore = Float.NEGATIVE_INFINITY;

        for (int c = 0; c < k; c++) {
          float score = similarityFunction.compare(vectors[i], centroids[c]);
          if (score > bestScore) {
            bestScore = score;
            bestCentroid = c;
          }
        }

        if (assignments[i] != bestCentroid) {
          assignments[i] = bestCentroid;
          changed = true;
        }
      }

      if (!changed) {
        break;
      }

      for (float[] centroid : centroids) {
        Arrays.fill(centroid, 0f);
      }
      int[] counts = new int[k];

      for (int i = 0; i < vectors.length; i++) {
        int cluster = assignments[i];
        for (int d = 0; d < dim; d++) {
          centroids[cluster][d] += vectors[i][d];
        }
        counts[cluster]++;
      }

      for (int c = 0; c < k; c++) {
        if (counts[c] > 0) {
          float scale = 1.0f / counts[c];
          for (int d = 0; d < dim; d++) {
            centroids[c][d] *= scale;
          }
        } else {
          // Re-init empty cluster randomly
          centroids[c] = vectors[random.nextInt(vectors.length)].clone();
        }
      }
    }

    return centroids;
  }

  /**
   * Deterministically downsamples a list of vectors to a maximum target size. Useful for reducing
   * clustering overhead on large datasets.
   */
  public static float[][] downsample(float[][] vectors, int maxSampleSize) {
    if (vectors.length <= maxSampleSize) {
      return vectors;
    }

    int step = vectors.length / maxSampleSize;
    float[][] sampled = new float[maxSampleSize][];
    for (int i = 0; i < maxSampleSize; i++) {
      sampled[i] = vectors[i * step];
    }
    return sampled;
  }
}
