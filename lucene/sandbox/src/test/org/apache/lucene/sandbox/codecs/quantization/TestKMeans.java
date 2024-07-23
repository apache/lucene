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

package org.apache.lucene.sandbox.codecs.quantization;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class TestKMeans extends LuceneTestCase {

  public void testKMeansAPI() throws IOException {
    int nVectors = random().nextInt(50, 1000);
    int nClusters = random().nextInt(1, nVectors / 20);
    int dims = random().nextInt(2, 20);
    int randIdx = random().nextInt(VectorSimilarityFunction.values().length);
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.values()[randIdx];
    float[][] vectors = generateData(nVectors, dims, nClusters);
    RandomAccessVectorValues.Floats vectorValues =
        RandomAccessVectorValues.fromFloats(Arrays.asList(vectors), dims);

    // default case
    {
      KMeans.Results results = KMeans.cluster(vectorValues, similarityFunction, nClusters);
      assertEquals(nClusters, results.centroids().length);
      assertEquals(nVectors, results.vectorCentroids().length);
    }
    // expert case
    {
      boolean assignCentroidsToVectors = random().nextBoolean();
      int randIdx2 = random().nextInt(KMeans.KmeansInitializationMethod.values().length);
      KMeans.KmeansInitializationMethod initializationMethod =
          KMeans.KmeansInitializationMethod.values()[randIdx2];
      int restarts = random().nextInt(1, 6);
      int iters = random().nextInt(1, 10);
      int sampleSize = random().nextInt(10, nVectors * 2);

      KMeans.Results results =
          KMeans.cluster(
              vectorValues,
              nClusters,
              assignCentroidsToVectors,
              random().nextLong(),
              initializationMethod,
              similarityFunction == VectorSimilarityFunction.COSINE,
              restarts,
              iters,
              sampleSize);
      assertEquals(nClusters, results.centroids().length);
      if (assignCentroidsToVectors) {
        assertEquals(nVectors, results.vectorCentroids().length);
      } else {
        assertNull(results.vectorCentroids());
      }
    }
  }

  private static float[][] generateData(int nSamples, int nDims, int nClusters) {
    float[][] data = new float[nSamples][nDims];
    float[][] centroids = new float[nClusters][nDims];
    // Generate random centroids
    for (int i = 0; i < nClusters; i++) {
      for (int j = 0; j < nDims; j++) {
        centroids[i][j] = random().nextFloat() * 100;
      }
    }
    // Generate data points around centroids
    for (int i = 0; i < nSamples; i++) {
      int cluster = random().nextInt(nClusters);
      for (int j = 0; j < nDims; j++) {
        data[i][j] = centroids[cluster][j] + random().nextFloat() * 10 - 5;
      }
    }
    return data;
  }
}
