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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestKMeans extends LuceneTestCase {

  public void testKMeansAPI() throws IOException {
    int nClusters = random().nextInt(1, 10);
    int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
    int dims = random().nextInt(2, 20);
    int randIdx = random().nextInt(VectorSimilarityFunction.values().length);
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.values()[randIdx];
    FloatVectorValues vectors = generateData(nVectors, dims, nClusters);

    // default case
    {
      KMeans.Results results = KMeans.cluster(vectors, similarityFunction, nClusters);
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
              vectors,
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

  public void testKMeansSpecialCases() throws IOException {
    {
      // nClusters > nVectors
      int nClusters = 20;
      int nVectors = 10;
      FloatVectorValues vectors = generateData(nVectors, 5, nClusters);
      KMeans.Results results =
          KMeans.cluster(vectors, VectorSimilarityFunction.EUCLIDEAN, nClusters);
      // assert that we get 1 centroid, as nClusters will be adjusted
      assertEquals(1, results.centroids().length);
      assertEquals(nVectors, results.vectorCentroids().length);
    }
    {
      // small sample size
      int sampleSize = 2;
      int nClusters = 2;
      int nVectors = 300;
      FloatVectorValues vectors = generateData(nVectors, 5, nClusters);
      KMeans.KmeansInitializationMethod initializationMethod =
          KMeans.KmeansInitializationMethod.PLUS_PLUS;
      KMeans.Results results =
          KMeans.cluster(
              vectors,
              nClusters,
              true,
              random().nextLong(),
              initializationMethod,
              false,
              1,
              2,
              sampleSize);
      assertEquals(nClusters, results.centroids().length);
      assertEquals(nVectors, results.vectorCentroids().length);
    }
    {
      // test unassigned centroids
      int nClusters = 4;
      int nVectors = 400;
      FloatVectorValues vectors = generateData(nVectors, 5, nClusters);
      KMeans.Results results =
          KMeans.cluster(vectors, VectorSimilarityFunction.EUCLIDEAN, nClusters);
      float[][] centroids = results.centroids();
      List<Integer> unassignedIdxs = List.of(0, 3);
      KMeans.assignCentroids(vectors, centroids, unassignedIdxs);
      assertEquals(nClusters, centroids.length);
    }
  }

  private static FloatVectorValues generateData(int nSamples, int nDims, int nClusters) {
    List<float[]> vectors = new ArrayList<>(nSamples);
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
      float[] vector = new float[nDims];
      for (int j = 0; j < nDims; j++) {
        vector[j] = centroids[cluster][j] + random().nextFloat() * 10 - 5;
      }
      vectors.add(vector);
    }
    return FloatVectorValues.fromFloats(vectors, nDims);
  }
}
