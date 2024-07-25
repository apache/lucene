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

import static org.apache.lucene.sandbox.codecs.quantization.SampleReader.createSampleReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/** KMeans clustering algorithm for vectors */
public class KMeans {
  public static final int MAX_NUM_CENTROIDS = Short.MAX_VALUE; // 32767
  public static final int DEFAULT_RESTARTS = 5;
  public static final int DEFAULT_ITRS = 10;
  public static final int DEFAULT_SAMPLE_SIZE = 100_000;

  private final RandomAccessVectorValues.Floats vectors;
  private final int numVectors;
  private final int numCentroids;
  private final Random random;
  private final KmeansInitializationMethod initializationMethod;
  private final int restarts;
  private final int iters;

  /**
   * Cluster vectors into a given number of clusters
   *
   * @param vectors float vectors
   * @param similarityFunction vector similarity function. For COSINE similarity, vectors must be
   *     normalized.
   * @param numClusters number of cluster to cluster vector into
   * @return results of clustering: produced centroids and for each vector its centroid
   * @throws IOException when if there is an error accessing vectors
   */
  public static Results cluster(
      RandomAccessVectorValues.Floats vectors,
      VectorSimilarityFunction similarityFunction,
      int numClusters)
      throws IOException {
    return cluster(
        vectors,
        numClusters,
        true,
        42L,
        KmeansInitializationMethod.PLUS_PLUS,
        similarityFunction == VectorSimilarityFunction.COSINE,
        DEFAULT_RESTARTS,
        DEFAULT_ITRS,
        DEFAULT_SAMPLE_SIZE);
  }

  /**
   * Expert: Cluster vectors into a given number of clusters
   *
   * @param vectors float vectors
   * @param numClusters number of cluster to cluster vector into
   * @param assignCentroidsToVectors if {@code true} assign centroids for all vectors. Centroids are
   *     computed on a sample of vectors. If this parameter is {@code true}, in results also return
   *     for all vectors what centroids they belong to.
   * @param seed random seed
   * @param initializationMethod Kmeans initialization method
   * @param normalizeCenters for cosine distance, set to true, to use spherical k-means where
   *     centers are normalized
   * @param restarts how many times to run Kmeans algorithm
   * @param iters how many iterations to do within a single run
   * @param sampleSize sample size to select from all vectors on which to run Kmeans algorithm
   * @return results of clustering: produced centroids and if {@code assignCentroidsToVectors ==
   *     true} also for each vector its centroid
   * @throws IOException if there is error accessing vectors
   */
  public static Results cluster(
      RandomAccessVectorValues.Floats vectors,
      int numClusters,
      boolean assignCentroidsToVectors,
      long seed,
      KmeansInitializationMethod initializationMethod,
      boolean normalizeCenters,
      int restarts,
      int iters,
      int sampleSize)
      throws IOException {
    if (numClusters < 1 || numClusters > MAX_NUM_CENTROIDS) {
      throw new IllegalArgumentException(
          "[numClusters] must be between [1] and [" + MAX_NUM_CENTROIDS + "]");
    }

    Random random = new Random(seed);
    float[][] centroids;
    if (numClusters > 1) {
      RandomAccessVectorValues.Floats sampleVectors =
          vectors.size() <= sampleSize ? vectors : createSampleReader(vectors, sampleSize, seed);
      KMeans kmeans =
          new KMeans(sampleVectors, numClusters, random, initializationMethod, restarts, iters);
      centroids = kmeans.computeCentroids(normalizeCenters);
    } else {
      centroids = new float[1][vectors.dimension()];
    }

    short[] vectorCentroids = null;
    // Assign each vector to the nearest centroid and update the centres
    if (assignCentroidsToVectors) {
      vectorCentroids = new short[vectors.size()];
      // Use kahan summation to get more precise results
      KMeans.runKMeansStep(vectors, random, centroids, vectorCentroids, true, normalizeCenters);
    }
    return new Results(centroids, vectorCentroids);
  }

  private KMeans(
      RandomAccessVectorValues.Floats vectors,
      int numCentroids,
      Random random,
      KmeansInitializationMethod initializationMethod,
      int restarts,
      int iters) {
    this.vectors = vectors;
    this.numVectors = vectors.size();
    this.numCentroids = numCentroids;
    this.random = random;
    this.initializationMethod = initializationMethod;
    this.restarts = restarts;
    this.iters = iters;
  }

  private float[][] computeCentroids(boolean normalizeCenters) throws IOException {
    short[] vectorCentroids = new short[numVectors];
    double minSquaredDist = Double.MAX_VALUE;
    double squaredDist = 0;
    float[][] bestCentroids = null;

    for (int restart = 0; restart < restarts; restart++) {
      float[][] centroids =
          switch (initializationMethod) {
            case FORGY -> initializeForgy();
            case RESERVOIR_SAMPLING -> initializeReservoirSampling();
            case PLUS_PLUS -> initializePlusPlus();
          };

      for (int iter = 0; iter < iters; iter++) {
        squaredDist =
            runKMeansStep(vectors, random, centroids, vectorCentroids, false, normalizeCenters);
      }
      if (squaredDist < minSquaredDist) {
        minSquaredDist = squaredDist;
        bestCentroids = centroids;
      }
    }
    return bestCentroids;
  }

  /**
   * Initialize centroids using Forgy method: randomly select numCentroids vectors for initial
   * centroids
   */
  private float[][] initializeForgy() throws IOException {
    Set<Integer> selection = new HashSet<>();
    while (selection.size() < numCentroids) {
      selection.add(random.nextInt(numVectors));
    }
    float[][] initialCentroids = new float[numCentroids][];
    int i = 0;
    for (Integer selectedIdx : selection) {
      float[] vector = vectors.vectorValue(selectedIdx);
      initialCentroids[i++] = ArrayUtil.copyOfSubArray(vector, 0, vector.length);
    }
    return initialCentroids;
  }

  /** Initialize centroids using a reservoir sampling method */
  private float[][] initializeReservoirSampling() throws IOException {
    float[][] initialCentroids = new float[numCentroids][];
    for (int index = 0; index < numVectors; index++) {
      float[] vector = vectors.vectorValue(index);
      if (index < numCentroids) {
        initialCentroids[index] = ArrayUtil.copyOfSubArray(vector, 0, vector.length);
      } else if (random.nextDouble() < numCentroids * (1.0 / index)) {
        int c = random.nextInt(numCentroids);
        initialCentroids[c] = ArrayUtil.copyOfSubArray(vector, 0, vector.length);
      }
    }
    return initialCentroids;
  }

  /** Initialize centroids using Kmeans++ method */
  private float[][] initializePlusPlus() throws IOException {
    float[][] initialCentroids = new float[numCentroids][];
    // Choose the first centroid uniformly at random
    int firstIndex = random.nextInt(numVectors);
    float[] value = vectors.vectorValue(firstIndex);
    initialCentroids[0] = ArrayUtil.copyOfSubArray(value, 0, value.length);

    // Store distances of each point to the nearest centroid
    float[] minDistances = new float[numVectors];
    Arrays.fill(minDistances, Float.MAX_VALUE);

    // Step 2 and 3: Select remaining centroids
    for (int i = 1; i < numCentroids; i++) {
      // Update distances with the new centroid
      double totalSum = 0;
      for (int j = 0; j < numVectors; j++) {
        // TODO: replace with RandomVectorScorer::score possible on quantized vectors
        float dist = VectorUtil.squareDistance(vectors.vectorValue(j), initialCentroids[i - 1]);
        if (dist < minDistances[j]) {
          minDistances[j] = dist;
        }
        totalSum += minDistances[j];
      }

      // Randomly select next centroid
      double r = totalSum * random.nextDouble();
      double cummulativeSum = 0;
      int nextCentroidIndex = -1;
      for (int j = 0; j < numVectors; j++) {
        cummulativeSum += minDistances[j];
        if (cummulativeSum >= r) {
          nextCentroidIndex = j;
          break;
        }
      }
      // Update centroid
      value = vectors.vectorValue(nextCentroidIndex);
      initialCentroids[i] = ArrayUtil.copyOfSubArray(value, 0, value.length);
    }
    return initialCentroids;
  }

  /**
   * Run kmeans step
   *
   * @param vectors float vectors
   * @param random random
   * @param centroids centroids, new calculated centroids are written here
   * @param docCentroids for each document which centroid it belongs to, results will be written
   *     here
   * @param useKahanSummation for large datasets use Kahan summation to calculate centroids, since
   *     we can easily reach the limits of float precision
   * @param normalizeCentroids if centroids should be normalized; used for cosine similarity only
   * @throws IOException if there is an error accessing vector values
   */
  private static double runKMeansStep(
      RandomAccessVectorValues.Floats vectors,
      Random random,
      float[][] centroids,
      short[] docCentroids,
      boolean useKahanSummation,
      boolean normalizeCentroids)
      throws IOException {
    short numCentroids = (short) centroids.length;

    float[][] newCentroids = new float[numCentroids][centroids[0].length];
    int[] newCentroidSize = new int[numCentroids];
    float[][] compensations = null;
    if (useKahanSummation) {
      compensations = new float[numCentroids][centroids[0].length];
    }

    double sumSquaredDist = 0;
    for (int docID = 0; docID < vectors.size(); docID++) {
      float[] vector = vectors.vectorValue(docID);

      short bestCentroid = 0;
      if (numCentroids > 1) {
        float minSquaredDist = Float.MAX_VALUE;
        for (short c = 0; c < numCentroids; c++) {
          // TODO: replace with RandomVectorScorer::score possible on quantized vectors
          float squareDist = VectorUtil.squareDistance(centroids[c], vector);
          if (squareDist < minSquaredDist) {
            bestCentroid = c;
            minSquaredDist = squareDist;
          }
        }
        sumSquaredDist += minSquaredDist;
      }

      newCentroidSize[bestCentroid] += 1;
      for (int dim = 0; dim < vector.length; dim++) {
        if (useKahanSummation) {
          float y = vector[dim] - compensations[bestCentroid][dim];
          float t = newCentroids[bestCentroid][dim] + y;
          compensations[bestCentroid][dim] = (t - newCentroids[bestCentroid][dim]) - y;
          newCentroids[bestCentroid][dim] = t;
        } else {
          newCentroids[bestCentroid][dim] += vector[dim];
        }
      }
      docCentroids[docID] = bestCentroid;
    }

    for (int c = 0; c < numCentroids; c++) {
      if (newCentroidSize[c] > 0) {
        for (int dim = 0; dim < newCentroids[c].length; dim++) {
          centroids[c][dim] = newCentroids[c][dim] / newCentroidSize[c];
        }
      } else {
        // this centroid did not get any points, assign a random data point to it
        int rndIdx = random.nextInt(docCentroids.length);
        float[] vector = vectors.vectorValue(rndIdx);
        for (int dim = 0; dim < newCentroids[c].length; dim++) {
          centroids[c][dim] = vector[dim];
        }
      }
    }
    if (normalizeCentroids) {
      for (int c = 0; c < centroids.length; c++) {
        VectorUtil.l2normalize(centroids[c], false);
      }
    }
    return sumSquaredDist;
  }

  /** Kmeans initialization methods */
  public enum KmeansInitializationMethod {
    FORGY,
    RESERVOIR_SAMPLING,
    PLUS_PLUS
  }

  /**
   * Results of KMeans clustering
   *
   * @param centroids the produced centroids
   * @param vectorCentroids for each vector which centroid it belongs to, we use short type, as we
   *     expect less than {@code MAX_NUM_CENTROIDS} which is equal to 32767 centroids. Can be {@code
   *     null} if they were not computed.
   */
  public record Results(float[][] centroids, short[] vectorCentroids) {}
}
