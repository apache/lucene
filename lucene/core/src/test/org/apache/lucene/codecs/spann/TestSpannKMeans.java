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

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSpannKMeans extends LuceneTestCase {

 public void testCleanSeparation() {
  float[][] points =
    new float[][] {
     {0, 0}, {0.1f, 0.1f}, // Cluster A
     {10, 10}, {10.1f, 10.1f} // Cluster B
    };

  // K=2, should perfectly separate
  float[][] centroids = SpannKMeans.cluster(points, 2, VectorSimilarityFunction.EUCLIDEAN, 10);

  assertEquals(2, centroids.length);

  // Verify centroids are close to (0,0) and (10,10)
  boolean hasZero = false;
  boolean hasTen = false;

  for (float[] c : centroids) {
   float seedDist0 = c[0] * c[0] + c[1] * c[1]; // SqrDist from 0,0
   float seedDist10 =
     (c[0] - 10) * (c[0] - 10) + (c[1] - 10) * (c[1] - 10); // SqrDist from 10,10

   if (seedDist0 < 1.0f) hasZero = true;
   if (seedDist10 < 1.0f) hasTen = true;
  }

  assertTrue("Expected centroid near origin", hasZero);
  assertTrue("Expected centroid near (10,10)", hasTen);
 }

 public void testFewerPointsThanClusters() {
  float[][] points = new float[][] {{1, 2}, {3, 4}};
  // Request K=5, but only have 2 points
  float[][] centroids = SpannKMeans.cluster(points, 5, VectorSimilarityFunction.EUCLIDEAN, 10);

  assertEquals(2, centroids.length);
  assertArrayEquals(points[0], centroids[0], 0.0f);
  assertArrayEquals(points[1], centroids[1], 0.0f);
 }

 public void testSingleCluster() {
  float[][] points = new float[][] {{1, 1}, {2, 2}, {3, 3}};
  // K=1, should average them -> (2, 2)
  float[][] centroids = SpannKMeans.cluster(points, 1, VectorSimilarityFunction.EUCLIDEAN, 10);

  assertEquals(1, centroids.length);
  assertEquals(2.0f, centroids[0][0], 0.001f);
  assertEquals(2.0f, centroids[0][1], 0.001f);
 }

 public void testEmptyInput() {
  float[][] points = new float[0][0];
  float[][] centroids = SpannKMeans.cluster(points, 5, VectorSimilarityFunction.EUCLIDEAN, 10);
  assertEquals(0, centroids.length);
 }
}
