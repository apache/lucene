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

package org.apache.lucene.search;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTopKnnResults extends LuceneTestCase {

  public void testCollectAndProvideResults() {
    TopKnnCollector results = new TopKnnCollector(5, Integer.MAX_VALUE, null);
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
    for (int i = 0; i < nodes.length; i++) {
      results.collect(nodes[i], scores[i]);
    }
    TopDocs topDocs = results.topDocs();
    int[] sortedNodes = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {2, 7, 8, 10, 4}, sortedNodes);
    assertArrayEquals(new float[] {4f, 2f, 2f, 1.2f, 1f}, sortedScores, 0f);
  }

  public void testConcurrentGlobalMinSimilarity() {
    MaxScoreAccumulator globalMinSimAcc = new MaxScoreAccumulator();
    // greediness = 0.2; allowing to consider 4 top results out of 5
    TopKnnCollector results1 = new TopKnnCollector(5, Integer.MAX_VALUE, globalMinSimAcc, 0.2f);
    TopKnnCollector results2 = new TopKnnCollector(5, Integer.MAX_VALUE, globalMinSimAcc, 0.2f);

    int[] nodes1 = new int[] {1, 2, 3, 4, 5, 6, 7};
    float[] scores1 = new float[] {1f, 2f, 3f, 4f, 5f, 6f, 7f};
    int[] nodes2 = new int[] {8, 9, 10, 11, 12, 13, 14};
    float[] scores2 = new float[] {8f, 9f, 10f, 11f, 12f, 13f, 14f};

    for (int i = 0; i < 5; i++) {
      assertEquals(Float.NEGATIVE_INFINITY, results2.minCompetitiveSimilarity(), 0f);
      assertEquals(Float.NEGATIVE_INFINITY, results1.minCompetitiveSimilarity(), 0f);

      results2.collect(nodes2[i], scores2[i]);
      results2.incVisitedCount(1);
      results1.collect(nodes1[i], scores1[i]);
      results1.incVisitedCount(1);
    }
    // as soon as top k results are collected,
    // both collectors should start to see the global min similarity with greediness
    assertEquals(8f, results2.minCompetitiveSimilarity(), 0f);
    assertEquals(2f, results1.minCompetitiveSimilarity(), 0f);

    results2.collect(nodes2[5], scores2[5]);
    results2.incVisitedCount(1);
    results1.collect(nodes1[5], scores1[5]);
    results1.incVisitedCount(1);
    assertEquals(9f, results2.minCompetitiveSimilarity(), 0f);
    assertEquals(3f, results1.minCompetitiveSimilarity(), 0f);

    results2.collect(nodes2[6], scores2[6]);
    results2.incVisitedCount(1);
    results1.collect(nodes1[6], scores1[6]);
    results1.incVisitedCount(1);
    assertEquals(10f, results2.minCompetitiveSimilarity(), 0f);
    // as global similarity is updated periodically, the collect1 still sees the old cached global
    // value
    assertEquals(4f, results1.minCompetitiveSimilarity(), 0f);

    TopDocs topDocs1 = results1.topDocs();
    TopDocs topDocs2 = results2.topDocs();
    TopDocs topDocs = TopDocs.merge(5, new TopDocs[] {topDocs1, topDocs2});

    int[] sortedNodes = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {14, 13, 12, 11, 10}, sortedNodes);
    assertArrayEquals(new float[] {14f, 13, 12f, 11f, 10f}, sortedScores, 0f);
  }
}
