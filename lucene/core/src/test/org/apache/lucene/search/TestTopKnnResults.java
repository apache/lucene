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
    TopKnnCollector results = new TopKnnCollector(5, Integer.MAX_VALUE);
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
    for (int i = 0; i < nodes.length; i++) {
      results.collect(nodes[i], scores[i]);
    }
    TopDocs topDocs = results.knnResult().topDocs();
    int[] sortedNodes = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {2, 7, 8, 10, 4}, sortedNodes);
    assertArrayEquals(new float[] {4f, 2f, 2f, 1.2f, 1f}, sortedScores, 0f);
  }

  /** When globalK > k, hits beyond the best k are kept but only the best k are "strong". */
  public void testGlobalKLargerThanK() {
    TopKnnCollector results = new TopKnnCollector(2, 4, Integer.MAX_VALUE);
    assertTrue(results.collect(1, 0.5f));
    assertTrue(results.collect(2, 0.9f));
    assertTrue(results.collect(3, 0.7f)); // demote doc 1 to the weak queue
    assertTrue(results.collect(4, 0.6f)); // straight to the weak queue
    assertTrue(results.collect(6, 0.95f)); // demote doc 3 to the weak queue, evict doc 1
    assertFalse(results.collect(5, 0.4f));

    assertEquals(4, results.numCollected());
    KnnResult result = results.knnResult();
    ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;

    assertEquals(KnnResult.Method.APPROXIMATE, result.strategy());
    assertEquals(2, result.strongResultCount());
    assertEquals(4, scoreDocs.length);
    assertArrayEquals(new int[] {6, 2, 3, 4}, docsOf(scoreDocs));
    assertArrayEquals(new float[] {0.95f, 0.9f, 0.7f, 0.6f}, scoresOf(scoreDocs), 0f);
  }

  /** Competitiveness (how far the search explores) is governed by k, not globalK. */
  public void testMinCompetitiveSimilarityIgnoresWeakHits() {
    TopKnnCollector results = new TopKnnCollector(2, 4, Integer.MAX_VALUE);
    results.collect(1, 0.5f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(2, 0.9f);
    assertEquals(0.5f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(3, 0.8f);
    results.collect(4, 0.7f);
    assertEquals(0.8f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(0.5f, results.minCollectableSimilarity(), 0f);
  }

  /** With globalK == k the collector degenerates to a single queue. */
  public void testGlobalKEqualToK() {
    TopKnnCollector results = new TopKnnCollector(2, 2, Integer.MAX_VALUE);
    results.collect(1, 0.5f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(2, 0.9f);
    assertEquals(0.5f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(0.5f, results.minCollectableSimilarity(), 0f);

    KnnResult result = results.knnResult();
    assertEquals(2, result.strongResultCount());
    assertEquals(2, result.topDocs().scoreDocs.length);
  }

  /** globalK below k is bumped up to k. */
  public void testGlobalKBelowKIsClamped() {
    TopKnnCollector results = new TopKnnCollector(2, 1, Integer.MAX_VALUE);
    results.collect(1, 0.5f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(2, 0.9f);
    assertEquals(0.5f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(0.5f, results.minCollectableSimilarity(), 0f);

    KnnResult result = results.knnResult();
    assertEquals(KnnResult.Method.APPROXIMATE, result.strategy());
    assertEquals(2, result.strongResultCount());
    assertEquals(2, result.topDocs().scoreDocs.length);
  }

  /** An exhaustive scan marks every collected hit as a strong result. */
  public void testExactMethodologyMakesAllResultsStrong() {
    TopKnnCollector results = new TopKnnCollector(2, 4, Integer.MAX_VALUE);
    results.collect(1, 0.5f);
    results.collect(2, 0.9f);
    results.collect(3, 0.7f);
    results.setMethodology(KnnResult.Method.EXACT);

    KnnResult result = results.knnResult();

    assertEquals(KnnResult.Method.EXACT, result.strategy());
    assertEquals(3, result.topDocs().scoreDocs.length);
    assertEquals(3, result.strongResultCount());
  }

  private static int[] docsOf(ScoreDoc[] scoreDocs) {
    int[] docs = new int[scoreDocs.length];
    for (int i = 0; i < docs.length; i++) {
      docs[i] = scoreDocs[i].doc;
    }
    return docs;
  }

  private static float[] scoresOf(ScoreDoc[] scoreDocs) {
    float[] scores = new float[scoreDocs.length];
    for (int i = 0; i < scores.length; i++) {
      scores[i] = scoreDocs[i].score;
    }
    return scores;
  }
}
