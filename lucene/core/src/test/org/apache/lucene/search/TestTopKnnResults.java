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
    TopDocs topDocs = results.topDocs().topDocs();
    int[] sortedNodes = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {2, 7, 8, 10, 4}, sortedNodes);
    assertArrayEquals(new float[] {4f, 2f, 2f, 1.2f, 1f}, sortedScores, 0f);
  }

  public void testSearchesForKButCollectsUpToGlobalK() {
    // searches for the best 2, but keeps up to 4 hits that were scored anyway
    TopKnnCollector results = new TopKnnCollector(2, 4, Integer.MAX_VALUE, null);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(1, 1f);
    results.collect(2, 2f);
    // the best 2 are known, so exploration stops below 1, but scored hits are still collectable
    assertEquals(1f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(Float.NEGATIVE_INFINITY, results.minCollectableSimilarity(), 0f);

    results.collect(3, 0.5f);
    results.collect(4, 3f);
    // the best 2 are now 3 and 2; the 4 kept hits bottom out at 0.5
    assertEquals(2f, results.minCompetitiveSimilarity(), 0f);
    assertEquals(0.5f, results.minCollectableSimilarity(), 0f);

    results.collect(5, 0.1f);
    KnnResult knnResult = results.topDocs();
    assertEquals(2, knnResult.strongResultCount());
    ScoreDoc[] scoreDocs = knnResult.topDocs().scoreDocs;
    assertEquals(4, scoreDocs.length);
    assertEquals(4, scoreDocs[0].doc);
    assertEquals(2, scoreDocs[1].doc);
    assertEquals(1, scoreDocs[2].doc);
    assertEquals(3, scoreDocs[3].doc);
    assertEquals(2, knnResult.strongTopDocs().scoreDocs.length);
  }

  public void testGlobalKMustNotBeLessThanK() {
    expectThrows(
        IllegalArgumentException.class, () -> new TopKnnCollector(5, 4, Integer.MAX_VALUE, null));
  }
}
