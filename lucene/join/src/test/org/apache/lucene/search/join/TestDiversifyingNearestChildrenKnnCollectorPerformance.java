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

package org.apache.lucene.search.join;

import java.io.IOException;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BitSet;

/**
 * Tests for {@link DiversifyingNearestChildrenKnnCollector}.
 *
 * <p>Correctness tests verify behaviour of the collector in various scenarios, including edge
 * cases.
 */
public class TestDiversifyingNearestChildrenKnnCollectorPerformance
    extends DiversifyingChildrenKnnCollectorTestCase {

  /** Collects all children in order and returns topDocs. */
  private static TopDocs collectAll(int k, BitSet parents, int[] childIds, float[] scores) {
    DiversifyingNearestChildrenKnnCollector collector =
        new DiversifyingNearestChildrenKnnCollector(k, Integer.MAX_VALUE, null, parents, null);
    for (int i = 0; i < childIds.length; i++) {
      collector.collect(childIds[i], scores[i]);
    }
    return collector.topDocs();
  }

  private int[] buildChildIds(int numParents, int childrenPerParent) {
    int totalChildren = numParents * childrenPerParent;
    int[] childIds = new int[totalChildren];
    for (int childIndex = 0; childIndex < childrenPerParent; childIndex++) {
      childIds[childIndex] = childIndex;
    }

    for (int childIndex = childrenPerParent; childIndex < totalChildren; childIndex++) {
      int parentCounter = childIndex / childrenPerParent;
      int previousParentDocId = (parentCounter) * (childrenPerParent + 1) - 1;
      int offset = (childIndex + 1) % childrenPerParent;
      if (offset == 0) {
        offset = childrenPerParent;
      }
      childIds[childIndex] = previousParentDocId + offset;
    }
    return childIds;
  }

  private float[] buildChildScores(int numParents, int childrenPerParent) {
    int totalChildren = numParents * childrenPerParent;
    float[] scores = new float[totalChildren];
    for (int childIndex = 0; childIndex < totalChildren; childIndex++) {
      scores[childIndex] = (childIndex + 1) * 0.1f;
    }
    return scores;
  }

  public void testCollect_shouldReturnSameAsBruteForceOrdering() throws IOException {
    int numParents = 200;
    int childrenPerParent = 10;
    BitSet parents = parentBitSet(numParents, childrenPerParent);

    int totalChildren = numParents * childrenPerParent;
    int[] childIds = buildChildIds(numParents, childrenPerParent);
    float[] scores = buildChildScores(numParents, childrenPerParent);

    // Brute-force: best (child, score) per parent, sorted by score desc, take top-k
    int[] bestChild = new int[numParents];
    float[] bestScore = new float[numParents];
    java.util.Arrays.fill(bestScore, Float.NEGATIVE_INFINITY);
    for (int ci = 0; ci < totalChildren; ci++) {
      int p = ci / childrenPerParent;
      if (scores[ci] > bestScore[p]) {
        bestScore[p] = scores[ci];
        bestChild[p] = childIds[ci];
      }
    }

    for (int k : new int[] {10, 50, 100}) {
      TopDocs topDocs = collectAll(k, parents, childIds, scores);
      assertEquals("size k=" + k, Math.min(k, numParents), topDocs.scoreDocs.length);
      int parentIndex = numParents - 1;
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        int actualDocId = topDocs.scoreDocs[i].doc;
        float actualScore = topDocs.scoreDocs[i].score;
        assertEquals("wrong result set for k=" + k, bestChild[parentIndex], actualDocId);
        assertEquals("wrong result set for k=" + k, bestScore[parentIndex], actualScore, 0f);
        parentIndex--;
      }
    }
  }

  /**
   * When the heap is full (size == k) and a better candidate arrives for a new parent, the
   * worst-scoring entry must be evicted. Tests the {@code updateTop} / overflow path.
   */
  public void testOverflowEvictsLowestScore() throws IOException {
    // k=3, 6 parents → last 3 parents must evict first 3 if their scores are higher
    int topK = 3;
    int numParents = 6;
    int childrenPerParent = 1;
    BitSet parents = parentBitSet(numParents, childrenPerParent);

    // Scores: first k parents get low scores, next k get high scores
    int[] childIds = buildChildIds(numParents, childrenPerParent);
    float[] scores = new float[numParents];
    for (int p = 0; p < numParents; p++) {
      scores[p] = p < topK ? 0.1f * (p + 1) : 0.9f - 0.1f * (p - topK);
    }

    TopDocs topDocs = collectAll(topK, parents, childIds, scores);
    assertEquals(topK, topDocs.scoreDocs.length);

    // All returned scores must be >= the lowest score of the high-score group
    float minHighScore = Float.MAX_VALUE;
    for (int p = topK; p < numParents; p++) {
      minHighScore = Math.min(minHighScore, scores[p]);
    }
    for (var sd : topDocs.scoreDocs) {
      assertTrue(
          "evicted result has lower score than expected: " + sd.score,
          sd.score >= minHighScore - 1e-6f);
    }
  }
}
