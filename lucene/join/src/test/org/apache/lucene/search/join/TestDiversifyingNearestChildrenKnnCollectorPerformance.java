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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.BitSet;

/**
 * Performance and correctness tests for {@link DiversifyingNearestChildrenKnnCollector}.
 *
 * <p>Correctness tests verify behaviour
 *
 * <p>Throughput tests print ops/sec figures to stdout so you can compare runs before and after a
 * change. They are annotated {@code @Nightly} and are skipped in normal CI; run them explicitly
 * with:
 *
 * <pre>
 *   ./gradlew -p lucene/join test --tests TestDiversifyingNearestChildrenKnnCollectorPerformance -Ptests.verbose=true -Ptests.filter=@Nightly
 * </pre>
 */
public class TestDiversifyingNearestChildrenKnnCollectorPerformance extends LuceneTestCase {

  /** Builds a BitSet whose set bits are the parent doc ids in a contiguous block-join layout. */
  private static BitSet parentBitSet(int numParents, int childrenPerParent) throws IOException {
    int[] parentDocIds = new int[numParents];
    for (int p = 1; p <= numParents; p++) {
      // layout: [child_0 … child_{C-1}, parent_C], repeated
      // e.g. with 3 children per parent: [0,1,2,3, 4,5,6,7, 8,9,10,11, ...] → parent doc ids are
      // 3,7,11,...
      parentDocIds[p - 1] = p * (childrenPerParent + 1) - 1;
    }
    int totalDocs = numParents * (childrenPerParent + 1); // children + 1 parent per block
    return BitSet.of(
        new TestToParentJoinKnnResults.IntArrayDocIdSetIterator(parentDocIds, numParents),
        totalDocs + 1);
  }

  /** Collects all children in order and returns topDocs. */
  private static TopDocs collectAll(int k, BitSet parents, int[] childIds, float[] scores) {
    DiversifyingNearestChildrenKnnCollector collector =
        new DiversifyingNearestChildrenKnnCollector(k, Integer.MAX_VALUE, parents);
    for (int i = 0; i < childIds.length; i++) {
      collector.collect(childIds[i], scores[i]);
    }
    return collector.topDocs();
  }

  public void testCollect_shouldReturnSameAsBruteForceOrdering() throws IOException {
    int numParents = 200;
    int childrenPerParent = 3;
    BitSet parents = parentBitSet(numParents, childrenPerParent);

    int totalChildren = numParents * childrenPerParent;
    int[] childIds = new int[totalChildren];
    float[] scores = new float[totalChildren];
    // Strictly-increasing scores: last child of each parent is always the best.
    for (int childIndex = 0; childIndex < childrenPerParent; childIndex++) {
      childIds[childIndex] = childIndex;
      scores[childIndex] = (childIndex + 1) * 0.1f;
    }

    for (int childIndex = childrenPerParent; childIndex < totalChildren; childIndex++) {
      int parentCounter = childIndex / childrenPerParent;
      int previousParentDocId = (parentCounter) * (childrenPerParent + 1) - 1;
      int offset = (childIndex + 1) % childrenPerParent;
      if (offset == 0) {
        offset = childrenPerParent;
      }
      childIds[childIndex] = previousParentDocId + offset;
      scores[childIndex] = (childIndex + 1) * 0.1f;
    }

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
   * When many children map to the same parent, only the best-scoring child per parent must appear
   * in the result. Tests the {@code updateElement} path.
   */
  public void testBestChildPerParentIsRetained() throws IOException {
    // 5 parents, 20 children each → heavy updateElement() pressure on a small heap
    int numParents = 5;
    int childrenPerParent = 20;
    BitSet parents = parentBitSet(numParents, childrenPerParent);

    int[] childIds = new int[numParents * childrenPerParent];
    float[] scores = new float[childIds.length];
    // Track expected best child and score per parent
    int[] bestChild = new int[numParents];
    float[] bestScore = new float[numParents];
    java.util.Arrays.fill(bestScore, Float.NEGATIVE_INFINITY);

    int ci = 0;
    for (int p = 0; p < numParents; p++) {
      int parentDoc = (p + 1) * (childrenPerParent + 1) - 1;
      for (int c = 0; c < childrenPerParent; c++) {
        int childDoc = parentDoc - childrenPerParent + c;
        float score = (float) (ci % 100) / 100f; // deterministic, varied
        childIds[ci] = childDoc;
        scores[ci] = score;
        if (score > bestScore[p]) {
          bestScore[p] = score;
          bestChild[p] = childDoc;
        }
        ci++;
      }
    }

    TopDocs topDocs = collectAll(numParents, parents, childIds, scores);
    assertEquals(numParents, topDocs.scoreDocs.length);

    // Each result doc must be the best child of its parent
    java.util.Set<Integer> resultDocs = new java.util.HashSet<>();
    for (var sd : topDocs.scoreDocs) {
      resultDocs.add(sd.doc);
    }
    for (int p = 0; p < numParents; p++) {
      assertTrue(
          "expected best child " + bestChild[p] + " for parent " + p + " in results",
          resultDocs.contains(bestChild[p]));
    }
  }

  /**
   * When the heap is full (size == k) and a better candidate arrives for a new parent, the
   * worst-scoring entry must be evicted. Tests the {@code updateTop} / overflow path.
   */
  public void testOverflowEvictsLowestScore() throws IOException {
    // k=3, 6 parents → last 3 parents must evict first 3 if their scores are higher
    int k = 3;
    int numParents = 6;
    BitSet parents = parentBitSet(numParents, 1 /* one child per parent */);

    // Scores: first k parents get low scores, next k get high scores
    int[] childIds = new int[numParents];
    float[] scores = new float[numParents];
    for (int p = 0; p < numParents; p++) {
      childIds[p] = p * 2; // child doc ids (parent at p*2+1)
      scores[p] = p < k ? 0.1f * (p + 1) : 0.9f - 0.1f * (p - k);
    }

    TopDocs topDocs = collectAll(k, parents, childIds, scores);
    assertEquals(k, topDocs.scoreDocs.length);

    // All returned scores must be >= the lowest score of the high-score group
    float minHighScore = Float.MAX_VALUE;
    for (int p = k; p < numParents; p++) {
      minHighScore = Math.min(minHighScore, scores[p]);
    }
    for (var sd : topDocs.scoreDocs) {
      assertTrue(
          "evicted result has lower score than expected: " + sd.score,
          sd.score >= minHighScore - 1e-6f);
    }
  }

  // ---------------------------------------------------------------------------
  // Throughput benchmarks (annotated @Nightly, not run in normal CI)
  // ---------------------------------------------------------------------------

  private static final int WARMUP_ITERS = 5;
  private static final int MEASURE_ITERS = 20;

  /**
   * Runs {@code MEASURE_ITERS} full collect+drain cycles and prints throughput to stdout.
   *
   * @param label short description printed in the output line
   * @param k heap capacity
   * @param numParents number of parent docs
   * @param childrenPerParent children per parent (controls update vs insert pressure)
   */
  private void benchmark(String label, int k, int numParents, int childrenPerParent)
      throws IOException {
    BitSet parents = parentBitSet(numParents, childrenPerParent);
    int totalChildren = numParents * childrenPerParent;
    int[] childIds = new int[totalChildren];
    float[] scores = new float[totalChildren];
    int ci = 0;
    for (int p = 0; p < numParents; p++) {
      int parentDoc = (p + 1) * (childrenPerParent + 1) - 1;
      for (int c = 0; c < childrenPerParent; c++) {
        childIds[ci] = parentDoc - childrenPerParent + c;
        scores[ci] = (float) (ci % 1000) / 1000f;
        ci++;
      }
    }

    // Warmup (results discarded)
    for (int iter = 0; iter < WARMUP_ITERS; iter++) {
      DiversifyingNearestChildrenKnnCollector collector =
          new DiversifyingNearestChildrenKnnCollector(k, Integer.MAX_VALUE, parents);
      for (int i = 0; i < totalChildren; i++) {
        collector.collect(childIds[i], scores[i]);
      }
      collector.topDocs();
    }

    // Measured
    long totalNanos = 0;
    for (int iter = 0; iter < MEASURE_ITERS; iter++) {
      DiversifyingNearestChildrenKnnCollector collector =
          new DiversifyingNearestChildrenKnnCollector(k, Integer.MAX_VALUE, parents);
      long t0 = System.nanoTime();
      for (int i = 0; i < totalChildren; i++) {
        collector.collect(childIds[i], scores[i]);
      }
      collector.topDocs();
      totalNanos += System.nanoTime() - t0;
    }

    long avgNs = totalNanos / MEASURE_ITERS;
    double opsPerSec = (double) totalChildren * 1_000_000_000L / avgNs;
    System.out.printf(
        "%-42s  k=%4d  children=%7d  avg=%6.2f ms  %.2f M collect/sec%n",
        label, k, totalChildren, avgNs / 1e6, opsPerSec / 1_000_000);
  }

  @Nightly
  public void testThroughput_SmallK_SmallK() throws IOException {
    benchmark("small-k", 10, 10_000, 10);
  }

  @Nightly
  public void testThroughput_ThresholdK_MediumK() throws IOException {
    benchmark("medium-k", 32, 10_000, 10);
  }

  @Nightly
  public void testThroughput_MediumK_HighK() throws IOException {
    benchmark("high-k", 100, 10_000, 10);
  }

  @Nightly
  public void testThroughput_DenseUpdates_smallK() throws IOException {
    // Many children per parent → heavy updateElement() pressure, small k
    benchmark("dense-updates", 10, 1_000, 100);
  }

  @Nightly
  public void testThroughput_DenseUpdates_highK() throws IOException {
    // Many children per parent → heavy updateElement() pressure, large k
    benchmark("dense-updates high k", 100, 1_000, 100);
  }
}
