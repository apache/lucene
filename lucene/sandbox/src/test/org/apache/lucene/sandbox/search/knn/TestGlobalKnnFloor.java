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

package org.apache.lucene.sandbox.search.knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;

public class TestGlobalKnnFloor extends LuceneTestCase {

  public void testInvalidK() {
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> new GlobalKnnFloor(0));
    assertTrue(e.getMessage(), e.getMessage().contains("k must be at least 1"));
    expectThrows(IllegalArgumentException.class, () -> new GlobalKnnFloor(-1));
  }

  public void testFloorUndefinedUntilKScoresObserved() {
    int k = TestUtil.nextInt(random(), 1, 50);
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    assertEquals(Float.NEGATIVE_INFINITY, floor.floor(), 0.0f);
    for (int i = 0; i < k - 1; i++) {
      float returned = floor.offer(new float[] {random().nextFloat()}, 1);
      assertEquals(
          "floor must stay undefined while fewer than k scores have been observed",
          Float.NEGATIVE_INFINITY,
          returned,
          0.0f);
    }
    float kthArrival = random().nextFloat();
    float returned = floor.offer(new float[] {kthArrival}, 1);
    assertTrue(
        "floor must be defined once k scores have been observed",
        returned > Float.NEGATIVE_INFINITY);
  }

  public void testFloorIsKthBestOfEverythingOffered() {
    int k = TestUtil.nextInt(random(), 1, 32);
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    int totalScores = TestUtil.nextInt(random(), k, 500);
    List<Float> allScores = new ArrayList<>(totalScores);
    int offered = 0;
    while (offered < totalScores) {
      int batchSize = TestUtil.nextInt(random(), 1, Math.min(16, totalScores - offered));
      float[] batch = new float[batchSize];
      for (int i = 0; i < batchSize; i++) {
        batch[i] = random().nextFloat() * 10;
        allScores.add(batch[i]);
      }
      Arrays.sort(batch);
      floor.offer(batch, batchSize);
      offered += batchSize;
    }
    allScores.sort(Comparator.reverseOrder());
    float expectedKthBest = allScores.get(k - 1);
    assertEquals(expectedKthBest, floor.floor(), 0.0f);
  }

  public void testFloorIsMonotonic() {
    int k = TestUtil.nextInt(random(), 1, 16);
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    float previous = floor.floor();
    for (int batch = 0; batch < 50; batch++) {
      int batchSize = TestUtil.nextInt(random(), 1, 8);
      float[] scores = new float[batchSize];
      for (int i = 0; i < batchSize; i++) {
        scores[i] = (random().nextFloat() - 0.5f) * 100;
      }
      Arrays.sort(scores);
      float current = floor.offer(scores, batchSize);
      assertTrue("floor must never decrease", current >= previous);
      previous = current;
    }
  }

  public void testAdvertiseIsIndependentOfHeapFill() {
    // An advertised bound is valid on its own authority; it must define the floor even when no
    // local scores have been observed yet.
    GlobalKnnFloor floor = new GlobalKnnFloor(10);
    floor.advertise(0.5f);
    assertEquals(0.5f, floor.floor(), 0.0f);
  }

  public void testAdvertiseBelowCurrentFloorIsIgnored() {
    GlobalKnnFloor floor = new GlobalKnnFloor(10);
    floor.advertise(0.5f);
    floor.advertise(0.4f);
    assertEquals(
        "a lower advertised bound must not lower the floor: duplicated or reordered remote "
            + "deliveries rely on this",
        0.5f,
        floor.floor(),
        0.0f);
    floor.advertise(0.6f);
    assertEquals(0.6f, floor.floor(), 0.0f);
  }

  public void testAdvertiseCombinesWithLocalScores() {
    int k = 4;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    float[] scores = new float[] {0.1f, 0.2f, 0.3f, 0.4f};
    floor.offer(scores, scores.length);
    assertEquals(0.1f, floor.floor(), 0.0f);
    // A remote bound above the local k-th best takes over.
    floor.advertise(0.25f);
    assertEquals(0.25f, floor.floor(), 0.0f);
    // Local scores that push the local k-th best above the advertised bound take over again.
    floor.offer(new float[] {0.5f, 0.6f, 0.7f}, 3);
    // Heap now holds {0.4, 0.5, 0.6, 0.7}: local k-th best is 0.4.
    assertEquals(0.4f, floor.floor(), 0.0f);
  }

  public void testAdvertiseRejectsNaN() {
    GlobalKnnFloor floor = new GlobalKnnFloor(10);
    expectThrows(IllegalArgumentException.class, () -> floor.advertise(Float.NaN));
  }

  public void testOfferRejectsNonPositiveLen() {
    GlobalKnnFloor floor = new GlobalKnnFloor(10);
    expectThrows(IllegalArgumentException.class, () -> floor.offer(new float[] {1f}, 0));
    expectThrows(IllegalArgumentException.class, () -> floor.offer(new float[] {1f}, -1));
  }

  public void testConcurrentPublishersConvergeToKthBest() throws Exception {
    int k = 64;
    int numThreads = TestUtil.nextInt(random(), 2, 8);
    int batchesPerThread = 50;
    int maxBatchSize = 16;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);

    // Pre-generate every thread's scores on the test thread so the expected k-th best can be
    // computed independently of scheduling.
    float[][][] batches = new float[numThreads][batchesPerThread][];
    List<Float> allScores = new ArrayList<>();
    for (int t = 0; t < numThreads; t++) {
      for (int b = 0; b < batchesPerThread; b++) {
        int batchSize = TestUtil.nextInt(random(), 1, maxBatchSize);
        float[] batch = new float[batchSize];
        for (int i = 0; i < batchSize; i++) {
          batch[i] = random().nextFloat();
          allScores.add(batch[i]);
        }
        Arrays.sort(batch);
        batches[t][b] = batch;
      }
    }
    allScores.sort(Comparator.reverseOrder());
    float expectedKthBest = allScores.get(k - 1);

    ExecutorService executor =
        Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("global-knn-floor-test"));
    try {
      CountDownLatch startingGun = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(numThreads);
      for (int t = 0; t < numThreads; t++) {
        final int thread = t;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    startingGun.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  float previous = Float.NEGATIVE_INFINITY;
                  for (float[] batch : batches[thread]) {
                    float current = floor.offer(batch, batch.length);
                    assertTrue(
                        "floor must appear monotonic to every publisher", current >= previous);
                    previous = current;
                  }
                }));
      }
      startingGun.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
    assertEquals(
        "after all publishers finish, the floor must be the k-th best of every score offered",
        expectedKthBest,
        floor.floor(),
        0.0f);
  }

  public void testConcurrentFillNeverOvershootsTrueKthBest() throws Exception {
    // Regression tripwire for a heap-fill race: a publisher whose batch lands while the heap is
    // not yet full must not treat its returned heap top as a valid bound, even if a concurrent
    // publisher fills the heap in the window between the offer returning and the fullness flag
    // being read. The scenario is adversarial by construction: one thread offers high scores that
    // do not fill the heap on their own, the other offers low scores that complete the fill. The
    // true k-th best is a low score, so a stale partial-heap top (a high score) published as the
    // floor overshoots it — and because the floor is a monotonic maximum, a single overshoot
    // survives to the final assertion. The race window is narrow, hence the many short rounds.
    int rounds = 500;
    int k = 32;
    ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("global-knn-floor-race-test"));
    try {
      for (int round = 0; round < rounds; round++) {
        GlobalKnnFloor floor = new GlobalKnnFloor(k);
        float[] highs = new float[k - 1];
        for (int i = 0; i < highs.length; i++) {
          highs[i] = 0.9f + i * 0.001f;
        }
        float[] lows = new float[k];
        for (int i = 0; i < lows.length; i++) {
          lows[i] = 0.1f + i * 0.001f;
        }
        // Top-k of the union: all k-1 highs plus the single best low, whose score is the k-th
        // best and the highest floor any correct execution can reach.
        float trueKthBest = lows[lows.length - 1];

        CountDownLatch startingGun = new CountDownLatch(1);
        Future<?> highPublisher =
            executor.submit(
                () -> {
                  try {
                    startingGun.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  floor.offer(highs, highs.length);
                });
        Future<?> lowPublisher =
            executor.submit(
                () -> {
                  try {
                    startingGun.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  floor.offer(lows, lows.length);
                });
        startingGun.countDown();
        highPublisher.get();
        lowPublisher.get();

        assertEquals(
            "round " + round + ": the floor must be exactly the k-th best of everything offered",
            trueKthBest,
            floor.floor(),
            0.0f);
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }
}
