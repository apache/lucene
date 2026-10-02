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
package org.apache.lucene.store;

import static org.apache.lucene.store.PrefetchBackoff.HITS_AT_MAX_SKIP;
import static org.apache.lucene.store.PrefetchBackoff.MAX_SKIP;
import static org.apache.lucene.store.PrefetchBackoff.MIN_SKIP;
import static org.apache.lucene.store.PrefetchBackoff.N;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public class TestPrefetchBackoff extends LuceneTestCase {

  private static final int WINDOW = 1 << 20;

  private static void hit(PrefetchBackoff backoff, int times) {
    for (int i = 0; i < times; i++) {
      backoff.onHit();
    }
  }

  private static double probeRate(PrefetchBackoff backoff) {
    int probes = 0;
    for (int i = 0; i < WINDOW; i++) {
      if (backoff.shouldProbe(i)) {
        probes++;
      }
    }
    return (double) probes / WINDOW;
  }

  private static void assertRate(double expected, double actual) {
    assertEquals(expected, actual, expected * 0.05);
  }

  public void testConstants() {
    assertEquals(1, Integer.bitCount(N));
    assertEquals(1, Integer.bitCount(MIN_SKIP));
    assertEquals(1, Integer.bitCount(MAX_SKIP));
    assertEquals(MAX_SKIP, PrefetchBackoff.skip(HITS_AT_MAX_SKIP));
    assertTrue(PrefetchBackoff.skip(HITS_AT_MAX_SKIP - 1) < MAX_SKIP);
  }

  public void testProbesUnconditionallyBeforeN() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe(random().nextInt()));
      backoff.onHit();
    }
  }

  public void testSkipSchedule() {
    assertEquals(MIN_SKIP, PrefetchBackoff.skip(N));
    assertEquals(MIN_SKIP, PrefetchBackoff.skip(2 * N - 1));
    assertEquals(2 * MIN_SKIP, PrefetchBackoff.skip(2 * N));
    assertEquals(4 * MIN_SKIP, PrefetchBackoff.skip(4 * N));
    assertEquals(MAX_SKIP, PrefetchBackoff.skip(HITS_AT_MAX_SKIP));
    assertEquals(MAX_SKIP, PrefetchBackoff.skip(2 * HITS_AT_MAX_SKIP));
    assertEquals(MAX_SKIP, PrefetchBackoff.skip(Integer.MAX_VALUE));
  }

  public void testProbeRateDoublesWithHits() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    int hits = 0;
    for (int skip = MIN_SKIP; skip <= MAX_SKIP; skip *= 2) {
      int target = N * (skip / MIN_SKIP);
      hit(backoff, target - hits);
      hits = target;
      assertRate(1.0 / skip, probeRate(backoff));
    }
  }

  public void testSaturatesAtMaxSkip() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    hit(backoff, 100 * HITS_AT_MAX_SKIP);
    assertRate(1.0 / MAX_SKIP, probeRate(backoff));
  }

  public void testMissResetsFromAnyTier() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int hits : new int[] {N, 3 * N, HITS_AT_MAX_SKIP, 10 * HITS_AT_MAX_SKIP}) {
      hit(backoff, hits);
      assertTrue(probeRate(backoff) < 1.0);
      backoff.onMiss();
      for (int i = 0; i < N; i++) {
        assertTrue("hits=" + hits + " call " + i, backoff.shouldProbe(random().nextInt()));
        backoff.onHit();
      }
      backoff.onMiss();
    }
  }

  public void testCadenceNotAlignedWithBatchLoops() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int tierHits : new int[] {N, HITS_AT_MAX_SKIP}) {
      backoff.onMiss();
      hit(backoff, tierHits);
      int skip = PrefetchBackoff.skip(tierHits);
      int[] probesPerResidue = new int[skip];
      for (int i = 0; i < WINDOW; i++) {
        if (backoff.shouldProbe(i)) {
          probesPerResidue[i % skip]++;
        }
      }
      // If probes lined up with the low bits of the counter, one residue would take all of them.
      // Counts for a well-mixed counter are Poisson around the mean, so allow a wide band.
      int expected = WINDOW / skip / skip;
      for (int r = 0; r < skip; r++) {
        String msg = "skip=" + skip + " residue " + r + " probes=" + probesPerResidue[r];
        assertTrue(msg, probesPerResidue[r] >= expected / 4);
        assertTrue(msg, probesPerResidue[r] <= expected * 4);
      }
    }
  }

  public void testConcurrentHitsAndProbes() throws Exception {
    PrefetchBackoff backoff = new PrefetchBackoff();
    int threads = 8;
    ExecutorService exec =
        Executors.newFixedThreadPool(threads, new NamedThreadFactory("TestPrefetchBackoff"));
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(exec.submit(() -> hit(backoff, N)));
      }
      for (Future<?> f : futures) {
        f.get();
      }
      futures.clear();
      // 8N total hits, possibly a few more from racy overshoot, is the 8 * MIN_SKIP tier
      int expectedSkip = PrefetchBackoff.skip(threads * N);
      assertEquals(8 * MIN_SKIP, expectedSkip);

      AtomicInteger probes = new AtomicInteger();
      for (int t = 0; t < threads; t++) {
        futures.add(
            exec.submit(
                () -> {
                  int local = 0;
                  for (int i = 0; i < WINDOW; i++) {
                    if (backoff.shouldProbe(i)) {
                      local++;
                    }
                  }
                  probes.addAndGet(local);
                }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
      assertRate(1.0 / expectedSkip, (double) probes.get() / (threads * WINDOW));
    } finally {
      exec.shutdown();
    }
  }
}
