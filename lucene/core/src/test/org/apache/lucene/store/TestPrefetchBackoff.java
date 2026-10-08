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

import static org.apache.lucene.store.PrefetchBackoff.N;
import static org.apache.lucene.store.PrefetchBackoff.SKIP;

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
      if (backoff.shouldProbe()) {
        probes++;
      }
    }
    return (double) probes / WINDOW;
  }

  private static void assertSampling(PrefetchBackoff backoff) {
    double expected = 1.0 / SKIP;
    assertEquals(expected, probeRate(backoff), expected * 0.05);
  }

  public void testConstants() {
    assertEquals(1, Integer.bitCount(N));
    assertEquals(1, Integer.bitCount(SKIP));
  }

  public void testStartsColdThenSamples() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe());
      backoff.onHit();
    }
    assertSampling(backoff);
  }

  public void testPreloadedStartsSampling() {
    PrefetchBackoff backoff = new PrefetchBackoff(true);
    assertSampling(backoff);
    // a miss still re-arms the full ramp
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe());
      backoff.onHit();
    }
    assertSampling(backoff);
  }

  public void testMissProbesUnconditionallyUntilNHits() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    hit(backoff, N);
    assertSampling(backoff);
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe());
      backoff.onHit();
    }
    assertSampling(backoff);
  }

  public void testHitsPastNAreNoOps() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    hit(backoff, 100 * N);
    assertSampling(backoff);
    // still exactly one miss away from re-arming
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe());
      backoff.onHit();
    }
    assertSampling(backoff);
  }

  public void testMissResetsAtAnyPoint() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int hits : new int[] {0, 1, N / 2, N - 1, N, 10 * N}) {
      backoff.onMiss();
      hit(backoff, hits);
      backoff.onMiss();
      assertTrue("hits=" + hits, backoff.shouldProbe());
      backoff.onHit();
      assertTrue("hits=" + hits, backoff.shouldProbe());
    }
  }

  public void testConcurrentHitsAndProbes() throws Exception {
    PrefetchBackoff backoff = new PrefetchBackoff();
    backoff.onMiss();
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

      AtomicInteger probes = new AtomicInteger();
      for (int t = 0; t < threads; t++) {
        futures.add(
            exec.submit(
                () -> {
                  int local = 0;
                  for (int i = 0; i < WINDOW; i++) {
                    if (backoff.shouldProbe()) {
                      local++;
                    }
                  }
                  probes.addAndGet(local);
                }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
      double expected = 1.0 / SKIP;
      assertEquals(expected, (double) probes.get() / (threads * WINDOW), expected * 0.05);
    } finally {
      exec.shutdown();
    }
  }
}
