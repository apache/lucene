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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public class TestPrefetchBackoff extends LuceneTestCase {

  private static void hit(PrefetchBackoff backoff, int times) {
    for (int i = 0; i < times; i++) {
      backoff.onHit();
    }
  }

  /**
   * Asserts the backoff is sampling: over SKIP * 4 calls from {@code seed}, exactly every SKIPth.
   */
  private static void assertSampling(PrefetchBackoff backoff, int seed) {
    int calls = seed;
    for (int i = 0; i < SKIP * 4; i++) {
      calls++;
      assertEquals("call " + calls, (calls & (SKIP - 1)) == 0, backoff.shouldProbe(calls));
    }
  }

  public void testConstants() {
    assertEquals(1, Integer.bitCount(N));
    assertEquals(1, Integer.bitCount(SKIP));
  }

  public void testStartsColdThenSamples() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    int seed = backoff.nextSeed();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe(++seed));
      backoff.onHit();
    }
    assertSampling(backoff, seed);
  }

  public void testPreloadedStartsSampling() {
    PrefetchBackoff backoff = new PrefetchBackoff(true);
    int seed = backoff.nextSeed();
    assertSampling(backoff, seed);
    // a miss still re-arms the full ramp
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe(++seed));
      backoff.onHit();
    }
    assertSampling(backoff, seed);
  }

  public void testMissProbesUnconditionallyUntilNHits() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    hit(backoff, N);
    assertSampling(backoff, 0);
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe(i));
      backoff.onHit();
    }
    assertSampling(backoff, 0);
  }

  public void testHitsPastNAreNoOps() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    hit(backoff, 100 * N);
    assertSampling(backoff, 0);
    // still exactly one miss away from re-arming
    backoff.onMiss();
    for (int i = 0; i < N; i++) {
      assertTrue("call " + i, backoff.shouldProbe(i));
      backoff.onHit();
    }
    assertSampling(backoff, 0);
  }

  public void testMissResetsAtAnyPoint() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    for (int hits : new int[] {0, 1, N / 2, N - 1, N, 10 * N}) {
      backoff.onMiss();
      hit(backoff, hits);
      backoff.onMiss();
      // 1 is never a sampling call, so these can only pass because the counter was reset
      assertTrue("hits=" + hits, backoff.shouldProbe(1));
      backoff.onHit();
      assertTrue("hits=" + hits, backoff.shouldProbe(1));
    }
  }

  public void testSeedsAreConsecutive() {
    PrefetchBackoff backoff = new PrefetchBackoff();
    int first = backoff.nextSeed();
    for (int i = 1; i < 3 * SKIP; i++) {
      assertEquals(first + i, backoff.nextSeed());
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
      for (int t = 0; t < threads; t++) {
        futures.add(exec.submit(() -> assertSampling(backoff, backoff.nextSeed())));
      }
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      exec.shutdown();
    }
  }
}
