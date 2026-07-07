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

import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFloorAwareKnnCollector extends LuceneTestCase {

  public void testInvalidGreediness() {
    GlobalKnnFloor floor = new GlobalKnnFloor(4);
    TopKnnCollector delegate = new TopKnnCollector(4, Integer.MAX_VALUE);
    expectThrows(
        IllegalArgumentException.class, () -> new FloorAwareKnnCollector(delegate, floor, -0.1f));
    expectThrows(
        IllegalArgumentException.class, () -> new FloorAwareKnnCollector(delegate, floor, 1.1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new FloorAwareKnnCollector(delegate, floor, Float.NaN));
  }

  public void testInvalidMinExplorationSlots() {
    GlobalKnnFloor floor = new GlobalKnnFloor(4);
    TopKnnCollector delegate = new TopKnnCollector(4, Integer.MAX_VALUE);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate, floor, 0.5f, 0, FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate, floor, 0.5f, -1, FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL));
  }

  public void testInvalidSyncInterval() {
    GlobalKnnFloor floor = new GlobalKnnFloor(4);
    TopKnnCollector delegate = new TopKnnCollector(4, Integer.MAX_VALUE);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate, floor, 0.5f, FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS, 0));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate, floor, 0.5f, FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS, -8));
    // Any positive interval is valid: synchronization is scheduled against a visited-count
    // threshold, not a bit mask, so no power-of-two restriction applies.
    new FloorAwareKnnCollector(
        delegate, floor, 0.5f, FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS, 100);
  }

  public void testInvalidGateK() {
    GlobalKnnFloor floor = new GlobalKnnFloor(8);
    TopKnnCollector delegate = new TopKnnCollector(8, Integer.MAX_VALUE);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate,
                floor,
                0.5f,
                FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS,
                FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL,
                0));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate,
                floor,
                0.5f,
                FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS,
                FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL,
                -1));
    // A gate above the queue size could never open: the queue's size saturates at its capacity.
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new FloorAwareKnnCollector(
                delegate,
                floor,
                0.5f,
                FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS,
                FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL,
                9));
  }

  public void testGreedinessForClampRoundTrips() {
    // The helper must invert the clamp sizing: constructing a collector with the derived
    // greediness yields a clamp of exactly the requested width, at any gate size. This is the
    // property that makes width-first configuration safe where a constant greediness is not.
    int[][] cases = {{6000, 600}, {184, 64}, {1012, 101}, {117, 58}, {10000, 16}};
    for (int[] c : cases) {
      int gateK = c[0];
      int clampSlots = c[1];
      float greediness = FloorAwareKnnCollector.greedinessForClamp(gateK, clampSlots);
      assertEquals(
          "gateK=" + gateK + " clampSlots=" + clampSlots,
          clampSlots,
          Math.round((1 - greediness) * gateK));
    }
    // A width of the whole gate or more means the floor cannot bind: greediness 0, stock search.
    assertEquals(0f, FloorAwareKnnCollector.greedinessForClamp(44, 64), 0.0f);
    assertEquals(0f, FloorAwareKnnCollector.greedinessForClamp(64, 64), 0.0f);

    expectThrows(
        IllegalArgumentException.class, () -> FloorAwareKnnCollector.greedinessForClamp(0, 16));
    expectThrows(
        IllegalArgumentException.class, () -> FloorAwareKnnCollector.greedinessForClamp(100, 0));
  }

  public void testGateBelowQueueSizeOpensAtGateKResults() {
    // A share-sized gate: the local queue holds 32 results but the collector's expected share of
    // the merged top-k is only 8, so the floor must engage after 8 collected results, long before
    // the queue fills. The local k-th best is still undefined at that point, so the exposed bound
    // comes entirely from min(clamp, floor).
    int queueSize = 32;
    int gateK = 8;
    GlobalKnnFloor floor = new GlobalKnnFloor(queueSize);
    floor.advertise(1000f);
    TopKnnCollector delegate = new TopKnnCollector(queueSize, Integer.MAX_VALUE);
    // greediness 1 with a slot minimum of 2 keeps the two best scores seen as exploration slots.
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(
            delegate, floor, 1f, 2, FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL, gateK);

    for (int doc = 0; doc < gateK - 1; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
      assertEquals(
          "the shared floor must be invisible until gateK results are collected",
          Float.NEGATIVE_INFINITY,
          collector.minCompetitiveSimilarity(),
          0.0f);
    }

    collector.incVisitedCount(1);
    collector.collect(gateK - 1, (float) gateK);
    // Scores are 1..8: the clamp queue keeps {7, 8}, so the bound is min(7, nextDown(1000)) = 7,
    // while the delegate's own bound is still NEGATIVE_INFINITY because its queue is not full.
    assertEquals(Float.NEGATIVE_INFINITY, delegate.minCompetitiveSimilarity(), 0.0f);
    assertEquals(
        "once the gate opens, the bound must come from the clamp and the floor",
        7f,
        collector.minCompetitiveSimilarity(),
        0.0f);
  }

  public void testGreedinessClampIsSizedFromGateK() {
    // With a queue of 40 but a gate of 8 at greediness 0.5, the clamp must keep (1 - 0.5) * 8 = 4
    // slots, not (1 - 0.5) * 40 = 20: the clamp protects the share-sized search the gate defines,
    // not the queue's capacity.
    int queueSize = 40;
    int gateK = 8;
    GlobalKnnFloor floor = new GlobalKnnFloor(queueSize);
    floor.advertise(1000f);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(
            new TopKnnCollector(queueSize, Integer.MAX_VALUE),
            floor,
            0.5f,
            1,
            FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL,
            gateK);

    for (int doc = 0; doc < gateK; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
    }

    // Scores are 1..8 and the clamp keeps the best 4 of them, {5, 6, 7, 8}: the bound must be the
    // clamp's minimum, 5. A clamp sized from the queue would hold all 8 scores and expose 1.
    assertEquals(
        "the clamp must keep (1 - greediness) * gateK slots",
        5f,
        collector.minCompetitiveSimilarity(),
        0.0f);
  }

  public void testConfigurableSlotMinimumControlsNeutralization() {
    // The neutralization property follows the configured minimum, not a baked-in number: with a
    // slot minimum of 4 at k=8, the clamp is narrower than the local queue and an advertised
    // floor must bind; with a slot minimum of 8 it must not.
    int k = 8;
    for (int slots : new int[] {4, 8}) {
      GlobalKnnFloor floor = new GlobalKnnFloor(k);
      floor.advertise(1000f);
      TopKnnCollector delegate = new TopKnnCollector(k, Integer.MAX_VALUE);
      FloorAwareKnnCollector collector =
          new FloorAwareKnnCollector(
              delegate, floor, 1f, slots, FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL);
      for (int doc = 0; doc < k; doc++) {
        collector.incVisitedCount(1);
        collector.collect(doc, doc + 1f);
      }
      if (slots < k) {
        assertTrue(
            "a clamp narrower than the local queue must let the floor bind",
            collector.minCompetitiveSimilarity() > delegate.minCompetitiveSimilarity());
      } else {
        assertEquals(
            "a clamp as wide as the local queue must neutralize the floor",
            delegate.minCompetitiveSimilarity(),
            collector.minCompetitiveSimilarity(),
            0.0f);
      }
    }
  }

  public void testAscentGateIgnoresFloorUntilLocalQueueFills() {
    int k = FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS + 4;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    // A sibling searcher has already converged and established a high floor. A correct collector
    // must not expose it before this searcher has escaped its own ascent, otherwise the graph
    // search would be terminated at its entry point before finding anything.
    floor.advertise(1000f);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(new TopKnnCollector(k, Integer.MAX_VALUE), floor);

    for (int doc = 0; doc < k - 1; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
      assertEquals(
          "the shared floor must be invisible while the local queue is filling",
          Float.NEGATIVE_INFINITY,
          collector.minCompetitiveSimilarity(),
          0.0f);
    }

    collector.incVisitedCount(1);
    collector.collect(k - 1, (float) k);
    assertTrue(
        "once the local queue is full, the shared floor must start binding",
        collector.minCompetitiveSimilarity() > Float.NEGATIVE_INFINITY);
    // With k exceeding the clamp's minimum size, the bound must exceed the local k-th best (1):
    // the floor is genuinely binding, not merely echoing the local queue.
    assertTrue(collector.minCompetitiveSimilarity() > 1f);
  }

  public void testGreedinessClampCapsTheSharedFloor() {
    // k and greediness sized so the fractional clamp exceeds the absolute minimum: 40 collected
    // scores with greediness 0.5 keep a non-competitive queue of 20 entries, so the effective
    // bound may never exceed the 20th best similarity this collector has seen.
    int k = 40;
    float greediness = 0.5f;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    floor.advertise(1000f);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(new TopKnnCollector(k, Integer.MAX_VALUE), floor, greediness);

    for (int doc = 0; doc < k; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
    }

    // Scores are 1..40: the local k-th best is 1, the 20th best seen is 21, and the floor is
    // 1000. The clamp must win.
    assertEquals(
        "the bound must be capped by the clamp queue's minimum, not jump to the shared floor",
        21f,
        collector.minCompetitiveSimilarity(),
        0.0f);
  }

  public void testSharedFloorBindsOneUlpBelowItsValue() {
    int localK = 20;
    // Size the floor for a larger result set so the local scores cannot define it: the only floor
    // source in this test is the advertised bound.
    GlobalKnnFloor floor = new GlobalKnnFloor(100);
    floor.advertise(2.5f);
    // greediness 1 collapses the clamp to its absolute minimum of DEFAULT_MIN_EXPLORATION_SLOTS
    // entries.
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(new TopKnnCollector(localK, Integer.MAX_VALUE), floor, 1f);

    // Three scores below the advertised floor, then 17 above it: the local k-th best (1) stays
    // below the floor while the clamp queue's minimum (the 16th best seen, 3.1) rises above it,
    // so min(clamp, nextDown(floor)) selects the floor term.
    collector.incVisitedCount(1);
    collector.collect(0, 1f);
    collector.incVisitedCount(1);
    collector.collect(1, 1.2f);
    collector.incVisitedCount(1);
    collector.collect(2, 1.4f);
    for (int doc = 3; doc < localK; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, 3f + 0.1f * (doc - 2));
    }

    // The bound must sit strictly below the floor: a hit scoring exactly at the floor may still
    // win the merged tie-break and must remain findable.
    assertEquals(Math.nextDown(2.5f), collector.minCompetitiveSimilarity(), 0.0f);
    assertTrue(collector.minCompetitiveSimilarity() < 2.5f);
  }

  public void testFloorIsNeutralizedAtSmallK() {
    // When k does not exceed the clamp's absolute minimum, the clamp queue is at least as large
    // as the local queue, its minimum can never exceed the local k-th best, and the shared floor
    // must have no effect at all: even a hostile advertised bound cannot change the bound stock
    // search would have used.
    int k = FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS / 2;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    floor.advertise(Float.MAX_VALUE);
    TopKnnCollector delegate = new TopKnnCollector(k, Integer.MAX_VALUE);
    FloorAwareKnnCollector collector = new FloorAwareKnnCollector(delegate, floor, 1f);

    for (int doc = 0; doc < 3 * k; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, random().nextFloat());
      assertEquals(
          "at k <= the clamp's slot minimum the bound must be exactly the delegate's",
          delegate.minCompetitiveSimilarity(),
          collector.minCompetitiveSimilarity(),
          0.0f);
    }
  }

  public void testCollectReportsSharedFloorUpdates() {
    // k equal to the clamp's absolute minimum makes both local structures the same size, so a
    // score rejected by the local queue is also rejected by the clamp queue and cannot report an
    // update through either.
    int k = FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(new TopKnnCollector(k, Integer.MAX_VALUE), floor, 0f);

    for (int doc = 0; doc < k; doc++) {
      collector.incVisitedCount(1);
      assertTrue("a locally accepted hit must report an update", collector.collect(doc, doc + 5f));
    }

    // Both queues hold 5..k+4. A worse score away from a synchronization boundary changes
    // nothing and must say so, otherwise the searcher would re-derive its bound for no reason.
    collector.incVisitedCount(1);
    assertFalse(
        "a rejected hit between synchronizations must not report an update",
        collector.collect(k, 1f));

    // Advance a full sync interval past the gate-open synchronization: even a rejected hit must
    // report an update there, because the re-read of the shared floor may have moved the
    // effective bound.
    collector.incVisitedCount(FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL);
    assertTrue(
        "a hit past the synchronization threshold must report an update after the floor re-read",
        collector.collect(k + 1, 1f));
  }

  public void testSyncHappensOnceIntervalElapsesRegardlessOfAlignment() {
    // The synchronization schedule must be a threshold over the visited count, not an exact
    // boundary match: collect() is invoked only for candidates that beat the current bound, so
    // visited counts arrive at irregular strides, and a boundary that falls between two collects
    // must trigger on the next collect rather than being skipped. Skipped boundaries starve the
    // floor exactly in the late phase of the search, when few candidates beat the bar.
    int k = 4;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(
            new TopKnnCollector(k, Integer.MAX_VALUE), floor, 0.5f, 1, 256, k);

    // Fill to the gate: the first batch publishes and defines the floor at the k-th best, 1.
    for (int doc = 0; doc < k; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
    }
    assertEquals(1f, floor.floor(), 0.0f);

    // A better score arrives, then the visited count jumps far past the next synchronization
    // threshold without ever landing on a multiple of the interval.
    collector.incVisitedCount(1);
    collector.collect(k, 100f);
    collector.incVisitedCount(300);
    assertTrue("test setup: land off any interval multiple", collector.visitedCount() % 256 != 0);
    collector.incVisitedCount(1);
    collector.collect(k + 1, 101f);

    assertTrue(
        "a collect past the synchronization threshold must publish the pending scores",
        floor.floor() > 1f);
  }

  public void testNonPublishingCollectorReadsButNeverFeedsTheFloor() {
    int k = 20;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    FloorAwareKnnCollector collector =
        new FloorAwareKnnCollector(
            new TopKnnCollector(k, Integer.MAX_VALUE), floor, 1f, 2, 256, k, false);
    assertFalse(collector.publishesToFloor());

    // Collect a full queue of scores: a publishing collector would have defined the floor here.
    for (int doc = 0; doc < k; doc++) {
      collector.incVisitedCount(1);
      collector.collect(doc, doc + 1f);
    }
    assertEquals(
        "a non-publishing collector must never feed the shared floor",
        Float.NEGATIVE_INFINITY,
        floor.floor(),
        0.0f);

    // The floor rises externally; the collector must still read it and prune against it. After
    // the final collect the clamp holds the two best scores seen ({20, 30}), so the bound is
    // max(local k-th best = 2, min(clamp = 20, nextDown(1000))) = 20 — above anything the local
    // queue alone would justify, provable only by a floor that was actually read.
    floor.advertise(1000f);
    collector.incVisitedCount(FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL);
    collector.collect(k, 30f);
    assertEquals(
        "a non-publishing collector must still prune against the shared floor",
        20f,
        collector.minCompetitiveSimilarity(),
        0.0f);
  }

  public void testDelegationOfCollectorPlumbing() {
    GlobalKnnFloor floor = new GlobalKnnFloor(3);
    TopKnnCollector delegate = new TopKnnCollector(3, 17);
    FloorAwareKnnCollector collector = new FloorAwareKnnCollector(delegate, floor);
    assertEquals(3, collector.k());
    assertEquals(17, collector.visitLimit());
    collector.incVisitedCount(5);
    assertEquals(5, collector.visitedCount());
    assertEquals(5, delegate.visitedCount());
    assertFalse(collector.earlyTerminated());
    collector.incVisitedCount(12);
    assertTrue(
        "the visit limit must keep terminating through the decorator", collector.earlyTerminated());
  }
}
