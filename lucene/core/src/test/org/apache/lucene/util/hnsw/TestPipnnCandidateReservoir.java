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
package org.apache.lucene.util.hnsw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestPipnnCandidateReservoir extends LuceneTestCase {

  public void testPackRoundTrip() {
    int node = TestUtil.nextInt(random(), 0, Integer.MAX_VALUE);
    int hash = random().nextInt(1 << 16);
    float score = random().nextFloat();
    long packed = PipnnCandidateReservoir.pack(node, hash, score);
    assertEquals(node, PipnnCandidateReservoir.nodeOf(packed));
    assertEquals(hash, PipnnCandidateReservoir.hashOf(packed));
    // the score survives as a bfloat16: same sign and exponent, 7 bits of mantissa
    float decoded = Float.intBitsToFloat(PipnnCandidateReservoir.scoreBitsOf(packed) << 16);
    assertEquals(score, decoded, Math.ulp(score) * (1 << 16));
  }

  public void testCloserOrdersByScoreThenNode() {
    assertTrue(
        PipnnCandidateReservoir.closer(
            PipnnCandidateReservoir.pack(5, 0, 0.9f), PipnnCandidateReservoir.pack(1, 0, 0.5f)));
    assertFalse(
        PipnnCandidateReservoir.closer(
            PipnnCandidateReservoir.pack(5, 0, 0.5f), PipnnCandidateReservoir.pack(1, 0, 0.9f)));
    // equal scores: lower node id wins
    assertTrue(
        PipnnCandidateReservoir.closer(
            PipnnCandidateReservoir.pack(1, 3, 0.5f), PipnnCandidateReservoir.pack(5, 7, 0.5f)));
    assertFalse(
        PipnnCandidateReservoir.closer(
            PipnnCandidateReservoir.pack(5, 3, 0.5f), PipnnCandidateReservoir.pack(1, 7, 0.5f)));
    // a slot is never closer than itself
    long slot = PipnnCandidateReservoir.pack(5, 3, 0.5f);
    assertFalse(PipnnCandidateReservoir.closer(slot, slot));
  }

  public void testInvalidCapacity() {
    expectThrows(IllegalArgumentException.class, () -> new PipnnCandidateReservoir(1, 0, true));
    expectThrows(IllegalArgumentException.class, () -> new PipnnCandidateReservoir(1, 128, true));
  }

  public void testSameBucketKeepsCloser() {
    PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(1, 4, true);
    assertTrue(reservoir.offer(0, 10, 7, 0.5f));
    assertEquals(1, reservoir.size(0));
    // farther candidate with the same hash is dropped
    assertFalse(reservoir.offer(0, 11, 7, 0.4f));
    assertEquals(1, reservoir.size(0));
    assertArrayEquals(new int[] {10}, candidates(reservoir, 0));
    // closer candidate with the same hash replaces it
    assertTrue(reservoir.offer(0, 12, 7, 0.6f));
    assertEquals(1, reservoir.size(0));
    assertArrayEquals(new int[] {12}, candidates(reservoir, 0));
    // a different hash gets its own slot
    assertTrue(reservoir.offer(0, 13, 8, 0.1f));
    assertEquals(2, reservoir.size(0));
  }

  public void testFullReservoirEvictsFarthest() {
    PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(1, 2, true);
    assertTrue(reservoir.offer(0, 1, 1, 0.3f));
    assertTrue(reservoir.offer(0, 2, 2, 0.6f));
    // not closer than the farthest stored candidate
    assertFalse(reservoir.offer(0, 3, 3, 0.2f));
    assertArrayEquals(new int[] {1, 2}, candidates(reservoir, 0));
    // closer: evicts node 1
    assertTrue(reservoir.offer(0, 4, 4, 0.5f));
    assertArrayEquals(new int[] {4, 2}, candidates(reservoir, 0));
  }

  public void testOfferingSameCandidateTwiceIsIdempotent() {
    PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(1, 4, true);
    assertTrue(reservoir.offer(0, 1, 1, 0.3f));
    assertFalse(reservoir.offer(0, 1, 1, 0.3f));
    assertEquals(1, reservoir.size(0));
  }

  public void testUnhashedKeepsClosestDistinctCandidates() {
    PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(1, 3, false);
    // identical hashes do not merge candidates when hashing is off
    assertTrue(reservoir.offer(0, 1, 0, 0.1f));
    assertTrue(reservoir.offer(0, 2, 0, 0.2f));
    assertTrue(reservoir.offer(0, 3, 0, 0.3f));
    assertEquals(3, reservoir.size(0));
    // but the same candidate id does
    assertFalse(reservoir.offer(0, 2, 5, 0.2f));
    assertEquals(3, reservoir.size(0));
    assertTrue(reservoir.offer(0, 4, 0, 0.4f));
    assertArrayEquals(new int[] {4, 2, 3}, candidates(reservoir, 0));
  }

  public void testNodesAreIndependent() {
    PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(3, 2, true);
    reservoir.offer(0, 7, 1, 0.1f);
    reservoir.offer(2, 8, 1, 0.9f);
    reservoir.offer(2, 9, 2, 0.8f);
    assertEquals(1, reservoir.size(0));
    assertEquals(0, reservoir.size(1));
    assertEquals(2, reservoir.size(2));
    assertArrayEquals(new int[] {7}, candidates(reservoir, 0));
    assertArrayEquals(new int[] {8, 9}, candidates(reservoir, 2));
  }

  /**
   * The surviving set must not depend on the order in which candidates arrive: it is always the
   * {@code capacity} closest among the per-bucket winners.
   */
  public void testHistoryIndependence() {
    int capacity = TestUtil.nextInt(random(), 1, 16);
    int numCandidates = TestUtil.nextInt(random(), 1, 200);
    int hashBits = TestUtil.nextInt(random(), 1, 6);
    List<long[]> offers = new ArrayList<>();
    for (int i = 0; i < numCandidates; i++) {
      // distinct candidate ids; scores drawn from a small set to produce ties
      float score = TestUtil.nextInt(random(), 0, 20) / 20f;
      offers.add(new long[] {i, random().nextInt(1 << hashBits), Float.floatToIntBits(score)});
    }
    int[] expected = expectedSurvivors(offers, capacity);

    for (int iter = 0; iter < 10; iter++) {
      Collections.shuffle(offers, random());
      PipnnCandidateReservoir reservoir = new PipnnCandidateReservoir(1, capacity, true);
      for (long[] offer : offers) {
        reservoir.offer(0, (int) offer[0], (int) offer[1], Float.intBitsToFloat((int) offer[2]));
      }
      int[] actual = candidates(reservoir, 0);
      Arrays.sort(actual);
      assertArrayEquals("iteration " + iter, expected, actual);
    }
  }

  public void testRamBytesUsedGrowsWithSize() {
    long small = new PipnnCandidateReservoir(10, 4, true).ramBytesUsed();
    long large = new PipnnCandidateReservoir(1000, 4, true).ramBytesUsed();
    assertTrue(large > small);
    assertTrue(large >= 1000L * 4 * Long.BYTES);
  }

  private static int[] expectedSurvivors(List<long[]> offers, int capacity) {
    // bucket winners: the closest candidate per hash, ties by lower node id
    Map<Integer, long[]> winners = new HashMap<>();
    for (long[] offer : offers) {
      int hash = (int) offer[1];
      long[] current = winners.get(hash);
      if (current == null || closer(offer, current)) {
        winners.put(hash, offer);
      }
    }
    List<long[]> sorted = new ArrayList<>(winners.values());
    sorted.sort((a, b) -> closer(a, b) ? -1 : (closer(b, a) ? 1 : 0));
    int[] survivors = new int[Math.min(capacity, sorted.size())];
    for (int i = 0; i < survivors.length; i++) {
      survivors[i] = (int) sorted.get(i)[0];
    }
    Arrays.sort(survivors);
    return survivors;
  }

  private static boolean closer(long[] a, long[] b) {
    return PipnnCandidateReservoir.closer(pack(a), pack(b));
  }

  private static long pack(long[] offer) {
    return PipnnCandidateReservoir.pack(
        (int) offer[0], (int) offer[1], Float.intBitsToFloat((int) offer[2]));
  }

  private static int[] candidates(PipnnCandidateReservoir reservoir, int node) {
    int[] dest = new int[reservoir.capacity()];
    int count = reservoir.candidates(node, dest);
    return Arrays.copyOf(dest, count);
  }
}
