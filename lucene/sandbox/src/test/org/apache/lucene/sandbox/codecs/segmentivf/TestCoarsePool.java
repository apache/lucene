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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * {@link CoarsePool} against a brute-force stable sort: whatever it admits and however its
 * threshold tightens, {@link CoarsePool#selectNearest} returns the nearest admitted slots, ties in
 * admission order.
 */
public class TestCoarsePool extends LuceneTestCase {

  public void testSelectsTheNearestAgainstASort() throws IOException {
    for (int iter = 0; iter < 200; iter++) {
      int bins = 2 + random().nextInt(64);
      int capacity = 1 + random().nextInt(100);
      int slots = random().nextInt(1000);
      // Few bins and many slots make heavy ties at the threshold.
      int[] distances = new int[slots];
      for (int i = 0; i < slots; i++) {
        distances[i] = random().nextInt(bins);
      }
      boolean[] accepted = new boolean[slots];
      for (int i = 0; i < slots; i++) {
        accepted[i] = random().nextInt(4) != 0;
      }
      SearchScratch scratch = new SearchScratch();
      CoarsePool pool = new CoarsePool(scratch, bins, capacity);
      if (random().nextBoolean()) {
        // A pool reused after clear() behaves as a fresh one.
        pool.reserve(slots);
        for (int i = 0; i < slots; i++) {
          pool.admit(distances[i], i);
        }
        pool.clear();
      }
      boolean blocks = random().nextBoolean();
      pool.reserve(slots);
      if (blocks) {
        for (int from = 0; from < slots; from += SearchScratch.ADMIT_BLOCK) {
          int count = Math.min(SearchScratch.ADMIT_BLOCK, slots - from);
          pool.admitBlock(distances, from, count, 0, slot -> accepted[slot]);
        }
      } else {
        for (int i = 0; i < slots; i++) {
          if (accepted[i]) {
            pool.admit(distances[i], i);
          }
        }
      }
      Integer[] expected =
          java.util.stream.IntStream.range(0, slots)
              .filter(i -> accepted[i])
              .boxed()
              .sorted(Comparator.comparingInt(i -> distances[i]))
              .toArray(Integer[]::new);
      for (int need : new int[] {0, 1, capacity / 2, capacity, capacity + 5}) {
        int want = Math.min(need, capacity);
        int selected = pool.selectNearest(want);
        int expectedCount = Math.min(want, expected.length);
        assertEquals("need=" + want, expectedCount, selected);
        int[] got = new int[selected];
        for (int i = 0; i < selected; i++) {
          got[i] = (int) scratch.selected[i];
          int distance = (int) (scratch.selected[i] >>> 32);
          assertEquals(distances[got[i]], distance);
        }
        int[] want0 =
            Arrays.stream(expected, 0, expectedCount).mapToInt(Integer::intValue).toArray();
        assertArrayEquals("bins=" + bins + " capacity=" + capacity + " need=" + want, want0, got);
      }
    }
  }
}
