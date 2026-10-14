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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

/** The tracking visited set behaves like a {@link FixedBitSet} through sets, clears and growth. */
public class TestTrackingVisitedBitSet extends LuceneTestCase {

  public void testSameAsFixedBitSet() {
    int numBits = 1 + random().nextInt(TEST_NIGHTLY ? 200_000 : 20_000);
    // track from the start, or only once the set grows past a random size, or never
    int minTrackedBits =
        switch (random().nextInt(3)) {
          case 0 -> 0;
          case 1 -> random().nextInt(2 * numBits + 1);
          default -> Integer.MAX_VALUE;
        };
    TrackingVisitedBitSet tracking = new TrackingVisitedBitSet(numBits, minTrackedBits);
    FixedBitSet reference = new FixedBitSet(numBits);
    int rounds = atLeast(20);
    for (int round = 0; round < rounds; round++) {
      if (random().nextInt(10) == 0) {
        // grow, as a builder does when its graph grows
        int capacity = numBits + random().nextInt(numBits / 4 + 1);
        tracking.ensureCapacityAndClear(capacity);
        reference = FixedBitSet.ensureCapacityAndClear(reference, capacity);
        numBits = reference.length();
        assertEquals(numBits, tracking.length());
      } else if (random().nextBoolean()) {
        tracking.ensureCapacityAndClear(numBits);
        reference.clear();
      } else {
        tracking.clear();
        reference.clear();
      }
      assertEquals(0, tracking.cardinality());
      // few bits (cleared one by one) or many (more than the limit of 256: cleared by words)
      int sets =
          random().nextBoolean()
              ? random().nextInt(100)
              : random().nextInt(Math.min(numBits, 4096) + 1);
      for (int s = 0; s < sets; s++) {
        int i = random().nextInt(numBits);
        if (random().nextBoolean()) {
          assertEquals(reference.getAndSet(i), tracking.getAndSet(i));
        } else {
          reference.set(i);
          tracking.set(i);
        }
        if (random().nextInt(20) == 0) {
          int j = random().nextInt(numBits);
          reference.clear(j);
          tracking.clear(j);
        }
      }
      assertEquals(reference.cardinality(), tracking.cardinality());
      for (int i = 0; i < numBits; i++) {
        assertEquals(reference.get(i), tracking.get(i));
      }
    }
  }

  // below 4096 words the set records up to 256 bits; one more and clear() clears every word
  public void testClearAtTheLimit() {
    int numBits = 300 + random().nextInt(20_000);
    TrackingVisitedBitSet tracking = new TrackingVisitedBitSet(numBits, 0);
    int rounds = atLeast(10);
    for (int round = 0; round < rounds; round++) {
      int distinct = 255 + random().nextInt(3);
      int set = 0;
      while (set < distinct) {
        if (tracking.getAndSet(random().nextInt(numBits)) == false) {
          set++;
        }
      }
      assertEquals(distinct, tracking.cardinality());
      if (random().nextBoolean()) {
        tracking.clear();
      } else {
        tracking.ensureCapacityAndClear(numBits);
      }
      assertEquals(0, tracking.cardinality());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, tracking.nextSetBit(0));
    }
  }
}
