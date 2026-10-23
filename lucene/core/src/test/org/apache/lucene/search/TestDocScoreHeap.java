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
package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestDocScoreHeap extends LuceneTestCase {

  /** Same ordering the heap encodes: most competitive first = score desc, then doc asc. */
  private static final Comparator<long[]> MOST_COMPETITIVE_FIRST =
      Comparator.<long[]>comparingDouble(h -> -Float.intBitsToFloat((int) h[1]))
          .thenComparingLong(h -> h[0]);

  /** The fast packed heap (doc ids below Integer.MAX_VALUE). */
  public void testPackedMatchesBruteForceTopK() {
    doTestMatchesBruteForceTopK(true);
  }

  /** The long-doc heap (doc ids spanning well beyond Integer.MAX_VALUE). */
  public void testLongMatchesBruteForceTopK() {
    doTestMatchesBruteForceTopK(false);
  }

  private void doTestMatchesBruteForceTopK(boolean docsFitInt) {
    // The packed heap requires doc ids below Integer.MAX_VALUE; the long heap spans past it.
    final long docBound = docsFitInt ? Integer.MAX_VALUE : 4L * Integer.MAX_VALUE;
    int iters = atLeast(50);
    for (int iter = 0; iter < iters; iter++) {
      int k = TestUtil.nextInt(random(), 1, 32);
      int numHits = TestUtil.nextInt(random(), 0, 200);
      // Small score/doc domains so score ties and doc ties are common.
      final int scoreDomain = TestUtil.nextInt(random(), 1, 6);

      DocScoreHeap heap = DocScoreHeap.create(k, docsFitInt);
      List<long[]> all = new ArrayList<>();
      for (int i = 0; i < numHits; i++) {
        float score = 1f + random().nextInt(scoreDomain); // positive, per collector contract
        long doc = (random().nextLong() >>> 1) % docBound;
        // encode as {doc, floatBits} so the reference comparator can decode
        all.add(new long[] {doc, Float.floatToIntBits(score)});

        // The collector only calls updateTop when the new hit beats the current least
        // competitive one; replicate that contract.
        if (score > heap.topScore() || (score == heap.topScore() && doc < heap.topDoc())) {
          heap.updateTop(doc, score);
        }
      }

      // Brute-force expected top-k.
      all.sort(MOST_COMPETITIVE_FIRST);
      int expectedCount = Math.min(k, numHits);
      List<String> expected = new ArrayList<>();
      for (int i = 0; i < expectedCount; i++) {
        long[] h = all.get(i);
        expected.add(h[0] + ":" + Float.intBitsToFloat((int) h[1]));
      }
      expected.sort(null);

      // Drain the heap least-competitive first; verify order is non-decreasing in competitiveness
      // and collect the real (non-sentinel) hits. Both implementations use a -Infinity sentinel
      // score, so a real hit is anything scoring above that.
      List<String> actual = new ArrayList<>();
      int size = heap.size();
      float prevScore = Float.NEGATIVE_INFINITY;
      long prevDoc = Long.MAX_VALUE;
      for (int i = 0; i < size; i++) {
        float s = heap.topScore();
        long d = heap.topDoc();
        // popping least-competitive first => score non-decreasing, and on score ties doc
        // non-increasing
        assertTrue("heap out of order", s > prevScore || (s == prevScore && d <= prevDoc));
        prevScore = s;
        prevDoc = d;
        if (s != Float.NEGATIVE_INFINITY) {
          actual.add(d + ":" + s);
        }
        heap.pop();
      }
      actual.sort(null);

      assertEquals(
          "docsFitInt=" + docsFitInt + " iter=" + iter + " k=" + k + " numHits=" + numHits,
          expected,
          actual);
    }
  }
}
