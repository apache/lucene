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

import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestMaxScoreAccumulator extends LuceneTestCase {

  public void testUnset() {
    MaxScoreAccumulator acc = new MaxScoreAccumulator();
    assertNull(acc.get());
  }

  public void testSimple() {
    // The accumulator keeps the pair with the highest score, tie-breaking on the lowest (global)
    // doc id. Doc ids may exceed Integer.MAX_VALUE.
    MaxScoreAccumulator acc = new MaxScoreAccumulator();
    acc.accumulate(0, 0f);
    assertEquals(new DocAndScore(0, 0f), acc.get());
    // higher doc, same score: keeps the lower doc
    acc.accumulate(10, 0f);
    assertEquals(new DocAndScore(0, 0f), acc.get());
    // higher score wins regardless of doc
    acc.accumulate(100, 1000f);
    assertEquals(new DocAndScore(100, 1000f), acc.get());
    // lower score does not win
    acc.accumulate(1000, 5f);
    assertEquals(new DocAndScore(100, 1000f), acc.get());
    // same score, lower doc wins
    acc.accumulate(99, 1000f);
    assertEquals(new DocAndScore(99, 1000f), acc.get());
    acc.accumulate(1000, 1001f);
    assertEquals(new DocAndScore(1000, 1001f), acc.get());
    acc.accumulate(10, 1001f);
    assertEquals(new DocAndScore(10, 1001f), acc.get());
    acc.accumulate(100, 1001f);
    assertEquals(new DocAndScore(10, 1001f), acc.get());
    // a global doc id beyond Integer.MAX_VALUE is preserved
    long bigDoc = Integer.MAX_VALUE + 1234L;
    acc.accumulate(bigDoc, 2000f);
    assertEquals(new DocAndScore(bigDoc, 2000f), acc.get());
  }

  public void testRandom() {
    MaxScoreAccumulator acc = new MaxScoreAccumulator();
    DocAndScore expected = null;
    int iters = atLeast(100);
    for (int i = 0; i < iters; i++) {
      float score = random().nextFloat() * 1000f;
      long doc = random().nextLong() & Long.MAX_VALUE;
      acc.accumulate(doc, score);
      DocAndScore candidate = new DocAndScore(doc, score);
      if (candidate.isMoreCompetitiveThan(expected)) {
        expected = candidate;
      }
      assertEquals(expected, acc.get());
    }
  }
}
