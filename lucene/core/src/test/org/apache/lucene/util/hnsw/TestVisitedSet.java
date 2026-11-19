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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestVisitedSet extends LuceneTestCase {

  public void testChoice() throws IOException {
    // the hash set if expectedVisitedNodes < graphSize / 128
    assertBitSet(VisitedSet.create(100, 101 * 128 - 1));
    assertHashSet(VisitedSet.create(100, 101 * 128));
    // an empty graph that a builder grows
    assertBitSet(VisitedSet.create(HnswGraphSearcher.expectedVisitedNodes(100, 0), 0));

    // k = 100 expects 1193 visits at these sizes
    assertBitSet(HnswGraphSearcher.createVisitedSet(100, 152_831));
    assertHashSet(HnswGraphSearcher.createVisitedSet(100, 152_832));

    RandomVectorScorerSupplier scorerSupplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.DOT_PRODUCT,
            FloatVectorValues.fromFloats(List.of(new float[] {1, 0}), 2));
    assertBitSet(
        HnswGraphBuilder.create(scorerSupplier, 16, 100, 42, 100_000).graphSearcher.visited);
    assertHashSet(
        HnswGraphBuilder.create(scorerSupplier, 16, 100, 42, 200_000).graphSearcher.visited);
  }

  private static void assertBitSet(VisitedSet visited) {
    assertNotEquals(Integer.MAX_VALUE, visited.length());
  }

  private static void assertHashSet(VisitedSet visited) {
    assertEquals(Integer.MAX_VALUE, visited.length());
  }

  public void testBehavior() {
    for (VisitedSet visited :
        List.of(VisitedSet.create(10, 10), VisitedSet.create(1, Integer.MAX_VALUE))) {
      assertFalse(visited.getAndSet(3));
      assertTrue(visited.getAndSet(3));
      visited.set(7);
      assertTrue(visited.getAndSet(7));
      visited.clear();
      assertFalse(visited.getAndSet(3));
      assertFalse(visited.getAndSet(7));
      visited.ensureCapacityAndClear(1000);
      assertTrue(visited.length() >= 1000);
      assertFalse(visited.getAndSet(3));
      assertFalse(visited.getAndSet(999));
    }
  }
}
