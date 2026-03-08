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

import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDistinctDocKnnCollector extends LuceneTestCase {

  public void testShortCircuit() throws IOException {
    int k = 1;
    TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE);

    // Setup mapping: ord 0 and 1 belong to Doc 100
    // ord 2 belongs to Doc 200
    MockKnnVectorValues vectorValues = new MockKnnVectorValues(new int[] {100, 100, 200});
    DistinctDocKnnCollector distinctCollector =
        new DistinctDocKnnCollector(collector, vectorValues);

    // 1. Initial state: both should be scored
    assertTrue(distinctCollector.shouldExploreNeighbors(0));
    assertTrue(distinctCollector.shouldExploreNeighbors(1));

    // 2. Collect ord 0 with high similarity
    distinctCollector.collect(0, 0.9f);
    assertEquals(0.9f, distinctCollector.minCompetitiveSimilarity(), 0.001f);

    // 3. ord 1 (Doc 100) should now be short-circuited
    assertFalse(
        "ord 1 should be short-circuited as Doc 100 is satisfied",
        distinctCollector.shouldExploreNeighbors(1));

    // 4. ord 2 (Doc 200) should still be scored
    assertTrue(distinctCollector.shouldExploreNeighbors(2));
  }

  private static class MockKnnVectorValues extends KnnVectorValues {
    private final int[] ordToDoc;

    MockKnnVectorValues(int[] ordToDoc) {
      this.ordToDoc = ordToDoc;
    }

    @Override
    public int ordToDoc(int ord) {
      return ordToDoc[ord];
    }

    @Override
    public int dimension() {
      return 0;
    }

    @Override
    public int size() {
      return ordToDoc.length;
    }

    @Override
    public KnnVectorValues copy() {
      return this;
    }

    @Override
    public VectorEncoding getEncoding() {
      return VectorEncoding.FLOAT32;
    }
  }
}
