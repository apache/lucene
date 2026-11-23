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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestHnswGraphSearcher extends LuceneTestCase {

  public void testEntryPointSearchRespectsVisitLimitWithinBatch() throws IOException {
    // Entry node 0 links to nodes 1..8 on level 1, which land in a single scoring batch
    int numNodes = 10;
    OnHeapHnswGraph graph = new OnHeapHnswGraph(8, numNodes);
    for (int node = 0; node < numNodes; node++) {
      graph.addNode(0, node);
    }
    for (int node = 0; node <= 8; node++) {
      graph.addNode(1, node);
    }
    assertTrue(graph.trySetNewEntryNode(0, 1));
    for (int node = 1; node <= 8; node++) {
      graph.getNeighbors(1, 0).addInOrder(node, 1f);
    }

    int[] scored = new int[1];
    RandomVectorScorer scorer =
        new RandomVectorScorer() {
          @Override
          public float score(int node) {
            scored[0]++;
            return node / (float) numNodes;
          }

          @Override
          public int maxOrd() {
            return numNodes;
          }
        };

    int visitLimit = 4;
    KnnCollector collector = new TopKnnCollector(10, visitLimit, KnnSearchStrategy.Hnsw.DEFAULT);

    HnswGraphSearcher.search(scorer, collector, graph, null);

    assertEquals(visitLimit, collector.visitedCount());
    assertEquals(visitLimit, scored[0]);
    assertTrue(collector.earlyTerminated());
  }
}
