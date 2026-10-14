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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestFilteredHnswGraphSearcher extends LuceneTestCase {

  public void testExploresStrictlyCompetitiveBoundaryCandidate() throws IOException {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(2, 4);
    for (int node = 0; node < 4; node++) {
      graph.addNode(0, node);
    }
    assertTrue(graph.trySetNewEntryNode(0, 0));
    graph.getNeighbors(0, 0).addInOrder(1, 1f);

    float entryScore = 0.5f;
    float betterScore = Math.nextUp(entryScore);
    float[] scores = {entryScore, betterScore, 0.1f, 0.1f};
    RandomVectorScorer scorer =
        new RandomVectorScorer() {
          @Override
          public float score(int node) {
            return scores[node];
          }

          @Override
          public int maxOrd() {
            return scores.length;
          }
        };

    FixedBitSet acceptOrds = new FixedBitSet(4);
    acceptOrds.set(0);
    acceptOrds.set(1);
    KnnCollector collector =
        new TopKnnCollector(1, Integer.MAX_VALUE, KnnSearchStrategy.Hnsw.DEFAULT);

    FilteredHnswGraphSearcher.create(collector.k(), graph, 2, acceptOrds)
        .search(collector, scorer, graph, acceptOrds);

    TopDocs results = collector.topDocs();
    assertEquals(1, results.scoreDocs.length);
    assertEquals(1, results.scoreDocs[0].doc);
    assertEquals(betterScore, results.scoreDocs[0].score, 0f);
  }

  public void testAllTiedScoresBoundedVisitation() throws IOException {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(2, 6);
    for (int node = 0; node < 6; node++) {
      graph.addNode(0, node);
    }
    assertTrue(graph.trySetNewEntryNode(0, 0));
    for (int node = 0; node < 5; node++) {
      graph.getNeighbors(0, node).addInOrder(node + 1, 1f);
    }

    RandomVectorScorer scorer =
        new RandomVectorScorer() {
          @Override
          public float score(int node) {
            return 0.5f;
          }

          @Override
          public int maxOrd() {
            return 6;
          }
        };

    FixedBitSet acceptOrds = new FixedBitSet(6);
    for (int node = 0; node < 4; node++) {
      acceptOrds.set(node);
    }
    KnnCollector collector =
        new TopKnnCollector(1, Integer.MAX_VALUE, KnnSearchStrategy.Hnsw.DEFAULT);

    FilteredHnswGraphSearcher.create(collector.k(), graph, 4, acceptOrds)
        .search(collector, scorer, graph, acceptOrds);

    // One candidate tied with the current minimum may be explored, then the search must stop
    // instead of walking the rest of the equal-scored chain.
    assertEquals(2, collector.visitedCount());
    TopDocs results = collector.topDocs();
    assertEquals(1, results.scoreDocs.length);
    assertEquals(0, results.scoreDocs[0].doc);
    assertEquals(0.5f, results.scoreDocs[0].score, 0f);
  }
}
