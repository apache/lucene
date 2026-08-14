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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;

/**
 * A filter-aware HNSW graph searcher that operates in two phases without modifying the graph.
 * Before the candidate frontier first reaches {@code k}, accepted second-order neighbors are added
 * as expansion-zone candidates. In the second phase, when an expanded candidate is rejected by the
 * filter, rejected direct neighbors are skipped without scoring.
 *
 * @lucene.experimental
 */
public final class PathSeerHnswGraphSearcher extends HnswGraphSearcher {
  private int[] directNeighbors = new int[0];
  private int[] toScore = new int[0];

  private PathSeerHnswGraphSearcher(
      NeighborQueue candidates, org.apache.lucene.util.BitSet visited) {
    super(candidates, visited);
  }

  /** Creates a PathSeer searcher for the supplied graph. */
  public static PathSeerHnswGraphSearcher create(int k, HnswGraph graph) {
    return new PathSeerHnswGraphSearcher(
        new NeighborQueue(k, true), HnswGraphSearcher.createBitSet(k, getGraphSize(graph)));
  }

  @Override
  void searchLevel(
      KnnCollector results,
      RandomVectorScorer scorer,
      int level,
      int[] eps,
      HnswGraph graph,
      Bits acceptOrds)
      throws IOException {
    assert level == 0 : "PathSeer only works on the base level";
    if (acceptOrds == null) {
      super.searchLevel(results, scorer, level, eps, graph, null);
      return;
    }

    candidates.clear();
    visited.clear();
    if (bulkScores == null || bulkScores.length < eps.length) {
      bulkScores = new float[eps.length];
    }
    if (results.earlyTerminated()) {
      return;
    }
    scoreEntryPoints(results, scorer, visited, eps, acceptOrds, candidates, bulkScores);
    if (results.earlyTerminated()) {
      return;
    }

    float minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
    boolean shouldExploreMinSim = true;
    boolean candidateFrontierReachedK = false;
    final int maxSecondOrder = Math.max(1, graph.maxConn() * 2);

    while (candidates.size() > 0 && results.earlyTerminated() == false) {
      candidateFrontierReachedK |= candidates.size() >= results.k();
      float topCandidateSimilarity = candidates.topScore();
      if (topCandidateSimilarity < minAcceptedSimilarity) {
        if (shouldExploreMinSim && Math.nextUp(topCandidateSimilarity) == minAcceptedSimilarity) {
          shouldExploreMinSim = false;
        } else {
          break;
        }
      }

      int topCandidateNode = candidates.pop();
      boolean shouldFilterRejectedNeighbors =
          candidateFrontierReachedK && acceptOrds.get(topCandidateNode) == false;
      graphSeek(graph, level, topCandidateNode);
      int directCount = 0;
      int neighbor;
      while ((neighbor = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
        directNeighbors = ArrayUtil.grow(directNeighbors, directCount + 1);
        directNeighbors[directCount++] = neighbor;
      }

      int scoreCount = 0;
      for (int i = 0; i < directCount; i++) {
        int direct = directNeighbors[i];
        if (visited.getAndSet(direct)) {
          continue;
        }
        if (shouldFilterRejectedNeighbors && acceptOrds.get(direct) == false) {
          continue;
        }
        toScore = ArrayUtil.grow(toScore, scoreCount + 1);
        toScore[scoreCount++] = direct;
      }

      if (candidateFrontierReachedK == false) {
        int secondOrderExamined = 0;
        outer:
        for (int i = 0; i < directCount; i++) {
          graphSeek(graph, level, directNeighbors[i]);
          int secondOrder;
          while ((secondOrder = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
            if (secondOrderExamined++ >= maxSecondOrder) {
              break outer;
            }
            if (visited.get(secondOrder) || acceptOrds.get(secondOrder) == false) {
              continue;
            }
            visited.set(secondOrder);
            toScore = ArrayUtil.grow(toScore, scoreCount + 1);
            toScore[scoreCount++] = secondOrder;
          }
        }
      }

      scoreCount = (int) Math.min(scoreCount, results.visitLimit() - results.visitedCount());
      if (bulkScores.length < scoreCount) {
        bulkScores = new float[scoreCount];
      }
      results.incVisitedCount(scoreCount);
      if (scoreCount > 0
          && scorer.bulkScore(toScore, bulkScores, scoreCount)
              > results.minCompetitiveSimilarity()) {
        for (int i = 0; i < scoreCount; i++) {
          float similarity = bulkScores[i];
          if (similarity >= minAcceptedSimilarity) {
            int ord = toScore[i];
            candidates.add(ord, similarity);
            if (acceptOrds.get(ord) && results.collect(ord, similarity)) {
              float oldMinAcceptedSimilarity = minAcceptedSimilarity;
              minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
              if (minAcceptedSimilarity > oldMinAcceptedSimilarity) {
                shouldExploreMinSim = true;
              }
            }
          }
        }
      }
      if (results.getSearchStrategy() != null) {
        results.getSearchStrategy().nextVectorsBlock();
      }
    }
  }
}
