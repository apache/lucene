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
import org.apache.lucene.internal.hppc.IntFloatHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;

/**
 * A {@link HnswGraphSearcher} that uses a set of seed ordinals to initiate the search.
 *
 * @lucene.experimental
 */
final class SeededHnswGraphSearcher extends AbstractHnswGraphSearcher {

  private final AbstractHnswGraphSearcher delegate;
  private final int[] seedOrds;
  private final IntFloatHashMap scoreMap;

  static SeededHnswGraphSearcher fromEntryPoints(
      AbstractHnswGraphSearcher delegate, KnnSearchStrategy.Seeded seeded, int graphSize)
      throws IOException {
    int numEps = seeded.numberOfEntryPoints();
    DocIdSetIterator eps = seeded.entryPoints();
    if (numEps <= 0) {
      throw new IllegalArgumentException("The number of entry points must be > 0");
    }
    int[] entryPoints = new int[numEps];
    int idx = 0;
    while (idx < entryPoints.length) {
      int entryPointOrdInt = eps.nextDoc();
      if (entryPointOrdInt == NO_MORE_DOCS) {
        throw new IllegalArgumentException(
            "The number of entry points provided is less than the number of entry points requested");
      }
      assert entryPointOrdInt < graphSize;
      entryPoints[idx++] = entryPointOrdInt;
    }
    return new SeededHnswGraphSearcher(delegate, entryPoints);
  }

  SeededHnswGraphSearcher(AbstractHnswGraphSearcher delegate, int[] seedOrds) {
    this(delegate, seedOrds, null);
  }

  SeededHnswGraphSearcher(AbstractHnswGraphSearcher delegate, int[] seedOrds, float[] scores) {
    this.delegate = delegate;
    this.seedOrds = seedOrds;
    if (scores != null) {
      assert seedOrds.length == scores.length;
      scoreMap = new IntFloatHashMap();
      for (int i = 0; i < seedOrds.length; i++) {
        scoreMap.put(seedOrds[i], scores[i]);
      }
    } else {
      scoreMap = null;
    }
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
    if (scoreMap != null) {
      delegate.searchLevel(
          results, new SeededRandomVectorScorer(scorer), level, eps, graph, acceptOrds);
    }
    delegate.searchLevel(results, scorer, level, eps, graph, acceptOrds);
  }

  @Override
  int[] findBestEntryPoint(RandomVectorScorer scorer, HnswGraph graph, KnnCollector collector) {
    return seedOrds;
  }

  private class SeededRandomVectorScorer implements RandomVectorScorer {

    private final RandomVectorScorer delegate;

    SeededRandomVectorScorer(RandomVectorScorer delegate) {
      assert scoreMap != null;
      this.delegate = delegate;
    }

    @Override
    public float score(int node) throws IOException {
      int index = scoreMap.indexOf(node);
      if (index >= 0) {
        return scoreMap.indexGet(index);
      }
      return delegate.score(node);
    }

    @Override
    public int maxOrd() {
      return delegate.maxOrd();
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return delegate.getAcceptOrds(acceptDocs);
    }
  }
}
