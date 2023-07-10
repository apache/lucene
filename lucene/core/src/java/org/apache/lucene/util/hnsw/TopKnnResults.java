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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.LongValues;

/**
 * TopKnnResults is a specific KnnResults, enforcing a minHeap is utilized for results.
 *
 * <p>This way as better results are found, the minimum result can be easily removed from the
 * collection
 *
 * <p>The size maximum size of the results is enforced by the provided `k`
 */
public class TopKnnResults extends KnnResults {
  public record Provider(int k) implements KnnResultsProvider {
    @Override
    public KnnResults getKnnResults(IntToIntFunction vectorToOrd) {
      return new TopKnnResults(k, vectorToOrd);
    }

  }

  private final int k;
  private final IntToIntFunction vectorToOrd;

  public TopKnnResults(int k, IntToIntFunction vectorToOrd) {
    super(k);
    this.k = k;
    this.vectorToOrd = vectorToOrd;
  }

  @Override
  public boolean isFull() {
    return size() >= k;
  }

  @Override
  public void popWhileFull() {
    while (size() > k) {
      pop();
    }
  }

  @Override
  protected void doClear() {}

  @Override
  public TopDocs topDocs() {
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(size(), k)];
    while (size() > 0) {
      int node = topNode();
      float score = topScore();
      pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(this.vectorToOrd.apply(node), score);
    }

    TotalHits.Relation relation =
            incomplete()
                    ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                    : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  @Override
  public String toString() {
    return "TopKnnResults[" + size() + "]";
  }
}
