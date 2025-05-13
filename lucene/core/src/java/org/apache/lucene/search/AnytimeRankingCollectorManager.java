/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Collection;
import java.util.PriorityQueue;
import org.apache.lucene.index.IndexReader;

/** CollectorManager that provides bin-aware SLA-constrained collectors. */
public final class AnytimeRankingCollectorManager
    implements CollectorManager<AnytimeRankingCollector, TopDocs> {

  private final int topK;
  private final long slaCutoffNanos;
  private final float[] binBoosts;

  public AnytimeRankingCollectorManager(int topK, long slaCutoffNanos, IndexReader reader)
      throws IOException {
    this.topK = topK;
    this.slaCutoffNanos = slaCutoffNanos;
    this.binBoosts = BinBoostCalculator.compute(reader);
  }

  @Override
  public AnytimeRankingCollector newCollector() {
    return new AnytimeRankingCollector(topK, slaCutoffNanos, binBoosts);
  }

  @Override
  public TopDocs reduce(Collection<AnytimeRankingCollector> collectors) throws IOException {
    PriorityQueue<ScoreDoc> queue =
        new PriorityQueue<>(topK, (a, b) -> Float.compare(a.score, b.score));
    long totalHits = 0;

    for (AnytimeRankingCollector collector : collectors) {
      TopDocs shard = collector.topDocs();
      totalHits += shard.totalHits.value();
      for (ScoreDoc sd : shard.scoreDocs) {
        if (queue.size() < topK) {
          queue.offer(sd);
        } else if (sd.score > queue.peek().score) {
          queue.poll();
          queue.offer(sd);
        }
      }
    }

    ScoreDoc[] results = queue.toArray(new ScoreDoc[0]);
    java.util.Arrays.sort(results, (a, b) -> Float.compare(b.score, a.score));
    return new TopDocs(
        new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), results);
  }
}
