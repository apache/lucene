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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Collector Manager for AnytimeRankingCollector
 */
public class AnytimeRankingCollectorManager
    implements CollectorManager<AnytimeRankingCollector, TopDocs> {
  private final int topK;
  private final long slaThresholdMs;

  public AnytimeRankingCollectorManager(int topK, long slaThresholdMs) {
    this.topK = topK;
    this.slaThresholdMs = slaThresholdMs;
  }

  @Override
  public AnytimeRankingCollector newCollector() {
    return new AnytimeRankingCollector(topK, slaThresholdMs);
  }

  @Override
  public TopDocs reduce(Collection<AnytimeRankingCollector> collectors) throws IOException {
    PriorityQueue<ScoreDoc> sortedDocs =
        new PriorityQueue<>(topK, Comparator.comparingDouble(sd -> sd.score));
    long totalHits = 0;

    for (AnytimeRankingCollector collector : collectors) {
      TopDocs topDocs = collector.topDocs();
      totalHits += topDocs.totalHits.value();

      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        if (sortedDocs.size() < topK) {
          sortedDocs.offer(scoreDoc);
        } else if (scoreDoc.score > sortedDocs.peek().score) {
          sortedDocs.poll();
          sortedDocs.offer(scoreDoc);
        }
      }
    }

    // Cannot use TopDocs.merge() since it assumes each shard has returned atleast K results
    // which is not necessarily true for early termination
    ScoreDoc[] results = sortedDocs.toArray(new ScoreDoc[0]);
    Arrays.sort(results, (a, b) -> Float.compare(b.score, a.score)); // Descending order

    return new TopDocs(
        new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), results);
  }
}
