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

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

public class AnytimeRankingCollector extends TopDocsCollector<ScoreDoc> {
    private static final int MIN_SLA_CHECK_INTERVAL = 32;
    private static final int MAX_SLA_CHECK_INTERVAL = 512;
    private static final int DOCS_PER_SLA_CHECK = 100;

    private final long slaThresholdNanos;
    private final long startTime;
    private int docCounter = 0;

    protected AnytimeRankingCollector(int topK, long slaThresholdMs) {
        super(new HitQueue(topK, false));
        this.slaThresholdNanos = slaThresholdMs * 1_000_000L;
        this.startTime = System.nanoTime();
    }

    @Override
    protected int topDocsSize() {
        return pq.size();
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
        return results == null
                ? new TopDocs(new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0])
                : new TopDocs(new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), results);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final int docBase = context.docBase;
        int estimatedInterval = context.reader().numDocs() / DOCS_PER_SLA_CHECK;
        int slaCheckInterval = Math.max(MIN_SLA_CHECK_INTERVAL,
                Math.min(MAX_SLA_CHECK_INTERVAL, estimatedInterval));

        return new LeafCollector() {
            private Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                float score = scorer.score();

                if (Float.isNaN(score) || score <= 0) {
                    return;
                }

                int globalDoc = doc + docBase;
                ScoreDoc scoreDoc = new ScoreDoc(globalDoc, score);

                if (pq.insertWithOverflow(scoreDoc) != scoreDoc) {
                    totalHits++;
                }

                if ((docCounter++ % slaCheckInterval) == 0) {
                    long elapsed = System.nanoTime() - startTime;
                    if (elapsed >= slaThresholdNanos) {
                        throw new CollectionTerminatedException();
                    }
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }
}