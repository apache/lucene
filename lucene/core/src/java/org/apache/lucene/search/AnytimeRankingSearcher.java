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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.util.HeuristicSLAEstimator;
import org.apache.lucene.util.SLAEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * AnytimeRankingSearcher runs SLA-aware queries with adaptive scoring and soft range boosting.
 */
public class AnytimeRankingSearcher {
    private final IndexSearcher searcher;
    private final int topK;
    private final long slaThresholdMs;
    private final SLAEstimator slaEstimator;
    private final String rangeField;

    public AnytimeRankingSearcher(IndexSearcher searcher, int topK, long slaThresholdMs, String contentField) {
        this(searcher, topK, slaThresholdMs, contentField, "docID");
    }

    public AnytimeRankingSearcher(IndexSearcher searcher, int topK, long slaThresholdMs,
                                  String contentField, String rangeField) {
        this.searcher = searcher;
        this.topK = topK;
        this.slaThresholdMs = slaThresholdMs;
        this.slaEstimator = new HeuristicSLAEstimator(contentField);
        this.rangeField = rangeField;
    }

    /**
     * Add range boosts
     */
    private Query maybeWrapWithRanges(Query baseQuery, Map<Integer, Double> rangeScores, int maxDocID) {
        if (rangeScores == null || rangeScores.isEmpty()) {
            return baseQuery;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(baseQuery, BooleanClause.Occur.MUST);

        List<Integer> ranges = new ArrayList<>(rangeScores.keySet());
        ranges.sort(Comparator.comparingDouble(rangeScores::get).reversed());

        // Split docID space evenly into range bins
        int binSize = Math.max(1, (int) Math.ceil((double) maxDocID / (ranges.size() + 1)));

        for (int range : ranges) {
            int lower = range * binSize;
            int upper = (range + 1) * binSize - 1;
            Query rangeQuery = IntPoint.newRangeQuery(rangeField, lower, upper);

            double boost = rangeScores.getOrDefault(range, 1.0);
            if (boost > 0.0) {
                rangeQuery = new BoostQuery(rangeQuery, (float) boost);
            }

            builder.add(rangeQuery, BooleanClause.Occur.SHOULD);
        }

        return builder.build();
    }

    public TopDocs search(Query query, Map<Integer, Double> rangeScores, int maxDocID) throws IOException {
        double estimatedSLA = slaEstimator.estimate(query, searcher.getIndexReader(), slaThresholdMs);
        AnytimeRankingCollectorManager collectorManager =
                new AnytimeRankingCollectorManager(topK, (long) estimatedSLA);

        Query finalQuery = maybeWrapWithRanges(query, rangeScores, maxDocID);
        return searcher.search(finalQuery, collectorManager);
    }
}