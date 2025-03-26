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
package org.apache.lucene.util;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

/**
 * Estimate SLA based the number of query terms, query complexity,
 * and average document frequency
 */
public class HeuristicSLAEstimator implements SLAEstimator {

    private static final int MAX_SAMPLE_TERMS = 64;
    private static final double MAX_MULT = 3.0;
    private static final double TERM_COST = 0.2;
    private static final double FREQ_COST = 0.0015;
    private static final double QUERY_COMPLEXITY = 0.15;

    private final String field;

    public HeuristicSLAEstimator(String field) {
        this.field = field;
    }

    @Override
    public double estimate(Query query, IndexReader reader, long baseSlaMs) throws IOException {
        final int termCount = estimateTermCount(query);
        final double avgDocFreq = estimateAvgDocFreq(reader);
        final int queryCost = estimateQueryCost(query);

        double m = 1.0;
        m += TERM_COST * termCount;
        m += FREQ_COST * avgDocFreq;
        m += QUERY_COMPLEXITY * queryCost;

        return baseSlaMs * Math.min(MAX_MULT, m);
    }

    /**
     * Estimate of number of terms that will participate in the scoring
     */
    private int estimateTermCount(Query query) {
        if (query instanceof MatchAllDocsQuery) {
            return 0;
        }

        if (query instanceof TermQuery) {
            return 1;
        }

        if (query instanceof PhraseQuery pq) {
            return pq.getTerms().length;
        }

        if (query instanceof BooleanQuery bq) {
            int total = 0;
            for (BooleanClause clause : bq) {
                BooleanClause.Occur occur = clause.occur();
                if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.SHOULD) {
                    total += estimateTermCount(clause.query());
                }
            }
            return total > 0 ? total : 1;
        }

        if (query instanceof DisjunctionMaxQuery dmq) {
            // Take the cost of max cost branch
            int max = 0;
            for (Query sub : dmq.getDisjuncts()) {
                max = Math.max(max, estimateTermCount(sub));
            }
            return max > 0 ? max : 1;
        }

        if (query instanceof SynonymQuery sq) {
            return sq.getTerms().size();
        }

        if (query instanceof MultiTermQuery) {
            // Conservative measure - no way to actually estimate
            return 2;
        }

        if (query instanceof BoostQuery bq) {
            return estimateTermCount(bq.getQuery());
        }

        if (query instanceof ConstantScoreQuery csq) {
            return estimateTermCount(csq.getQuery());
        }

        // Default baseline cost - fallback if unknown query type
        // Returning 0 will reduce the SLA to impractical values

        return 1;
    }

    /**
     * Coarse estimate of query execution cost.
     */
    private int estimateQueryCost(Query query) {
        if (query instanceof MatchAllDocsQuery) {
            return 1;
        }

        if (query instanceof TermQuery) {
            return 2;
        }

        if (query instanceof PhraseQuery pq) {
            return 3 + pq.getTerms().length;
        }

        if (query instanceof BooleanQuery bq) {
            int total = 0;
            for (BooleanClause clause : bq) {
                BooleanClause.Occur occur = clause.occur();
                if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.SHOULD) {
                    total += estimateQueryCost(clause.query());
                }
            }
            return Math.max(2, total);
        }

        if (query instanceof DisjunctionMaxQuery dmq) {
            // Max cost branch
            int max = 0;
            for (Query sub : dmq.getDisjuncts()) {
                max = Math.max(max, estimateQueryCost(sub));
            }
            return 2 + max;
        }

        if (query instanceof SynonymQuery sq) {
            return 2 + sq.getTerms().size();
        }

        if (query instanceof MultiTermQuery) {
            // Conservative estimate - no way to actually measure
            return 6;
        }

        if (query instanceof BoostQuery bq) {
            return estimateQueryCost(bq.getQuery());
        }

        if (query instanceof ConstantScoreQuery csq) {
            return estimateQueryCost(csq.getQuery());
        }

        // Default baseline cost - fallback if unknown query type
        // Returning 0 will reduce the SLA to impractical values
        return 4;
    }

    /**
     * Computes average document frequency across a sample of terms in the field.
     */
    private double estimateAvgDocFreq(IndexReader reader) throws IOException {
        long docFreqSum = 0;
        int sampled = 0;

        for (LeafReaderContext leaf : reader.leaves()) {
            Terms terms = leaf.reader().terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum te = terms.iterator();
            while (te.next() != null && sampled < MAX_SAMPLE_TERMS) {
                docFreqSum += te.docFreq();
                sampled++;
            }

            if (sampled >= MAX_SAMPLE_TERMS) {
                break;
            }
        }

        return sampled > 0 ? (double) docFreqSum / sampled : 0.0;
    }
}