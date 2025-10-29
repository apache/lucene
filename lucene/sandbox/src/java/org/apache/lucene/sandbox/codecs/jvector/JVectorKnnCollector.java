/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.knn.index.codec.jvector;

import lombok.Value;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.TopDocs;

/**
 * Wrapper class for KnnCollector that provides passing of additional parameters specific for JVector.
 */
@Value
public class JVectorKnnCollector implements KnnCollector {
    KnnCollector delegate;
    float threshold;
    float rerankFloor;
    int overQueryFactor;
    boolean usePruning;

    @Override
    public boolean earlyTerminated() {
        return delegate.earlyTerminated();
    }

    @Override
    public void incVisitedCount(int count) {
        delegate.incVisitedCount(count);
    }

    @Override
    public long visitedCount() {
        return delegate.visitedCount();
    }

    @Override
    public long visitLimit() {
        return delegate.visitLimit();
    }

    @Override
    public int k() {
        return delegate.k();
    }

    @Override
    public boolean collect(int docId, float similarity) {
        return delegate.collect(docId, similarity);
    }

    @Override
    public float minCompetitiveSimilarity() {
        return delegate.minCompetitiveSimilarity();
    }

    @Override
    public TopDocs topDocs() {
        return delegate.topDocs();
    }

    @Override
    public KnnSearchStrategy getSearchStrategy() {
        return delegate.getSearchStrategy();
    }
}
