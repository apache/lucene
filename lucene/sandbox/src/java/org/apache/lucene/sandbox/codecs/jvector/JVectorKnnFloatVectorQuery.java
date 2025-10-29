/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.knn.index.codec.jvector;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * {@link KnnFloatVectorQuery} that uses jVector to perform the search.
 * We use this wrapper simply because we can't pass jVector specific parameters with the upstream {@link KnnFloatVectorQuery}.
 */
public class JVectorKnnFloatVectorQuery extends KnnFloatVectorQuery {
    private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;
    private final int overQueryFactor;
    private final float threshold;
    private final float rerankFloor;
    private final boolean usePruning;

    public JVectorKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        int overQueryFactor,
        float threshold,
        float rerankFloor,
        boolean usePruning
    ) {
        super(field, target, k);
        this.overQueryFactor = overQueryFactor;
        this.threshold = threshold;
        this.rerankFloor = rerankFloor;
        this.usePruning = usePruning;
    }

    public JVectorKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        Query filter,
        int overQueryFactor,
        float threshold,
        float rerankFloor,
        boolean usePruning
    ) {
        super(field, target, k, filter);
        this.overQueryFactor = overQueryFactor;
        this.threshold = threshold;
        this.rerankFloor = rerankFloor;
        this.usePruning = usePruning;
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        int visitedLimit,
        KnnCollectorManager knnCollectorManager
    ) throws IOException {
        final KnnCollector delegateCollector = knnCollectorManager.newCollector(visitedLimit, KnnSearchStrategy.Hnsw.DEFAULT, context);
        final KnnCollector knnCollector = new JVectorKnnCollector(delegateCollector, threshold, rerankFloor, overQueryFactor, usePruning);
        LeafReader reader = context.reader();
        FloatVectorValues floatVectorValues = reader.getFloatVectorValues(field);
        if (floatVectorValues == null) {
            FloatVectorValues.checkField(reader, field);
            return NO_RESULTS;
        }
        if (Math.min(knnCollector.k(), floatVectorValues.size()) == 0) {
            return NO_RESULTS;
        }
        reader.searchNearestVectors(field, getTargetCopy(), knnCollector, acceptDocs);
        TopDocs results = knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }
}
