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
