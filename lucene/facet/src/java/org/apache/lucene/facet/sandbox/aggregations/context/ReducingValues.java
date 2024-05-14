package org.apache.lucene.facet.sandbox.aggregations.context;

import org.apache.lucene.index.LeafReaderContext;

public interface ReducingValues {

    void setLeaf(LeafReaderContext context);

    boolean advance(int doc);

    default int reduce(int prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    default long reduce(long prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    default float reduce(float prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    default double reduce(double prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }
}
