package org.apache.lucene.facet.sandbox.aggregations.context;

import org.apache.lucene.index.LeafReaderContext;

/***
 * TODO: add doc
 */
public interface ReducingValues {

    /**TODO: add doc**/
    void setLeaf(LeafReaderContext context);

    /**TODO: add doc**/
    boolean advance(int doc);

    /**TODO: add doc**/
    default int reduce(int prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    /**TODO: add doc**/
    default long reduce(long prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    /**TODO: add doc**/
    default float reduce(float prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }

    /**TODO: add doc**/
    default double reduce(double prev) {
        throw new UnsupportedOperationException("Incompatible type");
    }
}
