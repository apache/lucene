package org.apache.lucene.sandbox.facet.recorders;

import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafRecorder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.abstracts.Reducer;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;

/**
 * Facet recorder that computes multiple long aggregations per facet.
 * TODO: implement!
 * TODO: I think we can use long[] as a value per facet, benefit that it gives us
 *   - reduce memory footprint as we have one map with facet ordinal as a key for all aggregations rather than
 *     N maps.
 *   - We need to sync map access only once per document, not sync per document per facet id. Also, if facetId is already in the map
 *     I think we can sync on the value (array) itself, not the entire map?
 *   - We can also try collecting in each leaf in a separate map and merge then in reduce(), this can be fast enough
 *     if we need to merge one map per leaf rather than N maps per leaf.
 *   - implementing rollup if necessary is easier and faster as we can accumulate values for parent in an array and then
 *     assign the array as a value rather than updates each running aggregation value individually.
 *   - [premature optimization idea] if instead of one array we keep aggregations in two LongVector (one for MAX aggregation
 *     and one for SUM) we can benefit from SIMD?
 */
public abstract class LongAggregationsFacetRecorder implements FacetRecorder {

    final LongValuesSource[] longValuesSources;
    final Reducer[] reducers;

    /** TODO */
    LongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
        assert longValuesSources.length == reducers.length;
        this.longValuesSources = longValuesSources;
        this.reducers = reducers;
        initialisePerOrdinalValues();
    }

    /** TODO: add doc
     *
     * @param useSyncMap TK
     * @param longValuesSources TK
     * @param reducers TK
     * @return TK
     */
    public static LongAggregationsFacetRecorder create(boolean useSyncMap, LongValuesSource[] longValuesSources, Reducer[] reducers) {
        if (useSyncMap) {
            return new SyncLongAggregationsFacetRecorder(longValuesSources, reducers);
        }
        return new ReducingLongAggregationsFacetRecorder(longValuesSources, reducers);
    }

    abstract void initialisePerOrdinalValues();

    /**TODO:
     *
     * @param context TK
     * @return TK
     * @throws IOException TK
     */
    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
        LongValues[] longValues = new LongValues[longValuesSources.length];
        for (int i=0; i< longValuesSources.length; i++) {
            longValues[i] = longValuesSources[i].getValues(context, null);
        }
        return getLeafRecorder(longValues, reducers);
    }

    abstract LongAggregationsFacetLeafRecorder getLeafRecorder(LongValues[] longValues, Reducer[] reducers);

    /**TODO
     *
     * @param ord TK
     * @param valuesId TK
     * @return TK
     */
    public abstract long getRecordedValue(int ord, int valuesId);

    static abstract class LongAggregationsFacetLeafRecorder implements FacetLeafRecorder {

        final LongValues[] longValues;

        final Reducer[] reducers;

        LongAggregationsFacetLeafRecorder(LongValues[] longValues, Reducer[] reducers) {
            this.longValues = longValues;
            this.reducers = reducers;
        }
        @Override
        public void record(int docId, int facetId) throws IOException {
            for (int i=0; i < longValues.length; i++) {
                LongValues values = longValues[i];
                if (values.advanceExact(docId)) {
                    recordValue(i, facetId, values.longValue());
                }
            }
        }

        abstract void recordValue(int i, int facetId, long value);

        @Override
        public void finish(FacetLeafCutter cutter) {

        }
    }
}
