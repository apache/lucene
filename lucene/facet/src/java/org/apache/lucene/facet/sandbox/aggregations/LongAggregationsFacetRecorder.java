package org.apache.lucene.facet.sandbox.aggregations;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRollup;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;
import java.util.Iterator;

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
public class LongAggregationsFacetRecorder implements FacetRecorder {

    private IntObjectHashMap<long[]> aggregates;

    final LongValuesSource[] longValuesSources;
    final Reducer[] reducers;

    static class SafeIntLongHashMap extends IntLongHashMap {
        @Override
        public synchronized long put(int key, long value) {
            return super.put(key, value);
        }
    }

    final SafeIntLongHashMap[] perOrdinalValues;

    /** TODO */
    public LongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
        assert longValuesSources.length == reducers.length;
        this.longValuesSources = longValuesSources;
        this.reducers = reducers;
        perOrdinalValues = new SafeIntLongHashMap[longValuesSources.length];
        for (int i = 0; i < longValuesSources.length; i++) {
            perOrdinalValues[i] = new SafeIntLongHashMap();
        }
    }

    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
        LongValues[] longValues = new LongValues[longValuesSources.length];
        for (int i=0; i< longValuesSources.length; i++) {
            longValues[i] = longValuesSources[i].getValues(context, null);
        }
        return new LongAggregationsFacetLeafRecorder(longValues, reducers, perOrdinalValues);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        if (perOrdinalValues.length == 0) {
            return null;
        }
        Iterator<IntCursor> ordIterator = perOrdinalValues[0].keys().iterator();
        return new OrdinalIterator() {
            @Override
            public int nextOrd() throws IOException {
                if (ordIterator.hasNext()) {
                    return ordIterator.next().value;
                } else {
                    return NO_MORE_ORDS;
                }
            }
        };
    }

    @Override
    public void reduce(FacetRollup facetRollup) throws IOException {
        // TODO: reduce
        if (facetRollup != null && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
            throw new UnsupportedOperationException("Rollup is required, but not implemented");
        }
    }

    /** Get aggregation value by its id */
    public long getRecordedValue(int ord, int aggregationId) {
        if (aggregationId < 0 || aggregationId >= perOrdinalValues.length) {
            throw new IllegalArgumentException("Invalid Aggregation");
        }
        // TODO: should we return any default value?
        return perOrdinalValues[aggregationId].get(ord);
    }

    static class LongAggregationsFacetLeafRecorder implements FacetLeafRecorder {

        final LongValues[] longValues;

        final Reducer[] reducers;

        final IntLongHashMap[] perOrdinalValues;

        LongAggregationsFacetLeafRecorder(LongValues[] longValues, Reducer[] reducers, IntLongHashMap[] perOrdinalValues) {
            this.longValues = longValues;
            this.reducers = reducers;
            this.perOrdinalValues = perOrdinalValues;
        }
        @Override
        public void record(int docId, int facetId) throws IOException {
            for (int i=0; i < longValues.length; i++) {
                LongValues values = longValues[i];
                if (values.advanceExact(docId)) {
                    long prev = perOrdinalValues[i].get(facetId);
                    long reducedValue = reducers[i].reduce(prev, values.longValue());
                    perOrdinalValues[i].put(facetId, reducedValue);
                }
            }
        }

        @Override
        public void finish(FacetLeafCutter cutter) {

        }
    }
}
