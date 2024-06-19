package org.apache.lucene.sandbox.facet.recorders;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.sandbox.facet.abstracts.Reducer;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;
import java.util.Iterator;

/**
 * TODO: add docs
 */
public class SyncLongAggregationsFacetRecorder extends LongAggregationsFacetRecorder {

    static class SafeIntLongHashMap extends IntLongHashMap {
        @Override
        public synchronized long put(int key, long value) {
            return super.put(key, value);
        }
    }

    SafeIntLongHashMap[] perOrdinalValues;

    /**
     * TODO
     *
     * @param longValuesSources TK
     * @param reducers TK
     */
    public SyncLongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
        super(longValuesSources, reducers);
    }

    @Override
    void initialisePerOrdinalValues() {
        perOrdinalValues = new SafeIntLongHashMap[longValuesSources.length];
        for (int i = 0; i < longValuesSources.length; i++) {
            perOrdinalValues[i] = new SafeIntLongHashMap();
        }
    }

    @Override
    LongAggregationsFacetLeafRecorder getLeafRecorder(LongValues[] longValues, Reducer[] reducers) {
        return new SyncLongAggFacetLeafRecorder(longValues, reducers, perOrdinalValues);
    }

    @Override
    public long getRecordedValue(int ord, int valuesId) {
        if (valuesId < 0 || valuesId >= perOrdinalValues.length) {
            throw new IllegalArgumentException("Invalid request for ordinal values");
        }
        // TODO: should we return any default value?
        return perOrdinalValues[valuesId].get(ord);
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
        if (facetRollup != null && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
            throw new UnsupportedOperationException("Rollup is required, but not implemented");
        }
    }

    private static class SyncLongAggFacetLeafRecorder extends LongAggregationsFacetLeafRecorder {

        final IntLongHashMap[] perOrdinalValues;
        SyncLongAggFacetLeafRecorder(LongValues[] longValues, Reducer[] reducers, IntLongHashMap[] perOrdinalValues) {
            super(longValues, reducers);
            this.perOrdinalValues = perOrdinalValues;
        }

        @Override
        void recordValue(int i, int facetId, long value) {
            // TODO: this must be sync method!
            long prev = perOrdinalValues[i].get(facetId);
            long reducedValue = reducers[i].reduce(prev, value);
            perOrdinalValues[i].put(facetId, reducedValue);
        }
    }
}
