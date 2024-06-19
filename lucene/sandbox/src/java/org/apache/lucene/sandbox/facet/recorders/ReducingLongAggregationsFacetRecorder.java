package org.apache.lucene.sandbox.facet.recorders;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.sandbox.facet.abstracts.Reducer;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;
import java.util.*;

/**
 * TODO: add doc
 */
public class ReducingLongAggregationsFacetRecorder extends LongAggregationsFacetRecorder {

    private IntObjectHashMap<long[]> perOrdinalValues;
    private List<IntObjectHashMap<long[]>> leafValues;

    /**
     * TODO
     *
     * @param longValuesSources
     * @param reducers
     */
    public ReducingLongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
        super(longValuesSources, reducers);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        if (perOrdinalValues.isEmpty()) {
            return null;
        }
        Iterator<IntCursor> ordIterator = perOrdinalValues.keys().iterator();
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
        boolean firstElement = true;
        for (IntObjectHashMap<long[]> leafValue : leafValues) {
            if (firstElement) {
                perOrdinalValues = leafValue;
                firstElement = false;
            } else {
                for(IntObjectCursor<long[]> elem: leafValue) {
                    long[] vals = perOrdinalValues.get(elem.key);
                    if (vals == null) {
                        perOrdinalValues.put(elem.key, elem.value);
                    } else {
                        for (int i = 0; i < longValuesSources.length; i++) {
                            vals[i] = reducers[i].reduce(vals[i], elem.value[i]);
                        }
                    }
                }
            }
        }
        if (firstElement) {
            // TODO: do we need empty map by default?
            perOrdinalValues = new IntObjectHashMap<>();
        }
        if (facetRollup != null && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
            throw new UnsupportedOperationException("Rollup is required, but not implemented");
        }
    }
    @Override
    void initialisePerOrdinalValues() {
        perOrdinalValues = new IntObjectHashMap<>();
        leafValues = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    LongAggregationsFacetLeafRecorder getLeafRecorder(LongValues[] longValues, Reducer[] reducers) {
        IntObjectHashMap<long[]> valuesRecorder = new IntObjectHashMap<>();
        leafValues.add(valuesRecorder);
        return new InnerLeafRecorder(longValues, reducers, valuesRecorder);
    }

    @Override
    public long getRecordedValue(int ord, int valuesId) {
        if (valuesId < 0 || valuesId >= longValuesSources.length) {
            throw new IllegalArgumentException("Invalid request for ordinal values");
        }
        long[] valuesForOrd = perOrdinalValues.get(ord);
        if (valuesForOrd != null) {
            return valuesForOrd[valuesId];
        }
        return -1;
    }

    private static class InnerLeafRecorder extends LongAggregationsFacetLeafRecorder {

        private final IntObjectHashMap<long[]> perOrdinalValues;

        InnerLeafRecorder(LongValues[] longValues, Reducer[] reducers, IntObjectHashMap<long[]> perOrdinalValues) {
            super(longValues, reducers);
            this.perOrdinalValues = perOrdinalValues;
        }

        @Override
        void recordValue(int i, int facetId, long value) {
            // TODO: we should not init array every time
            long[] valuesForOrd = perOrdinalValues.getOrDefault(facetId, new long[longValues.length]);
            long prev = valuesForOrd[i];
            valuesForOrd[i] = reducers[i].reduce(prev, value);
            perOrdinalValues.put(facetId, valuesForOrd);
        }
    }
}
