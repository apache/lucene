package org.apache.lucene.sandbox.facet.aggregations;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
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
        IntObjectHashMap<long[]> first = leafValues.get(0);
        int[] recordedOrds = first.keys;
        long[] values;
        // TODO: fix: some values might be missing if they are not in the first leaf?
        for (int ord: recordedOrds) {
            values = new long[longValuesSources.length];
            long[] leafValuesForOrd;
            for (IntObjectHashMap<long[]> leafValue : leafValues) {
                leafValuesForOrd = leafValue.get(ord);
                if (leafValuesForOrd != null) {
                    for (int i = 0; i < longValuesSources.length; i++) {
                        values[i] = reducers[i].reduce(values[i], leafValuesForOrd[i]);
                    }
                }
            }
            perOrdinalValues.put(ord, values);
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
            long[] valuesForOrd = perOrdinalValues.getOrDefault(facetId, new long[longValues.length]);
            long prev = valuesForOrd[i];
            valuesForOrd[i] = reducers[i].reduce(prev, value);
            perOrdinalValues.put(facetId, valuesForOrd);
        }
    }
}
