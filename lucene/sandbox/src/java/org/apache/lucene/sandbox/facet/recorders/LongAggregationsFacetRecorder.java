package org.apache.lucene.sandbox.facet.recorders;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.FacetSliceRecorder;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.sandbox.facet.abstracts.Reducer;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;
import java.util.*;

/**
 * {@link FacetRecorder} that computes multiple long aggregations per facet.
 * TODO: [premature optimization idea] if instead of one array we keep aggregations in two LongVector (one for MAX aggregation
 *     and one for SUM) we can benefit from SIMD?
 */
public class LongAggregationsFacetRecorder implements FacetRecorder {

    private IntObjectHashMap<long[]> perOrdinalValues;
    private List<IntObjectHashMap<long[]>> sliceValues;

    private final LongValuesSource[] longValuesSources;
    private final Reducer[] reducers;

    /** Constructor. */
    public LongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
        assert longValuesSources.length == reducers.length;
        this.longValuesSources = longValuesSources;
        this.reducers = reducers;
        perOrdinalValues = new IntObjectHashMap<>();
        sliceValues = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public FacetSliceRecorder getSliceRecorder() throws IOException {
        IntObjectHashMap<long[]> sliceValuesRecorder = new IntObjectHashMap<>();
        sliceValues.add(sliceValuesRecorder);
        return new LongAggregationsFacetSliceRecorder(longValuesSources, reducers, sliceValuesRecorder);
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
    public boolean isEmpty() {
        return perOrdinalValues.isEmpty();
    }

    @Override
    public void reduce(FacetRollup facetRollup) throws IOException {
        boolean firstElement = true;
        for (IntObjectHashMap<long[]> leafValue : sliceValues) {
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
        // TODO: implement rollup
        if (facetRollup != null && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
            throw new UnsupportedOperationException("Rollup is required, but not implemented");
        }
    }

    public long getRecordedValue(int ord, int valuesId) {
        if (valuesId < 0 || valuesId >= longValuesSources.length) {
            throw new IllegalArgumentException("Invalid request for ordinal values");
        }
        long[] valuesForOrd = perOrdinalValues.get(ord);
        if (valuesForOrd != null) {
            return valuesForOrd[valuesId];
        }
        return -1; // TODO: missing value, what do we want to return? Zero might be a better option.
    }

    private static class LongAggregationsFacetSliceRecorder implements FacetSliceRecorder {

        private final LongValuesSource[] longValuesSources;
        private final Reducer[] reducers;
        private final IntObjectHashMap<long[]> valuesRecorder;

        private LongAggregationsFacetSliceRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers,
                                                   IntObjectHashMap<long[]> valuesRecorder) {
            this.longValuesSources = longValuesSources;
            this.reducers = reducers;
            this.valuesRecorder = valuesRecorder;
        }

        @Override
        public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
            LongValues[] longValues = new LongValues[longValuesSources.length];
            for (int i=0; i< longValuesSources.length; i++) {
                longValues[i] = longValuesSources[i].getValues(context, null);
            }

            return new LongAggregationsFacetLeafRecorder(longValues, reducers, valuesRecorder);
        }
    }

    private static class LongAggregationsFacetLeafRecorder implements FacetLeafRecorder {

        private final LongValues[] longValues;

        private final Reducer[] reducers;
        private final IntObjectHashMap<long[]> perOrdinalValues;


        LongAggregationsFacetLeafRecorder(LongValues[] longValues, Reducer[] reducers, IntObjectHashMap<long[]> perOrdinalValues) {
            this.longValues = longValues;
            this.reducers = reducers;
            this.perOrdinalValues = perOrdinalValues;
        }
        @Override
        public void record(int docId, int facetId) throws IOException {
            long[] valuesForOrd = perOrdinalValues.get(facetId);
            if (valuesForOrd == null) {
                valuesForOrd = new long[longValues.length];
                perOrdinalValues.put(facetId, valuesForOrd);
            }

            LongValues values;
            for (int i=0; i < longValues.length; i++) {
                // TODO: cache advance/longValue results for current doc? Skipped for now as LongValues themselves can keep the cache.
                values = longValues[i];
                if (values.advanceExact(docId)) {
                    valuesForOrd[i] = reducers[i].reduce(valuesForOrd[i], values.longValue());
                }
            }
        }
    }
}
