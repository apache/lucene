package org.apache.lucene.facet.sandbox.aggregations.recorder;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.lucene.facet.sandbox.abstracts.*;
import org.apache.lucene.facet.sandbox.aggregations.context.ReducingValues;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**TODO: add doc**/
public class LongAggregationsRecorder implements FacetRecorder {

    final ReducingValues[] requestedFunctionValues;

    final List<LongAggregationsFacetLeafRecorder> leaves;

    // TODO: should we have one per leaf and then combine in the reduce method? Past tests on AggregationTaxonomyFacets
    // didn't show any benefits
    final SafeIntLongHashMap[] perOrdinalAggregations;

    LongAggregationsRecorder(ReducingValues[] functionValues) {
        requestedFunctionValues = functionValues;
        leaves = new ArrayList<>();
        perOrdinalAggregations = new SafeIntLongHashMap[functionValues.length];
        for (int i = 0; i < functionValues.length; i++) {
            perOrdinalAggregations[i] = new SafeIntLongHashMap();
        }
    }

    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
        Arrays.stream(requestedFunctionValues).forEach(f -> f.setLeaf(context));
        //        this.leaves.add(leafRecorder);
        return new LongAggregationsFacetLeafRecorder(requestedFunctionValues.clone(), perOrdinalAggregations);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        // TODO: what should this return? Should we return all recorded ordinals across?
        IntHashSet allOrdinals = new IntHashSet();
        for (SafeIntLongHashMap perOrdinalAggregation : perOrdinalAggregations) {
            allOrdinals.addAll(perOrdinalAggregation.keys());
        }

        Iterator<IntCursor> ordIterator = allOrdinals.iterator();

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

    static class LongAggregationsFacetLeafRecorder implements FacetLeafRecorder {

        final ReducingValues[] functionValues;
        IntLongHashMap[] perOrdinalAggregations;

        LongAggregationsFacetLeafRecorder(ReducingValues[] functionValues, IntLongHashMap[] perOrdinalAggregations) {
            this.functionValues = functionValues;
            this.perOrdinalAggregations = perOrdinalAggregations;
//            leafPerOrdinalAggregations = new IntLongHashMap[functionValues.length];
//            for (int i = 0; i < functionValues.length; i++) {
//                leafPerOrdinalAggregations[i] = new IntLongHashMap();
//            }
        }

        @Override
        public void record(int docId, int facetId) {
            Arrays.stream(functionValues).forEach(f -> f.advance(docId));
            for (int i = 0; i < functionValues.length; i++) {
                if (functionValues[i].advance(docId)) {
                    long prev = perOrdinalAggregations[i].get(facetId);
                    perOrdinalAggregations[i].put(facetId, functionValues[i].reduce(prev));
                }
            }
        }

        @Override
        public void finish(FacetLeafCutter cutter) {
            // Why does this need to take the leaf cutter?

        }

    }

    static class SafeIntLongHashMap extends IntLongHashMap {
        @Override
        public synchronized long put(int key, long value) {
            return super.put(key, value);
        }
    }
}

