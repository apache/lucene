package org.apache.lucene.facet.sandbox.aggregations;

import com.carrotsearch.hppc.IntLongMap;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRollup;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

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
 * TODO: should we also have interfaces that have method getCount(ord) and/or getSortValue(ord) so that we can use results
 *  from this recorder for {@link OrdRank} to sort by count?
 */
public class LongAggregationsFacetRecorder implements FacetRecorder {

    //private IntObjectHashMap<LongVector> aggregates;

    /** TODO */
    public LongAggregationsFacetRecorder() {
    }

    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
        // TODO: init DVS for each field to be aggregated
        return new LongAggregationLeafPayload();
    }

    @Override
    public OrdinalIterator recordedOrds() {
        // TODO: implement
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void reduce(FacetRollup facetRollup) throws IOException {
        // TODO: reduce

        if (facetRollup != null && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
            throw new UnsupportedOperationException("Rollup is required, but not implemented");
        }
    }

    /** Get aggregation value by its id */
    public long getAggregation(int ord, int aggregationId) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static class LongAggregationLeafPayload implements FacetLeafRecorder {
        private LongAggregationLeafPayload() {
            // TODO
        }

        private RunningAggregation[] aggregations;


        @Override
        public void record(int docId, int facetId) {
            // TODO advance all values sources to the docID if not advanced yet.
            // TODO: aggregate for given facetId all aggregations
        }

        @Override
        public void finish(FacetLeafCutter cutter) {
            // TODO
        }
    }

    private static final class RunningAggregation {
        AssociationLongAggregationFunction relevanceCombiner;
        /** Per ordinal values **/
        final IntLongMap values;

        private RunningAggregation(IntLongMap values, AssociationLongAggregationFunction relevanceCombiner) {
            this.relevanceCombiner = relevanceCombiner;
            this.values = values;
        }

        synchronized long getValue(int ord) {
            return values.getOrDefault(ord, 0);
        }

        synchronized void applyValue(int ord, long value) {
            long prevValue = values.getOrDefault(ord, 0);
            values.put(ord, applyCombiner(prevValue, value));
        }

        long applyCombiner(long left, long right) {
            return relevanceCombiner.aggregateLong(left, right);
        }

    }
}
