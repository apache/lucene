package org.apache.lucene.facet.sandbox.aggregations;

import com.carrotsearch.hppc.IntLongMap;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Facet recorder that computes multiple long aggregations per facet.
 * TODO: implement!
 */
public class LongAggregationsFacetRecorder implements FacetRecorder {
    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
        // TODO: init DVS for each field to be aggregated
        return new LongAggregationLeafPayload();
    }

    @Override
    public OrdinalIterator recordedOrds() {
        // TODO: implement
        return null;
    }

    public long getAggregation(int ord, int aggregationId) {
        // TODO: implement
        return 0L;
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
