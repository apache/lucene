package org.apache.lucene.facet.sandbox.aggregations;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRollup;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator.NO_MORE_ORDS;

/**
 * {@link FacetRecorder} to count facets.
 */
public class CountRecorder implements FacetRecorder {

    /**
     * Create
     */
    public CountRecorder() {
        super();
    }

    /**
     * Get count for provided ordinal.
     */
    public int getCount(int ord) {
        // TODO: allow or don't allow missing values?
        return values.get(ord);
    }

    private static final class SafeIntIntHashMap extends IntIntHashMap {
        @Override
        public synchronized int addTo(int key, int incrementValue) {
            return super.addTo(key, incrementValue);
        }
    }

    // TODO: lazy init
    // TODO: doing it in syncronized class must be much slower than what we are doing now in fast taxo facets! Should we
    //  try building its own map for each leaf, and then find a way to merge it effectively?
    IntIntMap values = new SafeIntIntHashMap();
    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
        return new CountLeafRecorder(values);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        // TODO: is that performant enough?
        // TODO: even if this is called before collection started, we want it to use results from the time when nextOrd
        //  is first called. Does ordIterator work like that? I've run some tests that confirmed expected behavior,
        //  but I'm not sure IntIntMap guarantees that.
        Iterator<IntCursor> ordIterator = values.keys().iterator();
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
        if (facetRollup == null) {
            return;
        }
        // Don't need to do anything now because we collect all to a sync IntIntMap
        // TODO: would that be faster to collect per leaf and then reduce?
        OrdinalIterator dimOrds = facetRollup.getDimOrdsToRollup();
        for(int dimOrd = dimOrds.nextOrd(); dimOrd != NO_MORE_ORDS; ) {
            // TODO: we call addTo because this is what IntTaxonomyFacets does (add to current value).
            //  We might want to just replace the value instead? We should not have current value in the map.
            values.addTo(dimOrd, rollup(dimOrd, facetRollup));
            dimOrd = dimOrds.nextOrd();
        }
    }

    private int rollup(int ord, FacetRollup facetRollup) throws IOException {
        OrdinalIterator childOrds = facetRollup.getChildrenOrds(ord);
        int accum = 0;
        for(int nextChild = childOrds.nextOrd(); nextChild != NO_MORE_ORDS; ) {
            accum += values.addTo(nextChild, rollup(nextChild, facetRollup));
            nextChild = childOrds.nextOrd();
        }
        return accum;
    }

    private static class CountLeafRecorder implements FacetLeafRecorder {

        private final IntIntMap values;

        public CountLeafRecorder(IntIntMap values) {
            this.values = values;
        }

        @Override
        public void record(int docId, int facetId) {
            this.values.addTo(facetId, 1);
        }

        @Override
        public void finish(FacetLeafCutter cutter) {

        }
    }
}
