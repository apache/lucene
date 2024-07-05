package org.apache.lucene.sandbox.facet.recorders;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.FacetSliceRecorder;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator.NO_MORE_ORDS;

/**
 * {@link FacetRecorder} to count facets.
 * TODO: add an option to keep counts in an array, to improve performance for facets with small number of ordinals
 *       e.g. range facets. Options:
 *  - first 100/1k counts in array, the rest - in a map; the limit can also be provided in a constructor?
 *    It is similar to what LongValuesFacetCounts do today.
 *  - have a constructor option expeted max ordinal, and if it is set and below some threshold - use array
 *    only?
 */
public class CountFacetRecorder implements FacetRecorder {
    IntIntMap values;
    List<IntIntMap> perSliceValues;

    /**
     * Create.
     * TODO: deprecate.
     */
    public CountFacetRecorder(boolean useSyncMap) {
        this();
    }

    /**
     * Create.
     */
    public CountFacetRecorder() {
        //super();
        // Has to be synchronizedList as we have one recorder per all slices.
        perSliceValues = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Get count for provided ordinal.
     */
    public int getCount(int ord) {
        // TODO: allow or don't allow missing values?
        return values.get(ord);
    }

    @Override
    public FacetSliceRecorder getSliceRecorder() throws IOException {
        IntIntMap sliceValues = new IntIntHashMap();
        perSliceValues.add(sliceValues);
        return new CountSliceRecorder(sliceValues);
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
    public boolean isEmpty() {
        return values.isEmpty();
    }


    @Override
    public void reduce(FacetRollup facetRollup) throws IOException {
        boolean firstElement = true;
        for (IntIntMap leafRecords: perSliceValues) {
            if (firstElement) {
                values = leafRecords;
                firstElement = false;
            } else {
                for(IntIntCursor elem: leafRecords) {
                    values.addTo(elem.key, elem.value);
                }
            }
        }
        if (firstElement) {
            // TODO: do we need empty map by default?
            values = new IntIntHashMap();
        }

        if (facetRollup == null) {
            return;
        }

        OrdinalIterator dimOrds = facetRollup.getDimOrdsToRollup();
        for(int dimOrd = dimOrds.nextOrd(); dimOrd != NO_MORE_ORDS; ) {
            // TODO: we call addTo because this is what IntTaxonomyFacets does (add to current value).
            //  We might want to just replace the value instead? We should not have dimOrd in the map yet.
            values.addTo(dimOrd, rollup(dimOrd, facetRollup));
            dimOrd = dimOrds.nextOrd();
        }
    }

    private int rollup(int ord, FacetRollup facetRollup) throws IOException {
        OrdinalIterator childOrds = facetRollup.getChildrenOrds(ord);
        int accum = 0;
        for(int nextChild = childOrds.nextOrd(); nextChild != NO_MORE_ORDS; ) {
            // TODO: we call addTo because this is what IntTaxonomyFacets does (add to current value).
            //  We might want to just replace the value instead? We should not have nextChild in the map yet.
            accum += values.addTo(nextChild, rollup(nextChild, facetRollup));
            nextChild = childOrds.nextOrd();
        }
        return accum;
    }

    private static class CountSliceRecorder implements FacetSliceRecorder {
        private final IntIntMap values;

        public CountSliceRecorder(IntIntMap values) {
            this.values = values;
        }

        @Override
        public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
                return new CountLeafRecorder(values);
        }
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
    }
}
