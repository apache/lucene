package org.apache.lucene.sandbox.facet.aggregations;

import org.apache.lucene.sandbox.facet.abstracts.GetOrd;
import org.apache.lucene.sandbox.facet.abstracts.OrdToComparable;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Class that consumes incoming ordinals, sorts them by provided long aggregations, and returns in sorted order.
 * TODO: rename to TopnOrdinalIterator, and create TopnOrdinalIterator to use when we want to sort everything
 *  e.g. ranges? Sorting must be faster than managing PQ of everything?
 */
public class TopnOrdinalIterator<T extends Comparable<T> & GetOrd> implements OrdinalIterator {

    private final OrdToComparable<T> ordToComparable;
    private final OrdinalIterator sourceOrds;
    private final int topN;
    private int[] result;
    private int currentIndex;
    /** TODO */
    public TopnOrdinalIterator(OrdinalIterator sourceOrds,
                               OrdToComparable<T> ordToComparable,
                               int topN) {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
        }
        this.sourceOrds = sourceOrds;
        this.ordToComparable = ordToComparable;
        this.topN = topN;
    }

    private void getTopN() throws IOException {
        assert result == null;
        // TODO: current taxonomy implementations limit queue size by taxo reader size too, but this
        //  probably doesn't make sense for large enough taxonomy indexes?
        //  e.g. TopOrdAndIntQueue q = new TopComparableQueue(Math.min(taxoReader.getSize(), topN));
        //  This is +1 for making OrdinalIterator size base rather than NO_MORE_ORDS base; which is happening
        //  to some Lucene classes already, and there must be good reason to do it?
        //  Note that getAllChildren doesn't use queues, so this is not the reason we are limiting by taxonomy size.
        // TODO: create queue lazily - skip if first nextOrd is NO_MORE_ORDS
        TopComparableQueue<T> queue = new TopComparableQueue<>(topN);
        T reuse = null;
        for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS;) {
            reuse = ordToComparable.getComparable(nextOrdinal, reuse);
            reuse = queue.insertWithOverflow(reuse);
            nextOrdinal = sourceOrds.nextOrd();
        }
        // Now we need to read from the queue as well as the queue gives the least element, not the top.
        result = new int[queue.size()];
        for(int i = result.length - 1; i >=0; i--) {
            result[i] = queue.pop().getOrd();
        }
        currentIndex = 0;
    }

    @Override
    public int nextOrd() throws IOException {
        if (result == null) {
            getTopN();
        }
        assert result != null;
        if (currentIndex >= result.length) {
            return NO_MORE_ORDS;
        }
        return result[currentIndex++];
    }

    /** Keeps highest results, first by largest int value, then tie-break by smallest ord. */
    private static class TopComparableQueue<T extends Comparable<T>> extends PriorityQueue<T> {

        /** Sole constructor. */
        public TopComparableQueue(int topN) {
            super(topN);
        }

        @Override
        protected boolean lessThan(T a, T b) {
            return a.compareTo(b) < 0;
        }
    }
}
