package org.apache.lucene.facet.sandbox.aggregations;

import org.apache.lucene.facet.sandbox.abstracts.GetOrd;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Class that consumes incoming ordinals, sorts them by provided long aggregations, and returns in sorted order.
 * TODO: do we really want to use OrdToComparable? Using some custom interface that has lessThan might be easier and faster?
 */
public class SortOrdinalIterator<T extends Comparable<T> & GetOrd> implements OrdinalIterator {

    private final OrdToComparable<T> ordToComparable;
    private final OrdinalIterator sourceOrds;
    private final int topN;
    private TopComparableQueue<T> queue;
    private boolean queueIsReady = false;

    /** TODO */
    public SortOrdinalIterator(OrdinalIterator sourceOrds,
                               OrdToComparable<T> ordToComparable,
                               int topN) {
        this.sourceOrds = sourceOrds;
        this.ordToComparable = ordToComparable;
        this.topN = topN;
    }

    private void fillQueue() throws IOException {
        assert queueIsReady == false;
        // TODO: current taxonomy implementations limit queue size by taxo reader size too, but this
        //  probably doesn't make sense for large enough taxonomy indexes?
        //  e.g. TopOrdAndIntQueue q = new TopComparableQueue(Math.min(taxoReader.getSize(), topN));
        //  This is +1 for making OrdinalIterator size base rather than NO_MORE_ORDS base; which is happening
        //  to some Lucene classes already, and there must be good reason to do it?
        //  Note that getAllChildren doesn't use queues, so this is not the reason we are limiting by taxonomy size.
        // TODO: create queue lazily - skip if first nextOrd is NO_MORE_ORDS
        this.queue = new TopComparableQueue<>(topN);
        T reuse = null;
        for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS;) {
            reuse = ordToComparable.getComparable(nextOrdinal, reuse);
            reuse = queue.insertWithOverflow(reuse);
            nextOrdinal = sourceOrds.nextOrd();
        }
        queueIsReady = true;
    }

    @Override
    public int nextOrd() throws IOException {
        if (queueIsReady == false) {
            fillQueue();
        }
        T res = queue.pop();
        if (res == null) {
            return NO_MORE_ORDS;
        }
        return res.getOrd();
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
