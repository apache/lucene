package org.apache.lucene.sandbox.facet.ordinal_iterators;

import com.carrotsearch.hppc.IntSet;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;

import java.io.IOException;

/**
 * {@link OrdinalIterator} that only keeps ordinals from a candidate set.
 */
public class CandidateSetOrdinalIterator implements OrdinalIterator {

    private final IntSet candidates;
    private final OrdinalIterator sourceOrds;

    /**
     * Constructor.
     * TODO: do we really nead source ords? I think we do, we don't want to return ords that are candidates yet we don't have results for them.
     */
    public CandidateSetOrdinalIterator(OrdinalIterator sourceOrds, IntSet candidates) {
        this.candidates = candidates;
        this.sourceOrds = sourceOrds;
    }

    @Override
    public int nextOrd() throws IOException {
        for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS;) {
            if (candidates.contains(nextOrdinal)) {
                return nextOrdinal;
            }
            nextOrdinal = sourceOrds.nextOrd();
        }
        return NO_MORE_ORDS;
    }
}
