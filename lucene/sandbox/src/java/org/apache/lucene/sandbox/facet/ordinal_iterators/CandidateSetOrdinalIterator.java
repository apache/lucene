package org.apache.lucene.sandbox.facet.ordinal_iterators;

import com.carrotsearch.hppc.IntSet;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;

import java.io.IOException;

/**
 * {@link OrdinalIterator} that filters out ordinals from delegate if they are not in the candidate set.
 *
 * Can be handy to get results only for specific facets.
 */
public class CandidateSetOrdinalIterator implements OrdinalIterator {

    private final IntSet candidates;
    private final OrdinalIterator sourceOrds;

    /**
     * Constructor.
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
