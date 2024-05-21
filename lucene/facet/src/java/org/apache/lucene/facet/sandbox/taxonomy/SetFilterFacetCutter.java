package org.apache.lucene.facet.sandbox.taxonomy;


import com.carrotsearch.hppc.IntSet;
import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.OrdToLabels;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Wrapper for another {@link FacetCutter} that skips ords that are not in the set.
 *
 * <p>Important: It assumes the incoming ords are in sorted order (which is the case for example for {@link TaxonomyFacetsCutter}),
 * so it also optimizes with min and max values.
 * TODO: create abstract wrapper implementation, use it here.
 */
public final class SetFilterFacetCutter implements FacetCutter {

    private final FacetCutter delegate;
    private final IntSet candidateOrds;
    private final int maxCandidateOrd;
    private final int minCandidateOrd;

    /** Constructor. */
    public SetFilterFacetCutter(FacetCutter delegate, IntSet candidateOrds, int maxCandidateOrd, int minCandidateOrd) {
        this.delegate = delegate;
        this.candidateOrds = candidateOrds;
        this.maxCandidateOrd = maxCandidateOrd;
        this.minCandidateOrd = minCandidateOrd;
        // Some guardrails to avoid inefficient use of this FacetCutter.
        assert candidateOrds.isEmpty() == false; // TODO: Should we allow empty candidate set edge case?
        assert candidateOrds.contains(OrdToLabels.INVALID_ORD) == false;
        assert minCandidateOrd >= 0 && minCandidateOrd < Integer.MAX_VALUE; // TODO: Should we allow empty candidate set edge case?
        assert maxCandidateOrd >= 0 && maxCandidateOrd < Integer.MAX_VALUE;
    }

    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        return new SetFilterFacetLeafCutter(delegate.createLeafCutter(context), candidateOrds, maxCandidateOrd, minCandidateOrd);
    }

    private static class SetFilterFacetLeafCutter implements FacetLeafCutter {

        private final IntSet candidateOrds;
        private final int maxCandidateOrd;
        private final int minCandidateOrd;

        private FacetLeafCutter delegate;

        private SetFilterFacetLeafCutter(FacetLeafCutter delegate, IntSet candidateOrds, int maxCandidateOrd, int minCandidateOrd) {
            this.delegate = delegate;
            this.candidateOrds = candidateOrds;
            this.maxCandidateOrd = maxCandidateOrd;
            this.minCandidateOrd = minCandidateOrd;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return this.delegate.advanceExact(doc);
        }

        @Override
        public int nextOrd() throws IOException {
            for(int nextDelegateOrd = delegate.nextOrd(); nextDelegateOrd != NO_MORE_ORDS; ) {
                if (nextDelegateOrd > maxCandidateOrd) {
                    return NO_MORE_ORDS;
                } else if (nextDelegateOrd >= minCandidateOrd && candidateOrds.contains(nextDelegateOrd)) {
                    return nextDelegateOrd;
                }
                // TODO: hmm, why don't we do it in the for loop?? Compiler does move it to the for loop I think.
                nextDelegateOrd = delegate.nextOrd();
            }
            return NO_MORE_ORDS;
        }
    }
}
