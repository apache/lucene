package org.apache.lucene.facet.sandbox;

import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRollup;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Arrays;

/** TODO */
public final class MultiFacetsRecorder implements FacetRecorder {

    private final FacetRecorder[] delegates;

    /** TODO */
    public MultiFacetsRecorder(FacetRecorder... delegates) {
        this.delegates = delegates;
    }
    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) {
        FacetLeafRecorder[] leafDelegates = Arrays.stream(delegates).map(k -> getLeafRecorder(context)).toArray(FacetLeafRecorder[]::new);
        return new MultiFacetsLeafRecorder(leafDelegates);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        assert delegates.length > 0;
        return delegates[0].recordedOrds();
    }

    @Override
    public void reduce(FacetRollup facetRollup) throws IOException {
        for (FacetRecorder recorder: delegates) {
            recorder.reduce(facetRollup);
        }
    }

    private static final class MultiFacetsLeafRecorder implements FacetLeafRecorder {

        private final FacetLeafRecorder[] delegates;

        private MultiFacetsLeafRecorder(FacetLeafRecorder[] delegates) {
            this.delegates = delegates;
        }

        @Override
        public void record(int docId, int facetId) {
            for (FacetLeafRecorder leafRecorder: delegates) {
                leafRecorder.record(docId, facetId);
            }
        }

        @Override
        public void finish(FacetLeafCutter cutter) {
            // TODO: remove the method
        }
    }
}
