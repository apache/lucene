package org.apache.lucene.sandbox.facet.recorders;

import org.apache.lucene.sandbox.facet.abstracts.FacetLeafRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 *  {@link FacetRecorder} that contains multiple FacetRecorders.
 * */
public final class MultiFacetsRecorder implements FacetRecorder {

    private final FacetRecorder[] delegates;

    /** Constructor */
    public MultiFacetsRecorder(FacetRecorder... delegates) {
        this.delegates = delegates;
    }
    @Override
    public FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
//        TODO: find out why streams gives StackOverFlowError?
//        FacetLeafRecorder[] leafDelegates = Arrays.stream(delegates).map(k -> getLeafRecorder(context)).toArray(FacetLeafRecorder[]::new);
        FacetLeafRecorder[] leafDelegates = new FacetLeafRecorder[delegates.length];
        for (int i=0; i < delegates.length; i++) {
            leafDelegates[i] = delegates[i].getLeafRecorder(context);
        }
        return new MultiFacetsLeafRecorder(leafDelegates);
    }

    @Override
    public OrdinalIterator recordedOrds() {
        assert delegates.length > 0;
        return delegates[0].recordedOrds();
    }

    @Override
    public boolean isEmpty() {
        assert delegates.length > 0;
        return delegates[0].isEmpty();
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
        public void record(int docId, int facetId) throws IOException {
            // TODO: handle collection terminated exception
            for (FacetLeafRecorder leafRecorder: delegates) {
                leafRecorder.record(docId, facetId);
            }
        }

    }
}
