package org.apache.lucene.sandbox.facet;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

import java.io.IOException;

/**
 * Wrapper for multiple collectors.
 * TODO: can we reuse existing implementations? I didn't see there is one when I created this class.
 *  exiting implementation relies on Object which makes sense as it can work with Collectors that return different types.
 *  In our case type param makes sense so that we don't need to cast results?
 */
public class MultiLeafCollector implements LeafCollector {

    LeafCollector[] leafCollectors;

    /**
     * TODO: implement.
     */
    public MultiLeafCollector() {
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // TODO
    }

    @Override
    public void collect(int doc) throws IOException {
      // TODO: collect all
      // TODO: handle termination!
    }

    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
        // TODO: any ideas?
        //  1. union of all sub collector competitiveIterator s?
        return LeafCollector.super.competitiveIterator();
    }

    @Override
    public void finish() throws IOException {
        LeafCollector.super.finish();
    }
}
