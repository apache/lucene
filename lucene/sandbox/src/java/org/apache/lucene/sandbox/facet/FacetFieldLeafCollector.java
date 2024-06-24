package org.apache.lucene.sandbox.facet;

import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafRecorder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

import java.io.IOException;

/**
 * LeafCollector that collects and records data for all facetIds for each doc.
 */
public class FacetFieldLeafCollector implements LeafCollector {

    private final FacetLeafCutter leafCutter;

    private final FacetLeafRecorder leafRecorder;

    /** Constructor */
    public FacetFieldLeafCollector(LeafReaderContext context, FacetLeafCutter leafCutter, FacetLeafRecorder leafPayload) {
        // TODO: we don't need context param?
        this.leafCutter = leafCutter;
        this.leafRecorder = leafPayload;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // TODO
    }

    @Override
    public void collect(int doc) throws IOException {
        if (leafCutter.advanceExact(doc) == false) {
            return;
        }
        for(int curOrd = leafCutter.nextOrd(); curOrd != FacetLeafCutter.NO_MORE_ORDS;) {
            leafRecorder.record(doc, curOrd);
            curOrd = leafCutter.nextOrd();
        }
    }

    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
        // TODO: any ideas?
        //  1. Docs that have values for the index field we about to facet on
        //  2. TK
        return LeafCollector.super.competitiveIterator();
    }

    // TODO do need to override any other methods?
}
