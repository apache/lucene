package org.apache.lucene.facet.sandbox;

import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafRecorder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

import java.io.IOException;

public class FacetFieldLeafCollector implements LeafCollector {

    private final FacetLeafCutter leafCutter;

    // TODO: should this be a list, so that we can register multiple types of recorders?
    //  E.g. min-and-max, long aggregations, double aggregations, vector aggregations?
    // TODO: Hmm, for min-and-max we can use yet another aggregations that work on the same field that is used
    //  in leafCutter; this feels a little weird, but that should work fast as we are positioned at the same doc and
    //  therefore should have the value cached? We need Min and Max for every range facet! Other option is to make sure
    //  we always add min and max as aggregations for ranges, know their aggregation IDs to read them.
    private final FacetLeafRecorder leafRecorder;

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
        leafCutter.advanceExact(doc);
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
