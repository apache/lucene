package org.apache.lucene.facet.sandbox;

import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * {@link Collector} that brings together {@link FacetCutter} and {@link FacetRecorder} to compute facets during
 *  collection phase.
 */
public class FacetFieldCollector implements Collector {
    private final FacetCutter facetCutter;
    private final FacetRecorder facetRecorder;

    /**
     * Collector for cutter+recorder pair.
     */
    public FacetFieldCollector(FacetCutter facetCutter, FacetRecorder facetRecorder) {
        this.facetCutter = facetCutter;
        this.facetRecorder = facetRecorder;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return new FacetFieldLeafCollector(
                context,
                facetCutter.createLeafCutter(context),
                facetRecorder.getLeafRecorder(context));
    }

    @Override
    public ScoreMode scoreMode() {
        // TODO: ???
        return null;
    }
}
