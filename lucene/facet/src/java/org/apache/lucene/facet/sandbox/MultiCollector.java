package org.apache.lucene.facet.sandbox;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Wrapper for multiple collectors.
 * TODO: can we reuse existing implementations? I didn't see there is one when I created this class.
 *  exiting implementation relies on Object which makes sense as it can work with Collectors that return different types.
 *  In our case type param makes sense so that we don't need to cast results?
 */
public class MultiCollector<V> implements Collector {
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return new MultiLeafCollector();
    }

    @Override
    public ScoreMode scoreMode() {
        return null; // TODO
    }

    @Override
    public void setWeight(Weight weight) {
        Collector.super.setWeight(weight); // TODO
    }
}
