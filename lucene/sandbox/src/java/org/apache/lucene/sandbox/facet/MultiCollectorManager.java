package org.apache.lucene.sandbox.facet;

import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Manager for collector that can triggers collection for sub-collectors.
 * Preserves the order of collectors.
 * Expects all sub collectors (managers) to return result of the same type.
 *
 * TODO: can we just reuse existing MultiCollectorManager? It relies on Object which makes sense as it can work with Collectors that return different types.
 *  *  In our case type param makes sense so that we don't need to cast results?
 *
 * TODO: implement! (if steel need it)
 * @param <R>
 */
public class MultiCollectorManager<V, R> implements CollectorManager<MultiCollector<V>, R> {

    private final List<CollectorManager<?, ?>> subManagers;

    /** TODO */
    public MultiCollectorManager(List<CollectorManager<?, ?>> subManagers) {
        this.subManagers = subManagers;
    }

    @Override
    public MultiCollector<V> newCollector() throws IOException {
        return new MultiCollector<>(); // TODO
    }

    @Override
    public R reduce(Collection<MultiCollector<V>> collectors) throws IOException {
        return null;
    }
}
