package org.apache.lucene.sandbox.facet.abstracts;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Recorder for a slice of work, similar to how {@link org.apache.lucene.search.Collector} and
 * {@link org.apache.lucene.search.CollectorManager} work - within Collector everything is single threaded,
 * so we don't need to synchronize. Within {@link FacetSliceRecorder} everything is also done by a single thread,
 * so we can keep single and simple data structure for data we are recording.
 *
 * TODO: in our case having additional layer of recorder per slice doesn't seem to improve performance at all,
 *  I think it is because we already have more threads than we have leafs? But still, I think its worth
 *  having this abstraction for other upstream Lucene users?
 */
public interface FacetSliceRecorder {
    /**
     * Get leaf recorder.
     */
    FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException;
}
