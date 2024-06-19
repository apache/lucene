package org.apache.lucene.sandbox.facet.abstracts;

import com.carrotsearch.hppc.IntArrayList;

import java.io.IOException;

/**
 * Iterate over ordinals.
 */
public interface OrdinalIterator {

    /**
     * This const is returned by nextOrd when there are no more ordinals.
     */
    int NO_MORE_ORDS = -1;

    /** Returns next ord for current document or {@link #NO_MORE_ORDS}. **/
    int nextOrd() throws IOException;

    /**
     * Convert to int array. Note that after this method is called original OrdinalIterator is exhausted.
     */
    default int[] toArray() throws IOException {
        IntArrayList cache = new IntArrayList();
        for (int nextOrdinal = this.nextOrd(); nextOrdinal != NO_MORE_ORDS;) {
            cache.add(nextOrdinal);
            nextOrdinal = this.nextOrd();
        }
        return cache.toArray();
    }
}
