package org.apache.lucene.facet.sandbox.abstracts;

import java.io.IOException;

public interface OrdinalIterator {
    int NO_MORE_ORDS = -1;

    /** returns next ord for current document or {@link #NO_MORE_ORDS}
     * TODO: should we implement numOfOrds instead? Cons: we probably don't always know the number in advance.
     *  e.g. when filtering by parentID, etc.
     **/
    int nextOrd() throws IOException;
}
