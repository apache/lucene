package org.apache.lucene.util.hppc;

/**
 * Forked from HPPC, holding int index and Object value
 */
public final class ObjectCursor<VType> {
    /**
     * The current value's index in the container this cursor belongs to. The meaning of this index
     * is defined by the container (usually it will be an index in the underlying storage buffer).
     */
    public int index;

    /**
     * The current value.
     */
    public VType value;

    @Override
    public String toString() {
        return "[cursor, index: " + index + ", value: " + value + "]";
    }
}
