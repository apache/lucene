package org.apache.lucene.sandbox.facet.abstracts;

/**
 * Generates {@link Comparable} for provided ordinal.
 * For example it can be used to find topN facet ordinals.
 * @param <T> something ordinals can be compared by.
 */
public interface OrdToComparable<T extends Comparable<T>> {

    /**
     * For given ordinal, get something it can be compared by.
     * @param ord ordinal.
     * @param reuse object that can be reused for building result. If null, new object should be created.
     * @return Comparable.
     */
    T getComparable(int ord, T reuse);
}
