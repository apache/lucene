package org.apache.lucene.sandbox.facet.ordinal_iterators;

import org.apache.lucene.sandbox.facet.abstracts.GetOrd;
import org.apache.lucene.sandbox.facet.abstracts.OrdToComparable;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * {@link OrdinalIterator} that consumes incoming ordinals, sorts them by Comparable, and returns in sorted order.
 */
public class SortOrdinalIterator<T extends Comparable<T> & GetOrd> implements OrdinalIterator {

    private final OrdToComparable<T> ordToComparable;
    private final OrdinalIterator sourceOrds;
    private int[] result;
    private int currentIndex;

    /**
     * @param sourceOrds source ordinals
     * @param ordToComparable object that creates Comparable for provided facet ordinal.
     *                        If null, ordinals are sorted in natural order (ascending).
     */
    public SortOrdinalIterator(OrdinalIterator sourceOrds,
                               OrdToComparable<T> ordToComparable) {
        this.sourceOrds = sourceOrds;
        this.ordToComparable = ordToComparable;
    }

    private void sort() throws IOException {
        assert result == null;
        result = sourceOrds.toArray();
        // TODO: it doesn't really work - we need List<T>.
        @SuppressWarnings({"unchecked"})
        T[] comparables = (T[]) new Object[result.length];
        for (int i = 0; i < result.length; i++) {
            comparables[i] = ordToComparable.getComparable(result[i], null);
        }
        new InPlaceMergeSorter() {
            @Override
            protected void swap(int i, int j) {
                int tmp = result[i];
                result[i] = result[j];
                result[j] = tmp;
                T tmp2 = comparables[i];
                comparables[i] = comparables[j];
                comparables[j] = tmp2;
            }

            @Override
            protected int compare(int i, int j) {
                return comparables[i].compareTo(comparables[j]);
            }
        }.sort(0, result.length);
        currentIndex = 0;
    }

    @Override
    public int nextOrd() throws IOException {
        if (result == null) {
            sort();
        }
        assert result != null;
        if (currentIndex >= result.length) {
            return NO_MORE_ORDS;
        }
        return result[currentIndex++];
    }
}
