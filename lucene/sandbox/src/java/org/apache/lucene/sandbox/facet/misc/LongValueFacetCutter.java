package org.apache.lucene.sandbox.facet.misc;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.lucene.sandbox.facet.abstracts.FacetCutter;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;
import org.apache.lucene.sandbox.facet.abstracts.OrdLabelBiMap;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO: add doc
 * This class is quite inefficient. Will optimise later.
 */
public class LongValueFacetCutter implements FacetCutter, OrdLabelBiMap {
    private final String field;
    // TODO: consider alternatives if this is a bottleneck
    private final LongIntHashMap valueToOrdMap;
    private final IntLongHashMap ordToValueMap;
    private final AtomicInteger maxOrdinal;

    /**
     * TODO add doc
     * @param field TK
     */
    public LongValueFacetCutter(String field) {
        this.field = field;
        valueToOrdMap = new LongIntHashMap() {
            @Override
            public synchronized boolean putIfAbsent(long key, int value) {
                return super.putIfAbsent(key, value);
            }
        };
        ordToValueMap = new IntLongHashMap();
        maxOrdinal = new AtomicInteger(-1);
    }
    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        // TODO: later add support for other value sources e.g: LongValues
        SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), field);
        return new FacetLeafCutter() {
            int currDoc = -1;
            final LongHashSet valuesForDoc = new LongHashSet();
            private Iterator<LongCursor> valuesCursor;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (doc < currDoc) {
                    return false;
                }
                if (doc == currDoc) {
                    return true;
                }
                valuesForDoc.clear();
                if (docValues.advanceExact(doc)) {
                    int numValues = docValues.docValueCount();
                    for (int i = 0; i < numValues; i++) {
                        long value = docValues.nextValue();
                        valueToOrdMap.putIfAbsent(value, maxOrdinal.incrementAndGet());
                        ordToValueMap.put(valueToOrdMap.get(value), value);
                        valuesForDoc.add(value);
                    }
                    currDoc = doc;
                    valuesCursor = valuesForDoc.iterator();
                }
                return false;
            }

            @Override
            public int nextOrd() throws IOException {
                if (valuesCursor.hasNext()) {
                    return valueToOrdMap.get(valuesCursor.next().value);
                }
                return NO_MORE_ORDS;
            }
        };
    }

    @Override
    public FacetLabel getLabel(int ordinal) throws IOException {
        if (ordToValueMap.containsKey(ordinal)) {
            return new FacetLabel(String.valueOf(ordToValueMap.get(ordinal)));
        }
        return null;
    }

    @Override
    public FacetLabel[] getLabels(int[] ordinals) throws IOException {
        FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
        for (int i = 0; i < ordinals.length; i++) {
            facetLabels[i] = getLabel(ordinals[i]);
        }
        return facetLabels;
    }

    @Override
    public int getOrd(FacetLabel label) throws IOException {
        long value = Long.parseLong(label.getLeaf());
        if (valueToOrdMap.containsKey(value)) {
            return valueToOrdMap.get(value);
        }
        return -1;
    }

    @Override
    public int[] getOrds(FacetLabel[] labels) throws IOException {
        int[] ords = new int[labels.length];
        for (int i=0; i< labels.length; i++) {
            ords[i] = getOrd(labels[i]);
        }
        return ords;
    }
}
