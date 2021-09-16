package org.apache.lucene.facet.taxonomy;

import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.hppc.IntIntHashMap;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TestIntIntHashMapWIthFixedBitSet extends FacetTestCase {

    int numLabels;
    IntTaxonomyFacets.IntIntHashMapWithFixedBitSet intIntHashMapWithFixedBitSet;
    IntIntHashMap testMap;

    @Before
    public void setup() {
        numLabels = atLeast(1000);
        intIntHashMapWithFixedBitSet = new IntTaxonomyFacets.IntIntHashMapWithFixedBitSet(numLabels);
        testMap = new IntIntHashMap();
    }

    @Test
    public void testBasic() {
        addTo(4, 20);
        get(4);
        addTo(4, 3);
        get(4);
        addTo(1, 1);
        get(1);
        addTo(1, 2);
        get(1);
        addTo(0, 1);
        get(0);
        Set<KeyValue> expectedSet = new HashSet<>();
        for (IntIntCursor cursor : intIntHashMapWithFixedBitSet) {
            KeyValue keyValue = new KeyValue();
            System.out.println("class: " + cursor.key + ": " + cursor.value);
            keyValue.key = cursor.key;
            keyValue.value = cursor.value;
            expectedSet.add(keyValue);
        }
        assertEquals(3, expectedSet.size());
        assertTrue(expectedSet.containsAll(List.of(
                new KeyValue(4, 23),
                new KeyValue(1, 3),
                new KeyValue(0, 1)
        )));
    }

    @Test
    public void testRandom() {
        int numIters = atLeast(1000);
        for (int i = 0; i < numIters; i++) {
            int key = TestUtil.nextInt(random(), 1, numLabels - 10);
            int value = atLeast(1);
            addTo(key, value);
            get(key);
        }
    }

    @Test
    public void testIteratorRandom() {
        int numIters = atLeast(1000);
        int[] keyStartRange = {0, 1};
        int[] keyEndRange = {numLabels - 1, numLabels - 2};
        for (int keyStart : keyStartRange) {
            for (int keyEnd : keyEndRange) {
                for (int i = 0; i < numIters; i++) {
                    int key = TestUtil.nextInt(random(), keyStart, keyEnd);
                    int value = atLeast(1);
                    addTo(key, value);
                }
                Set<KeyValue> groundTruthSet = new HashSet<>();
                Set<KeyValue> expectedSet = new HashSet<>();
                for (IntIntHashMap.IntIntCursor cursor : testMap) {
                    KeyValue keyValue = new KeyValue();
                    keyValue.key = cursor.key;
                    keyValue.value = cursor.value;
                    groundTruthSet.add(keyValue);
                }
                for (IntIntCursor cursor : intIntHashMapWithFixedBitSet) {
                    KeyValue keyValue = new KeyValue();
                    keyValue.key = cursor.key;
                    keyValue.value = cursor.value;
                    expectedSet.add(keyValue);
                }
                assertEquals(groundTruthSet.size(), expectedSet.size());
                assertTrue(groundTruthSet.containsAll(expectedSet));
            }
        }
    }

    @Test
    public void testMaxInt() {
        numLabels = Integer.MAX_VALUE;
        intIntHashMapWithFixedBitSet = new IntTaxonomyFacets.IntIntHashMapWithFixedBitSet(numLabels);
        int val = atLeast(1000);
        addTo(Integer.MAX_VALUE - 1, val);
        testRandom();
        testIteratorRandom();
    }

    private static class KeyValue {
        public int key;
        public int value;

        public KeyValue() { }

        public KeyValue(int key, int value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyValue keyValue = (KeyValue) o;
            return key == keyValue.key && value == keyValue.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    private void addTo(int key, int value) {
        int testVal = intIntHashMapWithFixedBitSet.addTo(key, value);
        int truthVal = testMap.addTo(key, value);
        assertEquals(truthVal, testVal);
    }

    private int get(int key) {
        int testVal = intIntHashMapWithFixedBitSet.get(key);
        int truthVal = testMap.get(key);
        assertEquals(truthVal, testVal);
        return testVal;
    }
}
