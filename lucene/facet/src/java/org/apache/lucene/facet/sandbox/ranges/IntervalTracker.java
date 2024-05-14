package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/** add doc **/
public interface IntervalTracker extends OrdinalIterator {
    /** add doc **/
    void set(int i);

    /** add doc **/
    int size();

    /** add doc **/
    void clear();

    /** add doc **/
    boolean get(int index);

    /**TODO: add doc**/
    void freeze();

    /** add doc **/
    class SingleIntervalTracker implements IntervalTracker {

        int tracker;

        int intervalsWithHit = 0;

        SingleIntervalTracker() {
            tracker = NO_MORE_ORDS;
        }

        @Override
        public void set(int i) {
            tracker = i;
        }


        @Override
        public int size() {
            return 1;
        }

        @Override
        public void clear() {
            tracker = -1;
            intervalsWithHit = 0;
        }

        @Override
        public boolean get(int index) {
            return index == tracker;
        }

        @Override
        public void freeze() {
            if (tracker != -1) {
                intervalsWithHit = 1;
            }
        }

        @Override
        public int nextOrd() throws IOException {
            int trackerValue = tracker;
            if (trackerValue != NO_MORE_ORDS) {
                tracker = NO_MORE_ORDS;
            }
            return trackerValue;
        }
    }

    /** add doc **/
    class MultiIntervalTracker implements IntervalTracker {

        FixedBitSet tracker;
        int trackerState;
        int bitFrom;

        int intervalsWithHit;

        MultiIntervalTracker(int size) {
            tracker = new FixedBitSet(size);
            trackerState = 0;
            bitFrom = 0;
            intervalsWithHit = 0;
        }

        @Override
        public void set(int i) {
            tracker.set(i);
        }

        @Override
        public int size() {
            return tracker.length();
        }

        @Override
        public void clear() {
            tracker.clear();
            bitFrom = 0;
            trackerState = 0;
            intervalsWithHit = 0;
        }

        @Override
        public boolean get(int index) {
            return tracker.get(index);
        }

        @Override
        public void freeze() {
            intervalsWithHit = tracker.cardinality();
        }

        @Override
        public int nextOrd() throws IOException {
            if (trackerState == intervalsWithHit) {
                return NO_MORE_ORDS;
            }
            trackerState++;
            int nextSetBit = tracker.nextSetBit(bitFrom);
            bitFrom = nextSetBit + 1;
            return nextSetBit;
        }
    }
}
