package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public interface IntervalTracker extends OrdinalIterator {
    void set(int i);

    int size();

    void clear();

    boolean get(int index);

    class SingleIntervalTracker implements IntervalTracker {

        int tracker;

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
        }

        @Override
        public boolean get(int index) {
            return index == tracker;
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

    class MultiIntervalTracker implements IntervalTracker {

        FixedBitSet tracker;
        int trackerState;

        MultiIntervalTracker(int size) {
            tracker = new FixedBitSet(size);
            trackerState = 0;
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
        }

        @Override
        public boolean get(int index) {
            return tracker.get(index);
        }

        @Override
        public int nextOrd() throws IOException {
            if (trackerState == tracker.length()) {
                return NO_MORE_ORDS;
            }
            int pos = tracker.nextSetBit(trackerState);
            trackerState = pos;
            return pos;
        }
    }
}
