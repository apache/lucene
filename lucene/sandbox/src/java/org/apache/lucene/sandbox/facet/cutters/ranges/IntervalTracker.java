/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.sandbox.facet.cutters.ranges;

import java.io.IOException;
import org.apache.lucene.sandbox.facet.ordinals.OrdinalIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * A specialised ordinal iterator that supports write (set and clear) operations. Clients can write
 * data and freeze the state before reading data from it like any other OrdinalIterator. Instances
 * may be reused by clearing the current iterator E.g. LongRangeFacetCutter uses IntervalTracker
 * instances to map ranges to ordinals and track per-range data and retrieve recorded ranges for a
 * data set.
 */
interface IntervalTracker extends OrdinalIterator {
  /** track information for the seen input interval * */
  void set(int i);

  /** return number of intervals seen * */
  int size();

  /** clear recorded information on this tracker. * */
  void clear();

  /** check if any data for the interval has been recorded * */
  boolean get(int index);

  /** finalise any state before read operations can be performed on this OrdinalIterator */
  void freeze();

  /**
   * Interval Tracker that tracks data for one interval only. The interval is recorded only once iff
   * data belonging to the interval is encountered. TODO: deprecate if not needed (if we have
   * dedicated classes to handle single value sources). *
   */
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

  /**
   * Interval Tracker that tracks data for multiple intervals. The interval is recorded only once
   * iff data belonging to the interval is encountered *
   */
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
