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
package org.apache.lucene.sandbox.facet.ranges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

/** {@link RangeFacetCutter} for ranges of long value that don't overlap. * */
class ExclusiveLongRangeFacetCutter extends LongRangeFacetCutter {
  ExclusiveLongRangeFacetCutter(
      String field,
      MultiLongValuesSource longValuesSource,
      LongValuesSource singleLongValuesSource,
      LongRange[] longRanges) {
    super(field, longValuesSource, singleLongValuesSource, longRanges);
  }

  @Override
  List<InclusiveRange> buildElementaryIntervals() {
    List<InclusiveRange> elementaryIntervals = new ArrayList<>();
    long prev = Long.MIN_VALUE;
    for (LongRangeAndPos range : sortedRanges) {
      if (range.range().min > prev) {
        // add a "gap" range preceding requested range if necessary:
        elementaryIntervals.add(new InclusiveRange(prev, range.range().min - 1));
      }
      // add the requested range:
      elementaryIntervals.add(new InclusiveRange(range.range().min, range.range().max));
      prev = range.range().max + 1;
    }
    if (elementaryIntervals.isEmpty() == false) {
      long lastEnd = elementaryIntervals.get(elementaryIntervals.size() - 1).end();
      if (lastEnd < Long.MAX_VALUE) {
        elementaryIntervals.add(new InclusiveRange(lastEnd + 1, Long.MAX_VALUE));
      }
    } else {
      // If no ranges were requested, create a single entry from MIN_VALUE to MAX_VALUE:
      elementaryIntervals.add(new InclusiveRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    return elementaryIntervals;
  }

  @Override
  public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
    if (singleValues != null) {
      LongValues values = singleValues.getValues(context, null);
      return new ExclusiveLongRangeSinglevalueFacetLeafCutter(
          values, boundaries, pos, requestedRangeCount);
    } else {
      MultiLongValues values = valuesSource.getValues(context);
      return new ExclusiveLongRangeMultivalueFacetLeafCutter(
          values, boundaries, pos, requestedRangeCount);
    }
  }

  /**
   * TODO: dedup ExclusiveLongRangeMultivalueFacetLeafCutter and
   * ExclusiveLongRangeSinglevalueFacetLeafCutter code - they are similar but they extend different
   * base classes.
   */
  static class ExclusiveLongRangeMultivalueFacetLeafCutter
      extends LongRangeMultivaluedFacetLeafCutter {

    ExclusiveLongRangeMultivalueFacetLeafCutter(
        MultiLongValues longValues, long[] boundaries, int[] pos, int requestedRangeCount) {
      super(longValues, boundaries, pos, requestedRangeCount);
    }

    @Override
    public int nextOrd() throws IOException {
      if (elementaryIntervalTracker == null) {
        return NO_MORE_ORDS;
      }
      while (true) {
        int ordinal = elementaryIntervalTracker.nextOrd();
        if (ordinal == NO_MORE_ORDS) {
          return NO_MORE_ORDS;
        }
        int result = pos[ordinal];
        if (result != SKIP_INTERVAL_POSITION) {
          return result;
        }
      }
    }
  }

  static class ExclusiveLongRangeSinglevalueFacetLeafCutter
      extends LongRangeSinglevaluedFacetLeafCutter {
    ExclusiveLongRangeSinglevalueFacetLeafCutter(
        LongValues longValues, long[] boundaries, int[] pos, int requestedRangeCount) {
      super(longValues, boundaries, pos, requestedRangeCount);
    }

    @Override
    public int nextOrd() throws IOException {
      /*if (elementaryIntervalTracker == null) {
          return NO_MORE_ORDS;
      }*/
      while (true) {
        int ordinal = elementaryIntervalTracker.nextOrd();
        if (ordinal == NO_MORE_ORDS) {
          return NO_MORE_ORDS;
        }
        int result = pos[ordinal];
        if (result != SKIP_INTERVAL_POSITION) {
          // TODO: as soon as we return single interval here we can set nextOrd to NO_MORE_ORDS
          return result;
        }
      }
    }
  }
}
