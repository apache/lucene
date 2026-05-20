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
import org.apache.lucene.facet.MultiDoubleValuesSource;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.util.NumericUtils;

/**
 * {@link FacetCutter} for ranges of double values.
 *
 * <p>Based on {@link DoubleRangeFacetCounts}, this class translates double ranges to long ranges
 * using {@link NumericUtils#doubleToSortableLong} and delegates faceting work to a {@link
 * LongRangeFacetCutter}.
 *
 * @lucene.experimental
 */
public final class DoubleRangeFacetCutter implements FacetCutter {

  private final LongRangeFacetCutter longRangeFacetCutter;

  /** Constructor. */
  public DoubleRangeFacetCutter(
      MultiDoubleValuesSource multiDoubleValuesSource, DoubleRange[] doubleRanges) {
    super();
    DoubleValuesSource singleDoubleValuesSource =
        MultiDoubleValuesSource.unwrapSingleton(multiDoubleValuesSource);
    LongValuesSource singleLongValuesSource;
    MultiLongValuesSource multiLongValuesSource;
    if (singleDoubleValuesSource != null) {
      singleLongValuesSource = singleDoubleValuesSource.toSortableLongDoubleValuesSource();
      multiLongValuesSource = null;
    } else {
      singleLongValuesSource = null;
      multiLongValuesSource = multiDoubleValuesSource.toSortableMultiLongValuesSource();
    }
    LongRange[] longRanges = mapDoubleRangesToSortableLong(doubleRanges);
    // TODO: instead of relying on either single value source or multi value source to be null, we
    // should create different factory methods for single and multi valued versions and use the
    // right one
    this.longRangeFacetCutter =
        LongRangeFacetCutter.createSingleOrMultiValued(
            multiLongValuesSource, singleLongValuesSource, longRanges);
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    return longRangeFacetCutter.createLeafCutter(context);
  }

  // TODO: it is exactly the same as DoubleRangeFacetCounts#getLongRanges (protected), we should
  // dedup
  private LongRange[] mapDoubleRangesToSortableLong(DoubleRange[] doubleRanges) {
    LongRange[] longRanges = new LongRange[doubleRanges.length];
    for (int i = 0; i < longRanges.length; i++) {
      DoubleRange dr = doubleRanges[i];
      longRanges[i] =
          new LongRange(
              dr.label,
              NumericUtils.doubleToSortableLong(dr.min),
              true,
              NumericUtils.doubleToSortableLong(dr.max),
              true);
    }
    return longRanges;
  }
}
