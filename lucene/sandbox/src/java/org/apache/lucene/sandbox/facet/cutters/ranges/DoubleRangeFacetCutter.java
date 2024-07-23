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
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.util.NumericUtils;

/**
 * {@link RangeFacetCutter} for ranges of double values.
 *
 * <p>Based on {@link org.apache.lucene.facet.range.DoubleRangeFacetCounts}, this class translates
 * double ranges to long ranges using {@link NumericUtils#doubleToSortableLong} and delegates
 * faceting work to a {@link LongRangeFacetCutter}.
 */
public class DoubleRangeFacetCutter extends RangeFacetCutter {

  LongRangeFacetCutter longRangeFacetCutter;

  MultiDoubleValuesSource multiDoubleValuesSource;
  DoubleValuesSource singleDoubleValuesSource;
  DoubleRange[] doubleRanges;

  MultiLongValuesSource multiLongValuesSource;
  LongValuesSource singleLongValuesSource;

  LongRange[] longRanges;

  /** Constructor. */
  public DoubleRangeFacetCutter(
      String field, MultiDoubleValuesSource valuesSource, DoubleRange[] doubleRanges) {
    super(field);
    this.multiDoubleValuesSource = valuesSource;
    this.singleDoubleValuesSource = MultiDoubleValuesSource.unwrapSingleton(valuesSource);
    this.doubleRanges = doubleRanges;
    if (singleDoubleValuesSource != null) { // TODO: ugly!
      this.singleLongValuesSource = singleDoubleValuesSource.toSortableLongDoubleValuesSource();
    } else {
      this.multiLongValuesSource = multiDoubleValuesSource.toSortableMultiLongValuesSource();
    }
    this.longRanges = mapDoubleRangesToSortableLong(doubleRanges);
    this.longRangeFacetCutter =
        LongRangeFacetCutter.create(
            field, multiLongValuesSource, singleLongValuesSource, longRanges);
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    return longRangeFacetCutter.createLeafCutter(context);
  }
}
