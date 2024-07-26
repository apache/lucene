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

import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.util.NumericUtils;

/** {@link FacetCutter} for ranges * */
public abstract class RangeFacetCutter implements FacetCutter {

  // TODO: make the constructor also take in requested value sources and ranges
  // Ranges can be done now, we need to make a common interface for ValueSources
  RangeFacetCutter() {}

  // TODO: it is exactly the same as DoubleRangeFacetCounts#getLongRanges (protected), we should
  // dedup
  LongRange[] mapDoubleRangesToSortableLong(DoubleRange[] doubleRanges) {
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
