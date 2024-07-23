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

import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.abstracts.FacetCutter;
import org.apache.lucene.util.NumericUtils;

/** {@link FacetCutter} for ranges * */
public abstract class RangeFacetCutter implements FacetCutter {
  // TODO: we don't always have field, e.g. for custom DoubleValuesSources - let's remove it from
  // here?
  String field;

  // TODO: make the constructor also take in requested value sources and ranges
  // Ranges can be done now, we need to make a common interface for ValueSources
  RangeFacetCutter(String field) {
    this.field = field;
  }

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
