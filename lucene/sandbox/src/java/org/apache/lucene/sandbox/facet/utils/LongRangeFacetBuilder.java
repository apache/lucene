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
package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.cutters.ranges.LongRangeFacetCutter;
import org.apache.lucene.sandbox.facet.labels.RangeOrdToLabel;

/**
 * {@link FacetBuilder} factory for long range facets.
 *
 * @lucene.experimental
 */
public final class LongRangeFacetBuilder {

  private LongRangeFacetBuilder() {}

  /** Request long range facets for numeric field by name. */
  public static CommonFacetBuilder create(String field, LongRange... ranges) {
    return create(field, MultiLongValuesSource.fromLongField(field), ranges);
  }

  /**
   * Request long range facets for provided {@link MultiLongValuesSource} by default sorted in
   * original ranges order.
   *
   * @param dimension dimension to return in results to match {@link
   *     org.apache.lucene.facet.range.LongRangeFacetCounts#getTopChildren(int, String, String...)}
   *     results
   * @param valuesSource value source
   * @param ranges ranges
   */
  public static CommonFacetBuilder create(
      String dimension, MultiLongValuesSource valuesSource, LongRange... ranges) {
    return new CommonFacetBuilder(
            dimension,
            LongRangeFacetCutter.create(valuesSource, ranges),
            new RangeOrdToLabel(ranges))
        .withSortByOrdinal();
  }
}
