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
package org.apache.lucene.facet.rangeonrange;

import static org.apache.lucene.document.LongRange.verifyAndEncode;

import java.io.IOException;
import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.search.Query;

/**
 * Represents counts for long range on range faceting. To be more specific, this means that given a
 * range (or list of ranges), this class will count all the documents in the {@link FacetsCollector}
 * (or that match a fast match query) that contain ranges that "match" the provided ranges. These
 * ranges are specified by the field parameter and expected to be of type {@link
 * org.apache.lucene.document.LongRangeDocValuesField}. Matching is defined by the queryType param,
 * you can see the type of matching supported by looking at {@link
 * org.apache.lucene.document.RangeFieldQuery.QueryType}. In addition, this class supports
 * multidimensional ranges. A multidimensional range will be counted as a match if every dimension
 * matches the corresponding indexed range's dimension.
 */
public class LongRangeOnRangeFacetCounts extends RangeOnRangeFacetCounts {

  /**
   * Constructor without the fast match query, see other constructor description for more details.
   */
  public LongRangeOnRangeFacetCounts(
      String field, FacetsCollector hits, RangeFieldQuery.QueryType queryType, LongRange... ranges)
      throws IOException {
    super(
        field,
        hits,
        queryType,
        null,
        Long.BYTES,
        getEncodedRanges(ranges),
        Range.getLabelsFromRanges(ranges));
  }

  /**
   * Represents counts for long range on range faceting. See class javadoc for more details.
   *
   * @param field specifies a {@link org.apache.lucene.document.LongRangeDocValuesField} that will
   *     define the indexed ranges
   * @param hits hits we want to count against
   * @param queryType type of intersection we want to count (IE: range intersection, range contains,
   *     etc.)
   * @param fastMatchQuery query to quickly discard hits using some heuristic
   * @param ranges ranges we want the counts of
   * @throws IOException low level exception
   */
  public LongRangeOnRangeFacetCounts(
      String field,
      FacetsCollector hits,
      RangeFieldQuery.QueryType queryType,
      Query fastMatchQuery,
      LongRange... ranges)
      throws IOException {
    super(
        field,
        hits,
        queryType,
        fastMatchQuery,
        Long.BYTES,
        getEncodedRanges(ranges),
        Range.getLabelsFromRanges(ranges));
  }

  private static byte[][] getEncodedRanges(LongRange... ranges) {
    byte[][] result = new byte[ranges.length][2 * Long.BYTES * ranges[0].dims];
    for (int i = 0; i < ranges.length; i++) {
      verifyAndEncode(ranges[i].min, ranges[i].max, result[i]);
    }
    return result;
  }
}
