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

/** Represents counts for long range on range faceting */
public class LongRangeOnRangeFacetCounts extends RangeOnRangeFacetCounts {

  /**
   * Represents counts for long range on range faceting
   *
   * @param field document's field
   * @param hits hits we want the counts of
   * @param queryType type of intersection we want to count
   * @param ranges ranges we want the counts of
   * @throws IOException low level exception
   */
  public LongRangeOnRangeFacetCounts(
      String field, FacetsCollector hits, RangeFieldQuery.QueryType queryType, LongRange... ranges)
      throws IOException {
    super(field, hits, queryType, null, ranges);
  }

  /**
   * Represents counts for long range on range faceting
   *
   * @param field document's field
   * @param hits hits we want the counts of
   * @param queryType type of intersection we want to count
   * @param fastMatchQuery query to quickly discard hits
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
    super(field, hits, queryType, fastMatchQuery, ranges);
  }

  @Override
  public byte[] getEncodedRange(Range range) {
    LongRange longRange = (LongRange) range;
    byte[] result = new byte[2 * Long.BYTES];
    verifyAndEncode(new long[] {longRange.min}, new long[] {longRange.max}, result);
    return result;
  }
}
