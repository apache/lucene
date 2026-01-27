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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToLongFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;

/** Utility class to re-order segments within an IndexReader to assist in early termination. */
public abstract class SegmentOrder {

  /** Produces a new view over an IndexReader by re-ordering the reader's segments */
  public abstract IndexReader reorder(IndexReader reader) throws IOException;

  /**
   * Builds a SegmentSorter that will reorder segments based on a given numeric Sort, such that
   * higher-sorting hits are more likely to be visited first, allowing early termination to more
   * efficiently skip whole segments. If the primary SortField of the Sort is not numeric then the
   * returned SegmentSorter will have no effect.
   */
  public static SegmentOrder fromSort(Sort sort) {
    SortField primarySort = sort.getSort()[0];
    Object missingValue = primarySort.getMissingValue();
    SortField.Type type = primarySort.getType();
    if (primarySort instanceof SortedNumericSortField snsf) {
      type = snsf.getNumericType();
    }
    return switch (type) {
      case INT ->
          numericSorter(
              primarySort.getField(),
              missingValue == null ? null : ((Integer) missingValue).longValue(),
              primarySort.getReverse(),
              b -> NumericUtils.sortableBytesToInt(b, 0));
      case LONG ->
          numericSorter(
              primarySort.getField(),
              missingValue == null ? null : (Long) missingValue,
              primarySort.getReverse(),
              b -> NumericUtils.sortableBytesToLong(b, 0));
      case FLOAT ->
          numericSorter(
              primarySort.getField(),
              missingValue == null
                  ? null
                  : (long) NumericUtils.floatToSortableInt((float) missingValue),
              primarySort.getReverse(),
              b -> NumericUtils.sortableBytesToInt(b, 0));
      case DOUBLE ->
          numericSorter(
              primarySort.getField(),
              missingValue == null
                  ? null
                  : NumericUtils.doubleToSortableLong((double) missingValue),
              primarySort.getReverse(),
              b -> NumericUtils.sortableBytesToLong(b, 0));
      // $CASES-OMITTED$
      default ->
          new SegmentOrder() {
            @Override
            public IndexReader reorder(IndexReader reader) {
              return reader;
            }
          };
    };
  }

  private static SegmentOrder numericSorter(
      String field, Long missingValue, boolean reverse, ToLongFunction<byte[]> pointDecoder) {
    NumericFieldReaderContextComparator comparator =
        new NumericFieldReaderContextComparator(field, missingValue, reverse, pointDecoder);
    return new SegmentOrder() {
      @Override
      public IndexReader reorder(IndexReader reader) throws IOException {
        return new BaseCompositeReader<>(
            reader.leaves().stream().map(LeafReaderContext::reader).toArray(LeafReader[]::new),
            comparator) {
          @Override
          protected void doClose() {
            // no op
          }

          @Override
          public CacheHelper getReaderCacheHelper() {
            return reader.getReaderCacheHelper();
          }
        };
      }
    };
  }

  private static class NumericFieldReaderContextComparator implements Comparator<LeafReader> {

    private final Map<Object, Long> cachedSortValues = new HashMap<>();
    private final String field;
    private final boolean reverse;
    private final Long missingValue;
    private final ToLongFunction<byte[]> pointDecoder;

    NumericFieldReaderContextComparator(
        String field, Long missingValue, boolean reverse, ToLongFunction<byte[]> pointDecoder) {
      this.field = field;
      this.missingValue = missingValue;
      this.reverse = reverse;
      this.pointDecoder = pointDecoder;
    }

    @Override
    public int compare(LeafReader o1, LeafReader o2) {
      return reverse
          ? Long.compare(getSortValue(o2), getSortValue(o1))
          : Long.compare(getSortValue(o1), getSortValue(o2));
    }

    private long getSortValue(LeafReader reader) {
      IndexReader.CacheKey key = reader.getCoreCacheHelper().getKey();
      if (cachedSortValues.containsKey(key) == false) {
        cachedSortValues.put(key, loadSortValue(reader));
      }
      return cachedSortValues.get(key);
    }

    private long loadSortValue(LeafReader reader) {
      try {
        DocValuesSkipper skipper = reader.getDocValuesSkipper(field);
        if (skipper != null) {
          if (skipper.docCount() == reader.maxDoc() || missingValue == null) {
            return reverse ? skipper.maxValue() : skipper.minValue();
          }
          if (reverse) {
            return Math.max(skipper.maxValue(), missingValue);
          } else {
            return Math.min(skipper.minValue(), missingValue);
          }
        }
        PointValues pointValues = reader.getPointValues(field);
        if (pointValues != null) {
          if (pointValues.getDocCount() == reader.maxDoc() || missingValue == null) {
            if (reverse) {
              return pointDecoder.applyAsLong(pointValues.getMaxPackedValue());
            } else {
              return pointDecoder.applyAsLong(pointValues.getMinPackedValue());
            }
          }
          if (reverse) {
            return Math.max(
                pointDecoder.applyAsLong(pointValues.getMaxPackedValue()), missingValue);
          } else {
            return Math.min(
                pointDecoder.applyAsLong(pointValues.getMinPackedValue()), missingValue);
          }
        }
      } catch (IOException _) {
        // We can't rethrow exceptions from inside a Comparator, so we instead
        // return as if there are no index structures to read values from.
        return reverse ? Long.MAX_VALUE : Long.MIN_VALUE;
      }
      return reverse ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
  }
}
