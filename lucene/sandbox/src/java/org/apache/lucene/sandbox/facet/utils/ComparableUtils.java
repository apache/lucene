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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.sandbox.facet.cutters.LongValueFacetCutter;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * Collection of static methods to provide most common comparables for sandbox faceting. You can
 * also use it as an example for creating your own {@link ComparableSupplier} to enable custom
 * facets top-n and sorting.
 *
 * @lucene.experimental
 */
public final class ComparableUtils {
  private ComparableUtils() {}

  /** {@link ComparableSupplier} to sort by ords (ascending). */
  public static ComparableSupplier<ByOrdinalComparable> byOrdinal() {
    return new ComparableSupplier<>() {
      public void reuseComparable(int ord, ByOrdinalComparable reuse) {
        reuse.ord = ord;
      }

      public ByOrdinalComparable createComparable(int ord) {
        ByOrdinalComparable result = new ByOrdinalComparable();
        result.ord = ord;
        return result;
      }
    };
  }

  /** Used for {@link #byOrdinal} result. */
  public static class ByOrdinalComparable implements Comparable<ByOrdinalComparable> {

    private int ord;

    @Override
    public int compareTo(ByOrdinalComparable o) {
      return Integer.compare(o.ord, ord);
    }
  }

  /**
   * {@link ComparableSupplier} to sort ordinals by count (descending) with ord as a tie-break
   * (ascending) using provided {@link CountFacetRecorder}.
   */
  public static ComparableSupplier<ByCountComparable> byCount(CountFacetRecorder recorder) {
    return new ComparableSupplier<>() {
      public void reuseComparable(int ord, ByCountComparable reuse) {
        reuse.ord = ord;
        reuse.count = recorder.getCount(ord);
      }

      public ByCountComparable createComparable(int ord) {
        ByCountComparable result = new ByCountComparable();
        result.ord = ord;
        result.count = recorder.getCount(ord);
        return result;
      }
    };
  }

  /** Used for {@link #byCount} result. */
  public static class ByCountComparable implements Comparable<ByCountComparable> {
    private ByCountComparable() {}

    private int count;
    private int ord;

    @Override
    public int compareTo(ByCountComparable o) {
      int cmp = Integer.compare(count, o.count);
      if (cmp == 0) {
        cmp = Integer.compare(o.ord, ord);
      }
      return cmp;
    }
  }

  /**
   * {@link ComparableSupplier} to sort ordinals by long aggregation (descending) with tie-break by
   * count (descending) or by ordinal (ascending) using provided {@link CountFacetRecorder} and
   * {@link LongAggregationsFacetRecorder}.
   */
  public static ComparableSupplier<ByAggregatedValueComparable> byAggregatedValue(
      CountFacetRecorder countRecorder,
      LongAggregationsFacetRecorder longAggregationsFacetRecorder,
      int aggregationId) {
    return new ComparableSupplier<>() {
      public void reuseComparable(int ord, ByAggregatedValueComparable reuse) {
        reuse.ord = ord;
        reuse.secondaryRank = countRecorder.getCount(ord);
        reuse.primaryRank = longAggregationsFacetRecorder.getRecordedValue(ord, aggregationId);
      }

      public ByAggregatedValueComparable createComparable(int ord) {
        ByAggregatedValueComparable result = new ByAggregatedValueComparable();
        reuseComparable(ord, result);
        return result;
      }
    };
  }

  /** Used for {@link #byAggregatedValue} result. */
  public static class ByAggregatedValueComparable
      implements Comparable<ByAggregatedValueComparable> {
    private ByAggregatedValueComparable() {}

    private int ord;
    private int secondaryRank;
    private long primaryRank;

    @Override
    public int compareTo(ByAggregatedValueComparable o) {
      int cmp = Long.compare(primaryRank, o.primaryRank);
      if (cmp == 0) {
        cmp = Integer.compare(secondaryRank, o.secondaryRank);
        if (cmp == 0) {
          cmp = Integer.compare(o.ord, ord);
        }
      }
      return cmp;
    }
  }

  /**
   * {@link ComparableSupplier} to sort ordinals by long value from {@link LongValueFacetCutter}
   * (descending).
   */
  public static ComparableSupplier<ByLongValueComparable> byLongValue(
      LongValueFacetCutter longValueFacetCutter) {
    return new ComparableSupplier<>() {
      public void reuseComparable(int ord, ByLongValueComparable reuse) {
        reuse.value = longValueFacetCutter.getValue(ord);
      }

      public ByLongValueComparable createComparable(int ord) {
        ByLongValueComparable result = new ByLongValueComparable();
        result.value = longValueFacetCutter.getValue(ord);
        return result;
      }
    };
  }

  /** Used for {@link #byLongValue} result. */
  public static final class ByLongValueComparable implements Comparable<ByLongValueComparable> {
    private ByLongValueComparable() {}

    private long value;

    @Override
    public int compareTo(ByLongValueComparable o) {
      return Long.compare(o.value, value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ByLongValueComparable other) {
        return other.value == value;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  /**
   * {@link ComparableSupplier} to sort ordinals by count (descending) from {@link
   * CountFacetRecorder} with tie-break by long value (ascending) from {@link LongValueFacetCutter}.
   */
  public static ComparableSupplier<ByCountAndLongValueComparable> byCount(
      CountFacetRecorder countFacetRecorder, LongValueFacetCutter longValueFacetCutter) {
    return new ComparableSupplier<>() {
      public void reuseComparable(int ord, ByCountAndLongValueComparable reuse) {
        reuse.value = longValueFacetCutter.getValue(ord);
        reuse.count = countFacetRecorder.getCount(ord);
      }

      public ByCountAndLongValueComparable createComparable(int ord) {
        ByCountAndLongValueComparable result = new ByCountAndLongValueComparable();
        reuseComparable(ord, result);
        return result;
      }
    };
  }

  /** Used for {@link #byCount(CountFacetRecorder, LongValueFacetCutter)} result. */
  public static class ByCountAndLongValueComparable
      implements Comparable<ByCountAndLongValueComparable> {
    private ByCountAndLongValueComparable() {}

    private int count;
    private long value;

    @Override
    public int compareTo(ByCountAndLongValueComparable o) {
      int cmp = Integer.compare(count, o.count);
      if (cmp == 0) {
        cmp = Long.compare(o.value, value);
      }
      return cmp;
    }
  }

  /**
   * Sort array of ordinals.
   *
   * <p>To get top-n ordinals use {@link
   * org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator} instead.
   *
   * @param ordinals array of ordinals to sort
   * @param comparableSupplier defines sort order
   */
  public static <T extends Comparable<T>> void sort(
      int[] ordinals, ComparableSupplier<T> comparableSupplier) throws IOException {
    List<T> comparables = new ArrayList<>(ordinals.length);
    for (int i = 0; i < ordinals.length; i++) {
      comparables.add(comparableSupplier.createComparable(ordinals[i]));
    }
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int tmp = ordinals[i];
        ordinals[i] = ordinals[j];
        ordinals[j] = tmp;
        Collections.swap(comparables, i, j);
      }

      @Override
      protected int compare(int i, int j) {
        return comparables.get(j).compareTo(comparables.get(i));
      }
    }.sort(0, ordinals.length);
  }
}
