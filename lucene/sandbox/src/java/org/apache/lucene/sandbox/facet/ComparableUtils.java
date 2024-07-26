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
package org.apache.lucene.sandbox.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.sandbox.facet.cutters.LongValueFacetCutter;
import org.apache.lucene.sandbox.facet.ordinals.OrdToComparable;
import org.apache.lucene.sandbox.facet.ordinals.OrdinalGetter;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * Collection of static methods to provide most common comparables for sandbox faceting. You can
 * also use it as an example for creating your own {@link OrdToComparable} to enable custom facets
 * top-n and sorting.
 */
public final class ComparableUtils {
  private ComparableUtils() {}

  private static class SkeletalOrdGetter implements OrdinalGetter {
    int ord;

    @Override
    public int getOrd() {
      return ord;
    }
  }

  /** {@link OrdToComparable} that can be used to sort by ords (ascending). */
  public static OrdToComparable<ComparableOrd> ordToComparableOrd() {
    return new OrdToComparable<>() {
      @Override
      public ComparableOrd getComparable(int ord, ComparableOrd reuse) {
        if (reuse == null) {
          reuse = new ComparableOrd();
        }
        reuse.ord = ord;
        return reuse;
      }
    };
  }

  /** Used for {@link #ordToComparableOrd} result. */
  public static class ComparableOrd extends SkeletalOrdGetter implements Comparable<ComparableOrd> {
    @Override
    public int compareTo(ComparableOrd o) {
      return Integer.compare(o.ord, ord);
    }
  }

  /**
   * {@link OrdToComparable} that can be used to sort ordinals by count (descending) with ord as a
   * tie-break (ascending) using provided {@link CountFacetRecorder}.
   */
  public static OrdToComparable<ComparableIntOrd> ordToComparableCountOrd(
      CountFacetRecorder recorder) {
    return new OrdToComparable<>() {
      @Override
      public ComparableIntOrd getComparable(int ord, ComparableIntOrd reuse) {
        if (reuse == null) {
          reuse = new ComparableIntOrd();
        }
        reuse.ord = ord;
        reuse.rank = recorder.getCount(ord);
        return reuse;
      }
    };
  }

  /** Used for {@link #ordToComparableCountOrd} result. */
  public static class ComparableIntOrd extends SkeletalOrdGetter
      implements Comparable<ComparableIntOrd> {
    private ComparableIntOrd() {}

    private int rank;

    @Override
    public int compareTo(ComparableIntOrd o) {
      int cmp = Integer.compare(rank, o.rank);
      if (cmp == 0) {
        cmp = Integer.compare(o.ord, ord);
      }
      return cmp;
    }
  }

  /**
   * {@link OrdToComparable} to sort ordinals by long aggregation (descending) with tie-break by
   * count (descending) with ordinal as a tie-break (ascending) using provided {@link
   * CountFacetRecorder} and {@link LongAggregationsFacetRecorder}.
   */
  public static OrdToComparable<ComparableLongIntOrd> ordToComparableRankCountOrd(
      CountFacetRecorder countRecorder,
      LongAggregationsFacetRecorder longAggregationsFacetRecorder,
      int aggregationId) {
    return new OrdToComparable<>() {
      @Override
      public ComparableLongIntOrd getComparable(int ord, ComparableLongIntOrd reuse) {
        if (reuse == null) {
          reuse = new ComparableLongIntOrd();
        }
        reuse.ord = ord;
        reuse.secondaryRank = countRecorder.getCount(ord);
        reuse.primaryRank = longAggregationsFacetRecorder.getRecordedValue(ord, aggregationId);
        return reuse;
      }
    };
  }

  /** Used for {@link #ordToComparableRankCountOrd} result. */
  public static class ComparableLongIntOrd extends SkeletalOrdGetter
      implements Comparable<ComparableLongIntOrd> {
    private ComparableLongIntOrd() {}

    private int secondaryRank;
    private long primaryRank;

    @Override
    public int compareTo(ComparableLongIntOrd o) {
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
   * {@link OrdToComparable} to sort ordinals by long value (descending) from {@link
   * LongValueFacetCutter}.
   */
  public static OrdToComparable<ComparableLong> ordToComparableValue(
      LongValueFacetCutter longValueFacetCutter) {
    return new OrdToComparable<>() {
      public ComparableLong getComparable(int ord, ComparableLong reuse) {
        if (reuse == null) {
          reuse = new ComparableLong();
        }
        reuse.ord = ord;
        reuse.value = longValueFacetCutter.getValue(ord);
        return reuse;
      }
    };
  }

  /** Used for {@link #ordToComparableValue} result. */
  public static final class ComparableLong extends SkeletalOrdGetter
      implements Comparable<ComparableLong> {
    private ComparableLong() {}

    private long value;

    @Override
    public int compareTo(ComparableLong o) {
      return Long.compare(o.value, value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ComparableLong other) {
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
   * {@link OrdToComparable} to sort ordinals by count (descending) from {@link CountFacetRecorder}
   * with tie-break by long value (ascending) from {@link LongValueFacetCutter}.
   */
  public static OrdToComparable<ComparableCountValue> ordToComparableCountValue(
      CountFacetRecorder countFacetRecorder, LongValueFacetCutter longValueFacetCutter) {
    return new OrdToComparable<>() {
      public ComparableCountValue getComparable(int ord, ComparableCountValue reuse) {
        if (reuse == null) {
          reuse = new ComparableCountValue();
        }
        reuse.ord = ord;
        reuse.value = longValueFacetCutter.getValue(ord);
        reuse.count = countFacetRecorder.getCount(ord);
        return reuse;
      }
    };
  }

  /** Used for {@link #ordToComparableCountValue} result. */
  public static class ComparableCountValue extends SkeletalOrdGetter
      implements Comparable<ComparableCountValue> {
    private ComparableCountValue() {}

    private int count;
    private long value;

    @Override
    public int compareTo(ComparableCountValue o) {
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
   * org.apache.lucene.sandbox.facet.ordinals.TopnOrdinalIterator} instead.
   *
   * @param ordinals array of ordinals to sort
   * @param ordToComparable defines sort order
   */
  public static <T extends Comparable<T>> void sort(
      int[] ordinals, OrdToComparable<T> ordToComparable) throws IOException {
    List<T> comparables = new ArrayList<>(ordinals.length);
    for (int i = 0; i < ordinals.length; i++) {
      comparables.add(ordToComparable.getComparable(ordinals[i], null));
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
