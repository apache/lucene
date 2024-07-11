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

import org.apache.lucene.sandbox.facet.abstracts.GetOrd;
import org.apache.lucene.sandbox.facet.abstracts.OrdToComparable;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;

/**
 * Collection of static methods to provide most common comparables for sandbox faceting. You can
 * also use it as an example for creating your own {@link OrdToComparable} to enable custom facets
 * sorting.
 */
public class ComparableUtils {
  private ComparableUtils() {}

  private static class SkeletalGetOrd implements GetOrd {
    int ord;

    @Override
    public int getOrd() {
      return ord;
    }
  }

  /** Result of */
  public static class OrdComparable extends SkeletalGetOrd implements Comparable<OrdComparable> {
    @Override
    public int compareTo(OrdComparable o) {
      return Integer.compare(o.ord, ord);
    }
  }

  /**
   * To sort facet ords by count (descending) with ord as a tie-break (ascending) using provided
   * {@link CountFacetRecorder}.
   */
  public static OrdToComparable<OrdComparable> ordToComparable() {
    return new OrdToComparable<>() {
      @Override
      public OrdComparable getComparable(int ord, OrdComparable reuse) {
        if (reuse == null) {
          reuse = new OrdComparable();
        }
        reuse.ord = ord;
        return reuse;
      }
    };
  }

  /** Result of {@link #countOrdToComparable} method below */
  public static class IntOrdComparable extends SkeletalGetOrd
      implements Comparable<IntOrdComparable> {
    private IntOrdComparable() {}
    ;

    private int rank;

    @Override
    public int compareTo(IntOrdComparable o) {
      int cmp = Integer.compare(rank, o.rank);
      if (cmp == 0) {
        cmp = Integer.compare(o.ord, ord);
      }
      return cmp;
    }
  }

  /**
   * To sort facet ords by count (descending) with ord as a tie-break (ascending) using provided
   * {@link CountFacetRecorder}.
   */
  public static OrdToComparable<IntOrdComparable> countOrdToComparable(
      CountFacetRecorder recorder) {
    return new OrdToComparable<>() {
      @Override
      public IntOrdComparable getComparable(int ord, IntOrdComparable reuse) {
        if (reuse == null) {
          reuse = new IntOrdComparable();
        }
        reuse.ord = ord;
        reuse.rank = recorder.getCount(ord);
        return reuse;
      }
    };
  }

  /** Result of {@link #rankCountOrdToComparable} methods below */
  public static class LongIntOrdComparable extends SkeletalGetOrd
      implements Comparable<LongIntOrdComparable> {
    private LongIntOrdComparable() {}
    ;

    private int secondaryRank;
    private long primaryRank;

    @Override
    public int compareTo(LongIntOrdComparable o) {
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
   * To sort facet ords by long aggregation (descending) with tie-break by count (descending) with
   * ord as a tie-break (ascending) using provided {@link CountFacetRecorder} and {@link
   * LongAggregationsFacetRecorder}.
   */
  public static OrdToComparable<LongIntOrdComparable> rankCountOrdToComparable(
      CountFacetRecorder countRecorder,
      LongAggregationsFacetRecorder longAggregationsFacetRecorder,
      int aggregationId) {
    return new OrdToComparable<>() {
      @Override
      public LongIntOrdComparable getComparable(int ord, LongIntOrdComparable reuse) {
        if (reuse == null) {
          reuse = new LongIntOrdComparable();
        }
        reuse.ord = ord;
        reuse.secondaryRank = countRecorder.getCount(ord);
        reuse.primaryRank = longAggregationsFacetRecorder.getRecordedValue(ord, aggregationId);
        return reuse;
      }
    };
  }
}
