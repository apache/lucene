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
package org.apache.lucene.facet.facetset;

import org.apache.lucene.util.NumericUtils;

/**
 * Defines a single range in a {@link FacetSet} dimension.
 *
 * @lucene.experimental
 */
public class DimRange {
  /** Inclusive min */
  public final long min;

  /** Inclusive max */
  public final long max;

  /**
   * Creates a LongRange.
   *
   * @param min inclusive min value in range
   * @param max inclusive max value in range
   */
  public DimRange(long min, long max) {
    this.min = min;
    this.max = max;
  }

  /**
   * Creates a {@link DimRange} for the given min and max long values. This method is also suitable
   * for int values.
   */
  public static DimRange fromLongs(long min, boolean minInclusive, long max, boolean maxInclusive) {
    if (!minInclusive) {
      if (min != Long.MAX_VALUE) {
        min++;
      } else {
        throw new IllegalArgumentException("Invalid min input: " + min);
      }
    }

    if (!maxInclusive) {
      if (max != Long.MIN_VALUE) {
        max--;
      } else {
        throw new IllegalArgumentException("Invalid max input: " + max);
      }
    }

    if (min > max) {
      throw new IllegalArgumentException(
          "Minimum cannot be greater than maximum, max=" + max + ", min=" + min);
    }

    return new DimRange(min, max);
  }

  /** Creates a {@link DimRange} for the given min and max double values. */
  public static DimRange fromDoubles(
      double min, boolean minInclusive, double max, boolean maxInclusive) {
    if (Double.isNaN(min)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      min = Math.nextUp(min);
    }

    if (Double.isNaN(max)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      max = Math.nextDown(max);
    }

    if (min > max) {
      throw new IllegalArgumentException("Minimum cannot be greater than maximum");
    }
    return new DimRange(
        NumericUtils.doubleToSortableLong(min), NumericUtils.doubleToSortableLong(max));
  }

  /** Creates a {@link DimRange} for the given min and max float values. */
  public static DimRange fromFloats(
      float min, boolean minInclusive, float max, boolean maxInclusive) {
    if (Float.isNaN(min)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      min = Math.nextUp(min);
    }

    if (Float.isNaN(max)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      max = Math.nextDown(max);
    }

    if (min > max) {
      throw new IllegalArgumentException("Minimum cannot be greater than maximum");
    }
    return new DimRange(NumericUtils.floatToSortableInt(min), NumericUtils.floatToSortableInt(max));
  }
}
