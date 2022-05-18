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
package org.apache.lucene.facet.hyperrectangle;

import java.util.Arrays;
import org.apache.lucene.util.NumericUtils;

/** Stores a hyper rectangle as an array of DoubleRangePairs */
public class DoubleHyperRectangle extends HyperRectangle {

  /** Creates DoubleHyperRectangle */
  public DoubleHyperRectangle(String label, DoubleRangePair... pairs) {
    super(label, convertToLongRangePairs(pairs));
  }

  private static LongRangePair[] convertToLongRangePairs(DoubleRangePair... pairs) {
    if (pairs == null || pairs.length == 0) {
      throw new IllegalArgumentException("Pairs cannot be null or empty");
    }
    return Arrays.stream(pairs).map(DoubleRangePair::toLongRangePair).toArray(LongRangePair[]::new);
  }

  /** Defines a single range in a DoubleHyperRectangle */
  public static class DoubleRangePair {
    /** Inclusive min */
    public final double min;

    /** Inclusive max */
    public final double max;

    /**
     * Creates a DoubleRangePair, very similar to the constructor of {@link
     * org.apache.lucene.facet.range.DoubleRange}
     *
     * @param minIn Min value of pair
     * @param minInclusive If minIn is inclusive
     * @param maxIn Max value of pair
     * @param maxInclusive If maxIn is inclusive
     */
    public DoubleRangePair(double minIn, boolean minInclusive, double maxIn, boolean maxInclusive) {
      if (Double.isNaN(minIn) || Double.isNaN(maxIn)) {
        throw new IllegalArgumentException(
            "min and max cannot be NaN: min=" + minIn + ", max=" + maxIn);
      }

      if (!minInclusive) {
        minIn = Math.nextUp(minIn);
      }

      if (!maxInclusive) {
        maxIn = Math.nextDown(maxIn);
      }

      if (minIn > maxIn) {
        throw new IllegalArgumentException(
            "Minimum cannot be greater than maximum, max=" + maxIn + ", min=" + minIn);
      }

      this.min = minIn;
      this.max = maxIn;
    }

    /**
     * Converts this to a LongRangePair with sortable long equivalents
     *
     * @return A LongRangePair equivalent of this object
     */
    public LongRangePair toLongRangePair() {
      long longMin = NumericUtils.doubleToSortableLong(min);
      long longMax = NumericUtils.doubleToSortableLong(max);
      return new LongRangePair(longMin, true, longMax, true);
    }
  }
}
