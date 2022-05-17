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

import org.apache.lucene.util.NumericUtils;

/** Stores a hyper rectangle as an array of DoubleRangePairs */
public class DoubleHyperRectangle extends HyperRectangle {

  /** Stores pair as LongRangePair */
  private final LongHyperRectangle.LongRangePair[] pairs;

  /** Created DoubleHyperRectangle */
  public DoubleHyperRectangle(String label, DoubleRangePair... pairs) {
    super(label, checkPairsAndGetDim(pairs));
    this.pairs = new LongHyperRectangle.LongRangePair[pairs.length];
    for (int dim = 0; dim < pairs.length; dim++) {
      long longMin = NumericUtils.doubleToSortableLong(pairs[dim].min);
      long longMax = NumericUtils.doubleToSortableLong(pairs[dim].max);
      this.pairs[dim] = new LongHyperRectangle.LongRangePair(longMin, true, longMax, true);
    }
  }

  @Override
  public LongHyperRectangle.LongRangePair getComparableDimRange(int dim) {
    return pairs[dim];
  }

  private static int checkPairsAndGetDim(DoubleRangePair... pairs) {
    if (pairs == null || pairs.length == 0) {
      throw new IllegalArgumentException("Pairs cannot be null or empty");
    }
    return pairs.length;
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
        // Why no Math.nextDown?
        maxIn = Math.nextAfter(maxIn, Double.NEGATIVE_INFINITY);
      }

      if (minIn > maxIn) {
        throw new IllegalArgumentException("Minimum cannot be greater than maximum");
      }

      this.min = minIn;
      this.max = maxIn;
    }
  }
}
