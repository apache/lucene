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

/** Holds the name and the number of dims for a HyperRectangle */
public abstract class HyperRectangle {
  /** Label that identifies this range. */
  public final String label;

  /** How many dimensions this hyper rectangle has (IE: a regular rectangle would have dims=2) */
  public final int dims;

  /** All subclasses should store pairs as comparable longs */
  protected final LongRangePair[] pairs;

  /** Sole constructor. */
  protected HyperRectangle(String label, LongRangePair... pairs) {
    if (label == null) {
      throw new IllegalArgumentException("label must not be null");
    }
    if (pairs == null || pairs.length == 0) {
      throw new IllegalArgumentException("Pairs cannot be null or empty");
    }
    this.label = label;
    this.dims = pairs.length;
    this.pairs = pairs;
  }

  /**
   * Returns comparable long range for a provided dim
   *
   * @param dim dimension of the request range
   * @return The comparable long version of the requested range
   */
  public LongRangePair getComparableDimRange(int dim) {
    return pairs[dim];
  }

  /** Defines a single range in a HyperRectangle */
  public static class LongRangePair {
    /** Inclusive min */
    public final long min;

    /** Inclusive max */
    public final long max;

    /**
     * Creates a LongRangePair, very similar to the constructor of {@link
     * org.apache.lucene.facet.range.LongRange}
     *
     * @param minIn Min value of pair
     * @param minInclusive If minIn is inclusive
     * @param maxIn Max value of pair
     * @param maxInclusive If maxIn is inclusive
     */
    public LongRangePair(long minIn, boolean minInclusive, long maxIn, boolean maxInclusive) {
      if (!minInclusive) {
        if (minIn != Long.MAX_VALUE) {
          minIn++;
        } else {
          throw new IllegalArgumentException("Invalid min input, min=" + minIn);
        }
      }

      if (!maxInclusive) {
        if (maxIn != Long.MIN_VALUE) {
          maxIn--;
        } else {
          throw new IllegalArgumentException("Invalid max input, max=" + maxIn);
        }
      }

      if (minIn > maxIn) {
        throw new IllegalArgumentException(
            "Minimum cannot be greater than maximum, max=" + maxIn + ", min=" + minIn);
      }

      this.min = minIn;
      this.max = maxIn;
    }

    /** True if this range accepts the provided value. */
    public boolean accept(long value) {
      return value >= min && value <= max;
    }
  }
}
