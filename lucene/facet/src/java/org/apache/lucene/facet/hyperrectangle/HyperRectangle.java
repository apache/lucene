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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.ArrayUtil;

/**
 * Holds the label, the number of dims, and the point pairs for a HyperRectangle
 *
 * @lucene.experimental
 */
public abstract class HyperRectangle {
  /** Label that identifies this range. */
  public final String label;

  /** How many dimensions this hyper rectangle has (IE: a regular rectangle would have dims=2) */
  public final int dims;

  private final ArrayUtil.ByteArrayComparator byteComparator =
      ArrayUtil.getUnsignedComparator(Long.BYTES);

  private final byte[] lowerPoints;
  private final byte[] upperPoints;

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

    this.lowerPoints =
        LongPoint.pack(Arrays.stream(pairs).mapToLong(pair -> pair.min).toArray()).bytes;
    this.upperPoints =
        LongPoint.pack(Arrays.stream(pairs).mapToLong(pair -> pair.max).toArray()).bytes;
  }

  /**
   * Checked a long packed value against this HyperRectangle. If you indexed a field with {@link
   * org.apache.lucene.document.LongPointDocValuesField} or {@link
   * org.apache.lucene.document.DoublePointDocValuesField}, those field values will be able to be
   * passed directly into this method.
   *
   * @param packedValue a byte array representing a long value
   * @return whether the packed long point intersects with this HyperRectangle
   */
  public final boolean matches(byte[] packedValue) {
    assert packedValue.length / Long.BYTES == dims
        : "Point dimension (dim="
            + packedValue.length / Long.BYTES
            + ") is incompatible with hyper rectangle dimension (dim="
            + dims
            + ")";
    for (int dim = 0; dim < dims; dim++) {
      int offset = dim * Long.BYTES;
      if (byteComparator.compare(packedValue, offset, lowerPoints, offset) < 0) {
        // Doc's value is too low, in this dimension
        return false;
      }
      if (byteComparator.compare(packedValue, offset, upperPoints, offset) > 0) {
        // Doc's value is too low, in this dimension
        return false;
      }
    }
    return true;
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
  }
}
