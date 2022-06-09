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

import java.util.Arrays;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.ArrayUtil;

/**
 * A {@link FacetSetMatcher} which considers a set as a match if all dimensions fall within the
 * given corresponding range.
 *
 * @lucene.experimental
 */
public class RangeFacetSetMatcher extends FacetSetMatcher {

  private final ArrayUtil.ByteArrayComparator byteComparator =
      ArrayUtil.getUnsignedComparator(Long.BYTES);

  private final byte[] lowerRanges;
  private final byte[] upperRanges;

  private final long[] lowerRangesLong;
  private final long[] upperRangesLong;

  /**
   * Constructs and instance to match facet sets with dimensions that fall within the given ranges.
   */
  public RangeFacetSetMatcher(String label, LongRange... dimRanges) {
    super(label, getDims(dimRanges));
    this.lowerRanges =
        LongPoint.pack(Arrays.stream(dimRanges).mapToLong(range -> range.min).toArray()).bytes;
    this.upperRanges =
        LongPoint.pack(Arrays.stream(dimRanges).mapToLong(range -> range.max).toArray()).bytes;
    this.lowerRangesLong = Arrays.stream(dimRanges).mapToLong(range -> range.min).toArray();
    this.upperRangesLong = Arrays.stream(dimRanges).mapToLong(range -> range.max).toArray();
  }

  @Override
  public boolean matches(byte[] packedValue, int start, int numDims) {
    assert numDims == dims
        : "Encoded dimensions (dims="
            + numDims
            + ") is incompatible with range dimensions (dims="
            + dims
            + ")";

    for (int dim = 0, valuesOffset = 0, packedOffset = start;
        dim < dims;
        dim++, valuesOffset += Long.BYTES, packedOffset += Long.BYTES) {
      if (byteComparator.compare(packedValue, packedOffset, lowerRanges, valuesOffset) < 0) {
        // Doc's value is too low in this dimension
        return false;
      }
      if (byteComparator.compare(packedValue, packedOffset, upperRanges, valuesOffset) > 0) {
        // Doc's value is too high in this dimension
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean matches(long[] dimValues, int numDims) {
    assert numDims == dims
        : "Encoded dimensions (dims="
            + numDims
            + ") is incompatible with range dimensions (dims="
            + dims
            + ")";

    for (int i = 0; i < dimValues.length; i++) {
      if (dimValues[i] < lowerRangesLong[i]) {
        // Doc's value is too low in this dimension
        return false;
      }
      if (dimValues[i] > upperRangesLong[i]) {
        // Doc's value is too high in this dimension
        return false;
      }
    }
    return true;
  }

  private static int getDims(LongRange... dimRanges) {
    if (dimRanges == null || dimRanges.length == 0) {
      throw new IllegalArgumentException("dimRanges cannot be null or empty");
    }
    return dimRanges.length;
  }

  /** Defines a single range in a FacetSet dimension. */
  public static class LongRange {
    /** Inclusive min */
    public final long min;

    /** Inclusive max */
    public final long max;

    /**
     * Creates a LongRange.
     *
     * @param min min value in range
     * @param minInclusive if min is inclusive
     * @param max max value in range
     * @param maxInclusive if max is inclusive
     */
    public LongRange(long min, boolean minInclusive, long max, boolean maxInclusive) {
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

      this.min = min;
      this.max = max;
    }
  }
}
