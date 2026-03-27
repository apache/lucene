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
import org.apache.lucene.util.NumericUtils;

/**
 * Utility to retrieve global min/max values of a numeric field across an IndexReader.
 *
 * <p>This method abstracts over different storage implementations used by Lucene:
 *
 * <ul>
 *   <li>BKD PointValues (IntPoint / LongPoint)
 *   <li>DocValuesSkipper (fast metadata when available)
 *   <li>NumericDocValues scan (correct fallback)
 * </ul>
 *
 * <p>Only single dimensional integral numeric fields are supported.
 *
 * <p>Returns {@code null} when:
 *
 * <ul>
 *   <li>field does not exist
 *   <li>field is floating point (float/double)
 *   <li>field is multi-dimensional
 *   <li>no segments contain values
 * </ul>
 */
public final class FieldMinMax {

  private FieldMinMax() {}

  /** Immutable holder for global minimum and maximum values. */
  public static final class MinMax {

    /** The minimum value across all documents. */
    public final long min;

    /** The maximum value across all documents. */
    public final long max;

    /**
     * Creates a new {@link MinMax} instance.
     *
     * @param min the minimum value
     * @param max the maximum value
     */
    public MinMax(long min, long max) {
      this.min = min;
      this.max = max;
    }
  }

  /** Returns global min/max or null if unavailable */
  public static MinMax get(IndexReader reader, String field) throws IOException {

    // ---- 1. Prefer PointValues (accurate index statistics) ----
    boolean found = false;
    long globalMin = Long.MAX_VALUE;
    long globalMax = Long.MIN_VALUE;

    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();

      PointValues values = leaf.getPointValues(field);
      if (values == null || values.getNumDimensions() != 1) {
        continue;
      }

      byte[] minPacked = values.getMinPackedValue();
      byte[] maxPacked = values.getMaxPackedValue();
      if (minPacked == null || maxPacked == null) {
        continue;
      }

      int bytes = values.getBytesPerDimension();
      Long min = decodeIntegral(minPacked, bytes);
      Long max = decodeIntegral(maxPacked, bytes);

      if (min != null && max != null) {
        found = true;
        globalMin = Math.min(globalMin, min);
        globalMax = Math.max(globalMax, max);
      }
    }

    if (found) {
      return new MinMax(globalMin, globalMax);
    }

    // ---- 2. Try DocValuesSkipper (fast metadata) ----
    long sMin = DocValuesSkipper.globalMinValue(reader, field);
    long sMax = DocValuesSkipper.globalMaxValue(reader, field);

    if (isValidSkipperRange(sMin, sMax)) {
      return new MinMax(sMin, sMax);
    }

    // ---- 3. Guaranteed fallback: scan NumericDocValues ----
    return scanNumericDocValues(reader, field);
  }

  /** Decode integral numeric point values only */
  private static Long decodeIntegral(byte[] packed, int bytesPerDim) {
    switch (bytesPerDim) {
      case Integer.BYTES:
        return (long) NumericUtils.sortableBytesToInt(packed, 0);
      case Long.BYTES:
        return NumericUtils.sortableBytesToLong(packed, 0);
      default:
        return null; // float/double unsupported
    }
  }

  /** Validate skipper sentinel semantics */
  private static boolean isValidSkipperRange(long min, long max) {
    if (min == Long.MAX_VALUE && max == Long.MIN_VALUE) return false;
    if (min == Long.MIN_VALUE && max == Long.MAX_VALUE) return false;
    return true;
  }

  /** Full scan fallback for NumericDocValues */
  private static MinMax scanNumericDocValues(IndexReader reader, String field) throws IOException {
    boolean found = false;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;

    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      NumericDocValues values = leaf.getNumericDocValues(field);
      if (values == null) continue;

      while (values.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
        long v = values.longValue();
        found = true;
        min = Math.min(min, v);
        max = Math.max(max, v);
      }
    }

    return found ? new MinMax(min, max) : null;
  }
}
