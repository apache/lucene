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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.NumericUtils;

/**
 * Utility class for retrieving global numeric field statistics (min/max values, document counts)
 * across all segments of an index. This class abstracts over {@link PointValues} and {@link
 * DocValuesSkipper}, probing {@link PointValues} first and falling back to {@link
 * DocValuesSkipper}.
 *
 * @lucene.experimental
 */
public final class NumericFieldStats {

  private NumericFieldStats() {}

  /**
   * Holds the global minimum and maximum values for a numeric field.
   *
   * @param min the global minimum value
   * @param max the global maximum value
   */
  public record MinMax(long min, long max) {}

  /**
   * Returns the global minimum and maximum values for the given numeric field across all segments.
   * Probes {@link PointValues} first; if unavailable, falls back to {@link DocValuesSkipper}.
   *
   * @param searcher the {@link IndexSearcher} to query
   * @param field the name of the numeric field
   * @return a {@link MinMax} containing the global min and max, or {@code null} if neither {@link
   *     PointValues} nor {@link DocValuesSkipper} are available for the field
   * @throws IOException if an I/O error occurs
   */
  public static MinMax globalMinMax(IndexSearcher searcher, String field) throws IOException {
    final MinMax result = globalMinMaxFromPoints(searcher.getIndexReader(), field);
    if (result != null) {
      return result;
    }
    return globalMinMaxFromSkipper(searcher, field);
  }

  /**
   * Returns the global document count for the given numeric field across all segments. Probes
   * {@link PointValues} first; if unavailable, falls back to {@link DocValuesSkipper}.
   *
   * @param searcher the {@link IndexSearcher} to query
   * @param field the name of the numeric field
   * @return the total number of documents containing the field, or {@code 0} if neither {@link
   *     PointValues} nor {@link DocValuesSkipper} are available
   * @throws IOException if an I/O error occurs
   */
  public static int globalDocCount(IndexSearcher searcher, String field) throws IOException {
    final int pointDocCount = PointValues.getDocCount(searcher.getIndexReader(), field);
    if (pointDocCount > 0) {
      return pointDocCount;
    }
    return DocValuesSkipper.globalDocCount(searcher, field);
  }

  private static MinMax globalMinMaxFromPoints(IndexReader reader, String field)
      throws IOException {
    final byte[] minPacked = PointValues.getMinPackedValue(reader, field);
    final byte[] maxPacked = PointValues.getMaxPackedValue(reader, field);
    if (minPacked == null || maxPacked == null) {
      return null;
    }
    return new MinMax(decodeLong(minPacked), decodeLong(maxPacked));
  }

  private static MinMax globalMinMaxFromSkipper(IndexSearcher searcher, String field)
      throws IOException {
    Long min = null;
    Long max = null;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      final LeafReader reader = ctx.reader();
      if (reader.getFieldInfos().fieldInfo(field) == null) {
        continue;
      }
      final DocValuesSkipper skipper = reader.getDocValuesSkipper(field);
      if (skipper == null) {
        return null;
      }
      if (min == null && max == null) {
        min = skipper.minValue();
        max = skipper.maxValue();
      } else {
        min = Math.min(min, skipper.minValue());
        max = Math.max(max, skipper.maxValue());
      }
    }
    if (min == null || max == null) {
      return null;
    }
    return new MinMax(min, max);
  }

  /**
   * Decodes a packed {@code byte[]} point value into a {@code long}. {@link PointValues} stores
   * numeric values as big-endian byte arrays with the sign bit flipped for sortable ordering.
   * {@code IntField} produces 4-byte arrays and {@code LongField} produces 8-byte arrays, so we
   * dispatch on length to call the appropriate {@link NumericUtils} decoder. The {@code int} case
   * widens to {@code long} via standard Java sign extension, which preserves the original value.
   *
   * <p>We return {@code long} unconditionally because the query layer already works with {@code
   * long} bounds (e.g. {@code SortedNumericDocValuesRangeQuery} stores its range as {@code long}
   * even for {@code IntField} queries). Callers that need the original {@code int} value can safely
   * narrow with {@code Math.toIntExact()}, which will never throw for values originating from an
   * {@code IntField}.
   */
  private static long decodeLong(byte[] packed) {
    return switch (packed.length) {
      case Integer.BYTES -> NumericUtils.sortableBytesToInt(packed, 0);
      case Long.BYTES -> NumericUtils.sortableBytesToLong(packed, 0);
      default ->
          throw new IllegalArgumentException(
              "Unsupported packed value length: "
                  + packed.length
                  + " (expected "
                  + Long.BYTES
                  + " or "
                  + Integer.BYTES
                  + ")");
    };
  }
}
