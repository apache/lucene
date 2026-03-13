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

/**
 * Utility class for retrieving global numeric field statistics from index metadata structures,
 * without accessing individual documents. It probes {@link PointValues} first and falls back to
 * {@link DocValuesSkipper}. Returns {@code null} when neither structure is available for the field.
 *
 * @lucene.experimental
 */
public final class NumericFieldStats {

  private NumericFieldStats() {}

  /**
   * Holds the global statistics for a numeric field.
   *
   * @param min the global minimum value
   * @param max the global maximum value
   * @param docCount the total number of documents containing the field
   */
  public record Stats(long min, long max, int docCount) {}

  /**
   * Returns the global statistics for the given numeric field across all segments. Probes {@link
   * PointValues} first; if unavailable, falls back to {@link DocValuesSkipper}.
   *
   * @param reader the {@link IndexReader} to query
   * @param field the name of the numeric field
   * @return a {@link Stats} containing the global min, max, and doc count, or {@code null} if
   *     neither {@link PointValues} nor {@link DocValuesSkipper} are available for the field
   * @throws IOException if an I/O error occurs
   */
  public static Stats getStats(IndexReader reader, String field) throws IOException {
    final Stats result = getStatsFromPoints(reader, field);
    if (result != null) {
      return result;
    }
    return getStatsFromSkipper(reader, field);
  }

  private static Stats getStatsFromPoints(IndexReader reader, String field) throws IOException {
    final byte[] minPacked = PointValues.getMinPackedValue(reader, field);
    final byte[] maxPacked = PointValues.getMaxPackedValue(reader, field);
    if (minPacked == null
        || maxPacked == null
        || minPacked.length > Long.BYTES
        || maxPacked.length > Long.BYTES) {
      return null;
    }
    final int docCount = PointValues.getDocCount(reader, field);
    return new Stats(decodeLong(minPacked), decodeLong(maxPacked), docCount);
  }

  private static Stats getStatsFromSkipper(IndexReader reader, String field) throws IOException {
    Long min = null;
    Long max = null;
    int docCount = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      final LeafReader leafReader = ctx.reader();
      if (leafReader.getFieldInfos().fieldInfo(field) == null) {
        continue;
      }
      final DocValuesSkipper skipper = leafReader.getDocValuesSkipper(field);
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
      docCount += skipper.docCount();
    }
    if (min == null || max == null) {
      return null;
    }
    return new Stats(min, max, docCount);
  }

  /**
   * Decodes a packed {@code byte[]} point value into a {@code long}. Handles any packed value
   * length from 1 to 8 bytes, covering {@code HalfFloatPoint} (2 bytes), {@code IntField} (4
   * bytes), {@code LongField} (8 bytes), and any other width that uses the standard sortable
   * encoding (big-endian with sign bit flipped).
   */
  private static long decodeLong(byte[] packed) {
    assert packed.length >= 1 && packed.length <= Long.BYTES;
    long result = (byte) (packed[0] ^ 0x80);
    for (int i = 1; i < packed.length; i++) {
      result = (result << 8) | (packed[i] & 0xFF);
    }
    return result;
  }
}
