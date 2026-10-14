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
package org.apache.lucene.document.column;

import org.apache.lucene.util.NumericUtils;

/**
 * A values cursor over a dense {@link LongColumn}. The cursor produces exactly {@link #size()}
 * values for consecutive batch-local doc-ids starting at 0, one per call to {@link #nextLong()}.
 *
 * <p>Implementations must throw an exception if {@link #nextLong()} is called more than {@link
 * #size()} times.
 *
 * @lucene.experimental
 */
public abstract class LongValuesCursor {

  private final int size;

  /**
   * Creates a cursor that will produce exactly {@code size} values, one per batch-local doc-id in
   * {@code [0, size)}. {@code size} is fixed for the cursor's lifetime and must equal the dense
   * column's {@code numDocs}.
   *
   * <p>Lucene's internal indexing paths will not consume past {@code size} across {@link
   * #nextLong()}, {@link #fillDocValues}, and the {@code fillPoints} variants. Defensive throws on
   * overrun are still encouraged to catch misuse from external callers.
   */
  protected LongValuesCursor(int size) {
    this.size = size;
  }

  /** Total number of values this cursor will produce. */
  public final int size() {
    return size;
  }

  /** Returns the next long value. Must not be called more than {@link #size()} times. */
  public abstract long nextLong();

  /**
   * Bulk-fill {@code length} values into {@code dst} starting at {@code offset}, advancing the
   * cursor by {@code length}. Combined {@link #nextLong()} and {@code fill} calls must not consume
   * more than {@link #size()} values; implementations must throw if they do.
   *
   * <p>The default implementation calls {@link #nextLong()} in a loop. Override to provide a more
   * efficient bulk fill (for example a {@link System#arraycopy} from a backing array).
   */
  public void fillDocValues(long[] dst, int offset, int length) {
    for (int i = 0; i < length; i++) {
      dst[offset + i] = nextLong();
    }
  }

  /**
   * Bulk-encode {@code length} values as 8-byte big-endian sortable long points (sign-flipped so
   * negatives sort before positives) into {@code dst} starting at byte {@code offset}, advancing
   * the cursor by {@code length}. Combined consumption across {@link #nextLong()}, {@link
   * #fillDocValues}, and the {@code fillPoints} variants must not exceed {@link #size()}.
   *
   * <p>Values are read in doc-values orientation: callers using this for {@link
   * org.apache.lucene.document.column.LongColumn.NumericKind#DOUBLE DOUBLE} columns are responsible
   * for ensuring the cursor's longs are already {@link
   * org.apache.lucene.util.NumericUtils#doubleToSortableLong sortable-long} encoded, which is the
   * documented {@link LongColumn} contract.
   *
   * <p>The default implementation calls {@link #nextLong()} per value. Override for a tight
   * indexable loop over a backing array.
   */
  public void fillLongPoints(byte[] dst, int offset, int length) {
    for (int i = 0; i < length; i++) {
      NumericUtils.longToSortableBytes(nextLong(), dst, offset + (i << 3));
    }
  }

  /**
   * Bulk-encode {@code length} values as 4-byte big-endian sortable int points (sign-flipped so
   * negatives sort before positives) into {@code dst} starting at byte {@code offset}, advancing
   * the cursor by {@code length}. Each value is narrowed via {@code (int) nextLong()}. Combined
   * consumption across {@link #nextLong()}, {@link #fillDocValues}, and the {@code fillPoints}
   * variants must not exceed {@link #size()}.
   *
   * <p>Values are read in doc-values orientation: callers using this for {@link
   * org.apache.lucene.document.column.LongColumn.NumericKind#FLOAT FLOAT} columns are responsible
   * for ensuring the cursor's longs are already {@link
   * org.apache.lucene.util.NumericUtils#floatToSortableInt sortable-int} encoded (in the low 32
   * bits), which is the documented {@link LongColumn} contract.
   *
   * <p>The default implementation calls {@link #nextLong()} per value. Override for a tight
   * indexable loop over a backing array.
   */
  public void fillIntPoints(byte[] dst, int offset, int length) {
    for (int i = 0; i < length; i++) {
      NumericUtils.intToSortableBytes((int) nextLong(), dst, offset + (i << 2));
    }
  }
}
