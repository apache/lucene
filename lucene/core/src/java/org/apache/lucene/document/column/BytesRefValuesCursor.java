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

import org.apache.lucene.util.BytesRef;

/**
 * A values cursor over a dense {@link BinaryColumn}. The cursor produces exactly {@link #size()}
 * values for consecutive batch-local doc-ids starting at 0, one per call to {@link #nextValue()}.
 *
 * <p>Implementations must throw an exception if {@link #nextValue()} is called more than {@link
 * #size()} times. The returned {@link BytesRef} is valid only until the next call to {@link
 * #nextValue()}.
 *
 * <p>Combined consumption across {@link #nextValue()} and {@link #fillPackedPoints} must not exceed
 * {@link #size()}.
 *
 * @lucene.experimental
 */
public abstract class BytesRefValuesCursor {

  private final int size;

  /**
   * Creates a cursor that will produce exactly {@code size} values, one per batch-local doc-id in
   * {@code [0, size)}. {@code size} is fixed for the cursor's lifetime and must equal the dense
   * column's {@code numDocs}.
   *
   * <p>Lucene's internal indexing paths will not consume past {@code size} across {@link
   * #nextValue()} and {@link #fillPackedPoints}. Defensive throws on overrun are still encouraged
   * to catch misuse from external callers.
   */
  protected BytesRefValuesCursor(int size) {
    this.size = size;
  }

  /** Total number of values this cursor will produce. */
  public final int size() {
    return size;
  }

  /**
   * Returns the next {@link BytesRef} value. Must not be called more than {@link #size()} times.
   */
  public abstract BytesRef nextValue();

  /**
   * Bulk-fill {@code length} fixed-width packed point records into {@code dst} starting at byte
   * {@code offset}, advancing the cursor by {@code length}. Each value must be exactly {@code
   * width} bytes and is passed through unchanged (no sortable encoding). Combined consumption
   * across {@link #nextValue()} and this method must not exceed {@link #size()}.
   *
   * <p>The default implementation calls {@link #nextValue()} per value and validates each length.
   * Override when the backing data is optimizable (e.g. a contiguous packed array); such overrides
   * take responsibility for the width contract.
   */
  public void fillPackedPoints(byte[] dst, int offset, int length, int width) {
    for (int i = 0; i < length; i++) {
      BytesRef v = nextValue();
      if (v.length != width) {
        throw new IllegalArgumentException(
            "dense point value has length=" + v.length + " but should be " + width);
      }
      System.arraycopy(v.bytes, v.offset, dst, offset + i * width, width);
    }
  }
}
