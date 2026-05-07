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

  /** Sole constructor. */
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
  public void fill(long[] dst, int offset, int length) {
    for (int i = 0; i < length; i++) {
      dst[offset + i] = nextLong();
    }
  }
}
