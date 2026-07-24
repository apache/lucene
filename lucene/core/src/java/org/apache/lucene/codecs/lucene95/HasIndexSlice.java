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
package org.apache.lucene.codecs.lucene95;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

/**
 * Implementors can return the IndexInput from which their values are read. For use by vector
 * quantizers.
 */
public interface HasIndexSlice {

  /** Returns an IndexInput from which to read this instance's values, or null if not available. */
  IndexInput getSlice();

  /**
   * Shared helper for off-heap vector value implementations that store fixed-size records on a
   * slice: issue a read-ahead {@link IndexInput#prefetch} for each of the given vector ordinals.
   *
   * <p>Zero or one ordinal is intentionally left to fault in on the immediately-following read,
   * since prefetching a record that is about to be read offers no overlap.
   *
   * @param slice the slice the records live on (one record every {@code byteSize} bytes); a {@code
   *     null} slice (e.g. an empty values instance) is tolerated and prefetches nothing
   * @param byteSize the on-disk size of one record
   * @param ordsToPrefetch ordinals to prefetch (only the first {@code numOrds} are used)
   * @param numOrds number of valid entries in {@code ordsToPrefetch}
   */
  static void prefetchOrdinals(IndexInput slice, int byteSize, int[] ordsToPrefetch, int numOrds)
      throws IOException {
    // n <= 1 also covers the empty (n == 0) and null-ordinals cases; nothing is prefetched when
    // there is no slice or at most one record (which faults in on the immediately-following read).
    final int n = (ordsToPrefetch == null) ? 0 : Math.min(numOrds, ordsToPrefetch.length);
    if (slice == null || n <= 1) {
      return;
    }
    for (int i = 0; i < n; i++) {
      final long offset = (long) ordsToPrefetch[i] * byteSize;
      slice.prefetch(offset, byteSize);
    }
  }
}
