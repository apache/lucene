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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

/** Utility class to decode postings. */
public class PostingDecodingUtil {

  /** The wrapper {@link IndexInput}. */
  public final IndexInput in;

  /** Sole constructor, called by sub-classes. */
  protected PostingDecodingUtil(IndexInput in) {
    this.in = in;
  }

  /**
   * Core methods for decoding blocks of docs / freqs / positions / offsets.
   *
   * <ul>
   *   <li>Read {@code count} longs.
   *   <li>For all {@code i} &gt;= 0 so that {@code bShift - i * dec} &gt; 0, apply shift {@code
   *       bShift - i * dec} and store the result in {@code b} at offset {@code count * i}.
   *   <li>Apply mask {@code cMask} and store the result in {@code c} starting at offset {@code
   *       cIndex}.
   * </ul>
   */
  public void splitLongs(
      int count, long[] b, int bShift, int dec, long bMask, long[] c, int cIndex, long cMask)
      throws IOException {
    // Default implementation, which takes advantage of the C2 compiler's loop unrolling and
    // auto-vectorization.
    in.readLongs(c, cIndex, count);
    int maxIter = (bShift - 1) / dec;
    for (int i = 0; i < count; ++i) {
      for (int j = 0; j <= maxIter; ++j) {
        b[count * j + i] = (c[cIndex + i] >>> (bShift - j * dec)) & bMask;
      }
      c[cIndex + i] &= cMask;
    }
  }
}
