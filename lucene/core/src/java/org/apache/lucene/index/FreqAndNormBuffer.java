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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;

/**
 * Wrapper around parallel arrays storing term frequencies and length normalization factors.
 *
 * @lucene.experimental
 */
public final class FreqAndNormBuffer {

  /** Term frequencies */
  public int[] freqs = IntsRef.EMPTY_INTS;

  /** Length normalization factors */
  public long[] norms = LongsRef.EMPTY_LONGS;

  /** Number of valid entries in the doc ID and float-valued feature arrays. */
  public int size;

  /** Sole constructor. */
  public FreqAndNormBuffer() {}

  /** Grow both arrays to ensure that they can store at least the given number of entries. */
  public void growNoCopy(int minSize) {
    if (freqs.length < minSize) {
      freqs = new int[ArrayUtil.oversize(minSize, Integer.BYTES)];
      norms = new long[freqs.length];
    }
  }

  /**
   * Add the given pair of term frequency and norm at the end of this buffer, growing underlying
   * arrays if necessary.
   */
  public void add(int freq, long norm) {
    if (freqs.length == size) {
      freqs = ArrayUtil.grow(freqs, size + 1);
      norms = ArrayUtil.growExact(norms, freqs.length);
    }
    freqs[size] = freq;
    norms[size] = norm;
    size++;
  }
}
