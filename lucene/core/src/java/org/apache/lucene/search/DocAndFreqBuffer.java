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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntsRef;

/**
 * Wrapper around parallel arrays storing doc IDs and their corresponding frequencies.
 *
 * @lucene.internal
 */
public final class DocAndFreqBuffer {

  /** Doc IDs */
  public int[] docs = IntsRef.EMPTY_INTS;

  /** Frequencies */
  public int[] freqs = IntsRef.EMPTY_INTS;

  /** Number of valid entries in the doc ID and frequency arrays. */
  public int size;

  /** Sole constructor. */
  public DocAndFreqBuffer() {}

  /** Grow both arrays to ensure that they can store at least the given number of entries. */
  public void growNoCopy(int minSize) {
    if (docs.length < minSize) {
      docs = ArrayUtil.growNoCopy(docs, minSize);
      freqs = new int[docs.length];
    }
  }

  /** Remove entries from this buffer if their bit is unset in the given {@link Bits}. */
  public void apply(Bits liveDocs) {
    int newSize = 0;
    for (int i = 0; i < size; ++i) {
      if (liveDocs.get(docs[i])) {
        docs[newSize] = docs[i];
        freqs[newSize] = freqs[i];
        newSize++;
      }
    }
    this.size = newSize;
  }
}
