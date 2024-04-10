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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A doc id set based on sorted int array.
 *
 * @lucene.internal
 */
public final class IntArrayDocIdSet extends DocIdSet {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(IntArrayDocIdSet.class);

  private final int[] docs;
  private final int length;

  /**
   * Build an IntArrayDocIdSet by an int array and len.
   *
   * @param docs A docs array whose length need to be greater than the param len. It needs to be
   *     sorted from 0(inclusive) to the len(exclusive), and the len-th doc in docs need to be
   *     {@link DocIdSetIterator#NO_MORE_DOCS}.
   * @param len The valid docs length in array.
   */
  public IntArrayDocIdSet(int[] docs, int len) {
    if (docs[len] != DocIdSetIterator.NO_MORE_DOCS) {
      throw new IllegalArgumentException();
    }
    assert assertArraySorted(docs, len)
        : "IntArrayDocIdSet need docs to be sorted"
            + Arrays.toString(ArrayUtil.copyOfSubArray(docs, 0, len));
    this.docs = docs;
    this.length = len;
  }

  private static boolean assertArraySorted(int[] docs, int length) {
    for (int i = 1; i < length; i++) {
      if (docs[i] < docs[i - 1]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(docs);
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    return new IntArrayDocIdSetIterator(docs, length);
  }

  static class IntArrayDocIdSetIterator extends DocIdSetIterator {

    private final int[] docs;
    private final int length;
    private int i = 0;
    private int doc = -1;

    IntArrayDocIdSetIterator(int[] docs, int length) {
      this.docs = docs;
      this.length = length;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return doc = docs[i++];
    }

    @Override
    public int advance(int target) throws IOException {
      int bound = 1;
      // given that we use this for small arrays only, this is very unlikely to overflow
      while (i + bound < length && docs[i + bound] < target) {
        bound *= 2;
      }
      i = Arrays.binarySearch(docs, i + bound / 2, Math.min(i + bound + 1, length), target);
      if (i < 0) {
        i = -1 - i;
      }
      return doc = docs[i++];
    }

    @Override
    public long cost() {
      return length;
    }
  }
}
