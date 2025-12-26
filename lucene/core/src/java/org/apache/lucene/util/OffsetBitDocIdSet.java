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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Wrapper for partition-relative bitsets that offsets doc IDs back to absolute values when
 * iterating.
 *
 * <p>This is used for partition-aware queries where a small bitset stores docs using
 * partition-relative indices (0 to partitionSize-1), but the iterator must return absolute doc IDs
 * (minDocId to maxDocId-1).
 *
 * @lucene.internal
 */
final class OffsetBitDocIdSet extends DocIdSet {
  private final BitDocIdSet delegate;
  private final int offset;

  /**
   * Creates an offset wrapper around a BitDocIdSet.
   *
   * @param bitSet the partition-relative bitset
   * @param cost the cost estimate
   * @param offset the value to add to convert relative indices to absolute doc IDs (typically
   *     minDocId)
   */
  OffsetBitDocIdSet(FixedBitSet bitSet, long cost, int offset) {
    this.delegate = new BitDocIdSet(bitSet, cost);
    this.offset = offset;
  }

  @Override
  public DocIdSetIterator iterator() {
    DocIdSetIterator delegateIterator = delegate.iterator();
    if (delegateIterator == null) {
      return null;
    }
    return new OffsetDocIdSetIterator(delegateIterator, offset);
  }

  @Override
  public long ramBytesUsed() {
    return delegate.ramBytesUsed();
  }
}
