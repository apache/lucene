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

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TernaryLongHeap;

/**
 * {@link DocScoreHeap} that packs each hit into a single {@code long} — the score in the high 32
 * bits and {@code Integer.MAX_VALUE - doc} in the low 32 bits — so the whole "score ascending, then
 * doc descending" ordering is a single {@code long} comparison in a {@link TernaryLongHeap}. This
 * is the fast path used whenever the reader's doc-id space fits an {@code int}; the doc id passed
 * to {@link #updateTop} is therefore always below {@link Integer#MAX_VALUE}.
 *
 * @lucene.internal
 */
final class PackedDocScoreHeap implements DocScoreHeap {

  private static final long SENTINEL = encode(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);

  private final TernaryLongHeap heap;

  PackedDocScoreHeap(int numHits) {
    heap = new TernaryLongHeap(Math.max(1, numHits), SENTINEL);
  }

  /**
   * Packs {@code (doc, score)} so that a natural {@code long} comparison orders by score ascending
   * then doc descending — i.e. the least competitive hit is the smallest code. Using {@code
   * Integer.MAX_VALUE - doc} in the low bits makes a higher doc id sort lower (less competitive).
   */
  private static long encode(int doc, float score) {
    return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (Integer.MAX_VALUE - doc);
  }

  private static float score(long code) {
    return NumericUtils.sortableIntToFloat((int) (code >>> 32));
  }

  private static long doc(long code) {
    return Integer.MAX_VALUE - ((int) code);
  }

  @Override
  public float topScore() {
    return score(heap.top());
  }

  @Override
  public long topDoc() {
    return doc(heap.top());
  }

  @Override
  public void updateTop(long doc, float score) {
    // This heap is only created when the doc-id space fits an int, so the cast is lossless.
    heap.updateTop(encode((int) doc, score));
  }

  @Override
  public void pop() {
    heap.pop();
  }

  @Override
  public int size() {
    return heap.size();
  }

  @Override
  public boolean isSentinel(int i) {
    return heap.get(i) == SENTINEL;
  }
}
