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

/**
 * A bounded min-heap of the most competitive {@code (doc, score)} hits for {@link
 * TopScoreDocCollector}, ordered so the <b>least competitive</b> hit is on top for eviction: lowest
 * score first, and among equal scores the <b>highest</b> doc id. It is pre-filled with sentinels,
 * so {@link #size()} equals the requested capacity and {@link #updateTop} can be called
 * immediately.
 *
 * <p>Two implementations back it, chosen by {@link #create}:
 *
 * <ul>
 *   <li>{@link PackedDocScoreHeap} packs {@code (score, doc)} into a single {@code long}; used when
 *       the reader's doc-id space fits an {@code int}.
 *   <li>{@link LongDocScoreHeap} keeps the doc as a {@code long} in a parallel array; used when the
 *       doc-id space exceeds {@link Integer#MAX_VALUE}.
 * </ul>
 *
 * @lucene.internal
 */
interface DocScoreHeap {

  /**
   * Creates a heap of the given capacity.
   *
   * @param numHits the number of hits to retain
   * @param docsFitInt whether every doc id the collector will see fits an {@code int} (i.e. the
   *     searched reader holds at most {@link Integer#MAX_VALUE} documents); selects the fast packed
   *     implementation when {@code true}
   */
  static DocScoreHeap create(int numHits, boolean docsFitInt) {
    return docsFitInt ? new PackedDocScoreHeap(numHits) : new LongDocScoreHeap(numHits);
  }

  /** The score of the least competitive hit currently on the heap. */
  float topScore();

  /** The doc id of the least competitive hit currently on the heap. */
  long topDoc();

  /** Replaces the least competitive hit with {@code (doc, score)} and restores heap order. */
  void updateTop(long doc, float score);

  /**
   * Removes the least competitive hit; read it with {@link #topDoc()}/{@link #topScore()} first.
   */
  void pop();

  /** Number of entries currently on the heap (including any remaining sentinels). */
  int size();

  /** True if the entry at 1-based slot {@code i} (in {@code [1, size]}) is still a sentinel. */
  boolean isSentinel(int i);
}
