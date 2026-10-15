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

import java.util.Arrays;

/**
 * {@link DocScoreHeap} that keeps the doc id as a {@code long} in a parallel array beside the
 * scores. Used when the doc-id space exceeds {@link Integer#MAX_VALUE}, where a doc cannot be
 * packed with the score into a single {@code long} (see {@link PackedDocScoreHeap}).
 *
 * <p>Modeled on {@code TernaryLongHeap}: 1-based indexing (slot 0 unused) and 3-ary fan-out. The
 * sift-down compares scores and reads the doc array only to break an exact score tie.
 *
 * @lucene.internal
 */
final class LongDocScoreHeap implements DocScoreHeap {

  private static final long SENTINEL_DOC = Long.MAX_VALUE;
  private static final float SENTINEL_SCORE = Float.NEGATIVE_INFINITY;

  private static final int ARITY = 3;

  private final long[] docs;
  private final float[] scores;
  private int size;

  LongDocScoreHeap(int numHits) {
    if (numHits < 1) {
      numHits = 1;
    }
    docs = new long[numHits + 1];
    scores = new float[numHits + 1];
    Arrays.fill(docs, 1, numHits + 1, SENTINEL_DOC);
    Arrays.fill(scores, 1, numHits + 1, SENTINEL_SCORE);
    size = numHits;
  }

  @Override
  public long topDoc() {
    return docs[1];
  }

  @Override
  public float topScore() {
    return scores[1];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isSentinel(int i) {
    return scores[i] == SENTINEL_SCORE && docs[i] == SENTINEL_DOC;
  }

  @Override
  public void updateTop(long doc, float score) {
    docs[1] = doc;
    scores[1] = score;
    downHeap(1);
  }

  @Override
  public void pop() {
    if (size <= 0) {
      throw new IllegalStateException("The heap is empty");
    }
    docs[1] = docs[size];
    scores[1] = scores[size];
    size--;
    downHeap(1);
  }

  private void downHeap(int i) {
    final long doc = docs[i];
    final float score = scores[i];
    for (; ; ) {
      int firstChild = ARITY * (i - 1) + 2;
      if (firstChild > size) {
        break; // i is a leaf
      }
      int lastChild = Math.min(firstChild + ARITY - 1, size);

      // Find the least competitive child (lowest score, ties broken by higher doc). The score is
      // the only thing consulted in the common case; the doc array is touched only to break an
      // exact
      // score tie, which keeps this hot loop reading a single array most of the time.
      int best = firstChild;
      float bestScore = scores[firstChild];
      for (int c = firstChild + 1; c <= lastChild; c++) {
        float childScore = scores[c];
        if (childScore < bestScore || (childScore == bestScore && docs[c] > docs[best])) {
          best = c;
          bestScore = childScore;
        }
      }

      // Stop if the sifting value is already the least competitive of the subtree.
      if (score < bestScore || (score == bestScore && doc > docs[best])) {
        break;
      }
      docs[i] = docs[best];
      scores[i] = bestScore;
      i = best;
    }
    docs[i] = doc;
    scores[i] = score;
  }
}
