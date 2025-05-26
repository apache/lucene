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

/**
 * An encoder do encode (doc, score) pair as a long whose sort order is same as {@code (o1, o2) ->
 * Float.compare(o1.score, o2.score)).thenComparing(Comparator.comparingInt((ScoreDoc o) ->
 * o.doc).reversed())}
 *
 * <p>Note that negative score is allowed but relationship between two codes encoded by negative
 * scores is undefined. The only thing guaranteed is codes encoded from negative scores are smaller
 * than codes encoded from non-negative scores.
 */
class DocScoreEncoder {

  static final long LEAST_COMPETITIVE_CODE = encode(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
  private static final int POS_INF_TO_SORTABLE_INT = scoreToSortableInt(Float.POSITIVE_INFINITY);

  static long encode(int docId, float score) {
    return encodeIntScore(docId, scoreToSortableInt(score));
  }

  static long encodeIntScore(int docId, int score) {
    return (((long) score) << 32) | (Integer.MAX_VALUE - docId);
  }

  static float toScore(long value) {
    return sortableIntToScore(toIntScore(value));
  }

  static int toIntScore(long value) {
    return (int) (value >>> 32);
  }

  static int docId(long value) {
    return Integer.MAX_VALUE - ((int) value);
  }

  static int nextUp(int intScore) {
    assert intScore <= POS_INF_TO_SORTABLE_INT;
    int nextUp = Math.min(POS_INF_TO_SORTABLE_INT, intScore + 1);
    assert nextUp == scoreToSortableInt(Math.nextUp(sortableIntToScore(intScore)));
    return nextUp;
  }

  /**
   * Score is non-negative float so wo use floatToRawIntBits instead of {@link
   * NumericUtils#floatToSortableInt}. We do not assert score >= 0 here to allow pass negative float
   * to indicate totally non-competitive, e.g. {@link #LEAST_COMPETITIVE_CODE}.
   */
  static int scoreToSortableInt(float score) {
    assert Float.isNaN(score) == false;
    return Float.floatToRawIntBits(score);
  }

  /**
   * @see #scoreToSortableInt(float)
   */
  static float sortableIntToScore(int scoreBits) {
    float score = Float.intBitsToFloat(scoreBits);
    assert Float.isNaN(score) == false;
    return score;
  }
}
