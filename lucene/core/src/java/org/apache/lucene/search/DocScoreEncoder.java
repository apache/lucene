/*
 * Copyright (c) 1997, 2021, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
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
