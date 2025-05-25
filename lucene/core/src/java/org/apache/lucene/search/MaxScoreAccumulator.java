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

import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.util.NumericUtils;

/** Maintains the maximum score and its corresponding document id concurrently */
final class MaxScoreAccumulator {
  // we use 2^10-1 to check the remainder with a bitwise operation
  private static final int DEFAULT_INTERVAL = 0x3ff;
  private static final int POS_INF_TO_SORTABLE_INT = NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY);
  static final long LEAST_COMPETITIVE_CODE = encode(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);

  // scores are always positive
  final LongAccumulator acc = new LongAccumulator(Math::max, Long.MIN_VALUE);

  // non-final and visible for tests
  long modInterval;

  MaxScoreAccumulator() {
    this.modInterval = DEFAULT_INTERVAL;
  }

  void accumulate(int docId, float score) {
    assert docId >= 0 && score >= 0;
    acc.accumulate(encode(docId, score));
  }

  void accumulateIntScore(int docId, int score) {
    assert docId >= 0 && score >= 0;
    acc.accumulate(encodeIntScore(docId, score));
  }

  static long encode(int docId, float score) {
    return encodeIntScore(docId, NumericUtils.floatToSortableInt(score));
  }

  static long encodeIntScore(int docId, int score) {
    return (((long) score) << 32) | (Integer.MAX_VALUE - docId);
  }

  static float toScore(long value) {
    return NumericUtils.sortableIntToFloat(toIntScore(value));
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
    assert nextUp == NumericUtils.floatToSortableInt(Math.nextUp(NumericUtils.sortableIntToFloat(intScore)));
    return nextUp;
  }

  long getRaw() {
    return acc.get();
  }
}
