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

/** Maintains the maximum score and its corresponding document id concurrently */
final class MaxScoreAccumulator {
  // we use 2^10-1 to check the remainder with a bitwise operation
  static final int DEFAULT_INTERVAL = 0x3ff;

  // scores are always positive
  final LongAccumulator acc = new LongAccumulator(MaxScoreAccumulator::maxEncode, Long.MIN_VALUE);

  // non-final and visible for tests
  long modInterval;

  MaxScoreAccumulator() {
    this.modInterval = DEFAULT_INTERVAL;
  }

  /**
   * Return the max encoded docId and score found in the two longs, following the encoding in {@link
   * #accumulate}.
   */
  private static long maxEncode(long v1, long v2) {
    float score1 = Float.intBitsToFloat((int) (v1 >> 32));
    float score2 = Float.intBitsToFloat((int) (v2 >> 32));
    int cmp = Float.compare(score1, score2);
    if (cmp == 0) {
      // tie-break on the minimum doc base
      return (int) v1 < (int) v2 ? v1 : v2;
    } else if (cmp > 0) {
      return v1;
    }
    return v2;
  }

  void accumulate(int docId, float score) {
    assert docId >= 0 && score >= 0;
    long encode = (((long) Float.floatToIntBits(score)) << 32) | docId;
    acc.accumulate(encode);
  }

  public static float toScore(long value) {
    return Float.intBitsToFloat((int) (value >> 32));
  }

  public static int docId(long value) {
    return (int) value;
  }

  long getRaw() {
    return acc.get();
  }
}
