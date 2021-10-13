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
package org.apache.lucene.jmh.base.rndgen;

import java.util.function.Function;

final class Doubles {

  private static final long POSITIVE_INFINITY_CORRESPONDING_LONG = 0x7ff0000000000000L;
  private static final long NEGATIVE_INFINITY_CORRESPONDING_LONG = 0xfff0000000000000L;
  // fraction portion of double, last 52 bits
  private static final long FRACTION_BITS = 1L << 53;
  private static final double DOUBLE_UNIT = 0x1.0p-53; // 1.0 / (1L << 53)
  private static final long NEGATIVE_ZERO_CORRESPONDING_LONG = Long.MIN_VALUE;

  private Doubles() {}

  static RndGen<Double> fromNegativeInfinityToPositiveInfinity() {
    return negative().mix(positive());
  }

  static RndGen<Double> negative() {
    return range(NEGATIVE_ZERO_CORRESPONDING_LONG, NEGATIVE_INFINITY_CORRESPONDING_LONG);
  }

  static RndGen<Double> positive() {
    return range(0, POSITIVE_INFINITY_CORRESPONDING_LONG);
  }

  static RndGen<Double> fromZeroToOne() {
    return range(0, FRACTION_BITS, l -> l * DOUBLE_UNIT);
  }

  static RndGen<Double> between(double min, double max) {
    SourceDSL.checkArguments(
        min <= max, "Cannot have the maximum (%s) smaller than the min (%s)", max, min);
    double adjustedMax = max - min;
    return fromZeroToOne().map(d -> (d * adjustedMax) + min);
  }

  static RndGen<Double> range(long startInclusive, long endInclusive) {
    return range(startInclusive, endInclusive, Double::longBitsToDouble);
  }

  static RndGen<Double> range(
      long startInclusive, long endInclusive, Function<Long, Double> conversion) {
    return Generate.longRange(startInclusive, endInclusive).map(conversion);
  }
}
