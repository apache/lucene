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
package org.apache.lucene.facet.rangeonrange;

import java.util.Arrays;
import java.util.Objects;

/** Represents a long range for RangeOnRange faceting */
public class LongRange extends Range {
  /** Minimum (inclusive). */
  public final long[] min;

  /** Maximum (inclusive). */
  public final long[] max;

  /**
   * Represents a single dimensional long range for RangeOnRange faceting
   *
   * @param label the name of the range
   * @param minIn the minimum
   * @param minInclusive if the minimum is inclusive
   * @param maxIn the maximum
   * @param maxInclusive if the maximum is inclusive
   */
  public LongRange(
      String label, long minIn, boolean minInclusive, long maxIn, boolean maxInclusive) {
    super(label, 1);

    if (minInclusive == false) {
      if (minIn != Long.MAX_VALUE) {
        minIn++;
      } else {
        failNoMatch();
      }
    }

    if (maxInclusive == false) {
      if (maxIn != Long.MIN_VALUE) {
        maxIn--;
      } else {
        failNoMatch();
      }
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.min = new long[] {minIn};
    this.max = new long[] {maxIn};
  }

  /**
   * Represents a multidimensional long range for RangeOnRange faceting
   *
   * @param label the name of the range
   * @param min the minimum, inclusive
   * @param max the maximum, inclusive
   */
  public LongRange(String label, long[] min, long[] max) {
    super(label, min.length);
    checkArgs(min, max);
    this.min = min;
    this.max = max;
  }

  @Override
  public String toString() {
    return "LongRange(label: "
        + label
        + ", min: "
        + Arrays.toString(min)
        + ", max: "
        + Arrays.toString(max)
        + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LongRange longRange = (LongRange) o;
    return Arrays.equals(min, longRange.min)
        && Arrays.equals(max, longRange.max)
        && label.equals(longRange.label)
        && dims == longRange.dims;
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, Arrays.hashCode(min), Arrays.hashCode(max), dims);
  }

  private void checkArgs(final long[] min, final long[] max) {
    if (min == null || max == null || min.length == 0 || max.length == 0) {
      failNoMatch();
    }
    if (min.length != max.length) {
      failNoMatch();
    }

    for (int i = 0; i < min.length; i++) {
      if (min[i] > max[i]) {
        failNoMatch();
      }
    }
  }
}
