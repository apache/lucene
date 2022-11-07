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

import java.util.Objects;

/** Represents a long range for RangeOnRange faceting */
public class LongRange extends Range {
  /** Minimum (inclusive). */
  public final long min;

  /** Maximum (inclusive). */
  public final long max;

  /**
   * Represents a long range for RangeOnRange faceting
   *
   * @param label the name of the range
   * @param minIn the minimum
   * @param minInclusive if the minimum is inclusive
   * @param maxIn the maximum
   * @param maxInclusive if the maximum is inclusive
   */
  public LongRange(
      String label, long minIn, boolean minInclusive, long maxIn, boolean maxInclusive) {
    super(label);

    if (!minInclusive) {
      if (minIn != Long.MAX_VALUE) {
        minIn++;
      } else {
        failNoMatch();
      }
    }

    if (!maxInclusive) {
      if (maxIn != Long.MIN_VALUE) {
        maxIn--;
      } else {
        failNoMatch();
      }
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.min = minIn;
    this.max = maxIn;
  }

  @Override
  public int getNumBytesPerRange() {
    return Long.BYTES;
  }

  @Override
  public String toString() {
    return "LongRange(" + label + ": " + min + " to " + max + ")";
  }

  @Override
  public boolean equals(Object _that) {
    if (_that instanceof LongRange == false) {
      return false;
    }
    LongRange that = (LongRange) _that;
    return that.label.equals(this.label) && that.min == this.min && that.max == this.max;
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, min, max);
  }
}
