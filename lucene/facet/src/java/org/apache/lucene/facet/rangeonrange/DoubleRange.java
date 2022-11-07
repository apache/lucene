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

/** Represents a double range for RangeOnRange faceting */
public class DoubleRange extends Range {
  /** Minimum (inclusive). */
  public final double min;

  /** Maximum (inclusive). */
  public final double max;

  /**
   * Represents a double range for RangeOnRange faceting
   *
   * @param label the name of the range
   * @param minIn the minimum
   * @param minInclusive if the minimum is inclusive
   * @param maxIn the maximum
   * @param maxInclusive if the maximum is inclusive
   */
  public DoubleRange(
      String label, double minIn, boolean minInclusive, double maxIn, boolean maxInclusive) {
    super(label);

    if (Double.isNaN(minIn)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      minIn = Math.nextUp(minIn);
    }

    if (Double.isNaN(maxIn)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      // Why no Math.nextDown?
      maxIn = Math.nextAfter(maxIn, Double.NEGATIVE_INFINITY);
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.min = minIn;
    this.max = maxIn;
  }

  @Override
  public int getNumBytesPerRange() {
    return Double.BYTES;
  }

  @Override
  public String toString() {
    return "DoubleRange(" + label + ": " + min + " to " + max + ")";
  }

  @Override
  public boolean equals(Object _that) {
    if (_that instanceof DoubleRange == false) {
      return false;
    }
    DoubleRange that = (DoubleRange) _that;
    return that.label.equals(this.label) && that.min == this.min && that.max == this.max;
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, min, max);
  }
}
