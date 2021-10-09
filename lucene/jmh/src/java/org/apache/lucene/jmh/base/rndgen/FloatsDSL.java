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

/**
 * A Class for creating Float Sources that will produce floats within a set interval and will shrink
 * within this domain.
 */
public class FloatsDSL {

  /** Instantiates a new Floats dsl. */
  public FloatsDSL() {
    /* TODO document why this constructor is empty */
  }

  /**
   * Generates Floats inclusively bounded below by Float.NEGATIVE_INFINITY and above by
   * Float.POSITIVE_INFINITY.
   *
   * @return a Source of type Float
   */
  public RndGen<Float> all() {
    return Floats.fromNegativeInfinityToPositiveInfinity();
  }

  /**
   * Generates Floats inclusively bounded below by Float.NEGATIVE_INFINITY and above by a value very
   * close to zero on the negative side.
   *
   * @return a Source of type Float
   */
  public RndGen<Float> negative() {
    return Floats.fromNegativeInfinityToNegativeZero();
  }

  /**
   * Generates Floats inclusively bounded below by a value very close to zero on the positive side
   * and above by Float.POSITIVE_INFINITY.
   *
   * @return a Source of type Float
   */
  public RndGen<Float> positive() {
    return Floats.fromZeroToPositiveInfinity();
  }

  /**
   * Generates Floats inclusively bounded below by zero and above by one.
   *
   * @return a Source of type Float
   */
  public RndGen<Float> fromZeroToOne() {
    return Floats.fromZeroToOne();
  }

  /**
   * Generates Floats inclusively between two bounds
   *
   * @param minInclusive minimum value to generate
   * @param maxInclusive maximum value to generate
   * @return a Gen of Floats between minInclusive and maxInclusive
   */
  public RndGen<Float> between(float minInclusive, float maxInclusive) {
    return Floats.between(minInclusive, maxInclusive);
  }

  /**
   * Starts a range
   *
   * @param startInclusive - lower bound of domain
   * @return start of range
   */
  public FloatDomainBuilder from(final float startInclusive) {
    return new FloatDomainBuilder(startInclusive);
  }

  /** The type Float domain builder. */
  public class FloatDomainBuilder {

    private final float startInclusive;

    private FloatDomainBuilder(float startInclusive) {
      this.startInclusive = startInclusive;
    }

    /**
     * Generates within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Float
     */
    public RndGen<Float> upToAndIncluding(final float endInclusive) {
      return between(startInclusive, endInclusive);
    }
  }
}
