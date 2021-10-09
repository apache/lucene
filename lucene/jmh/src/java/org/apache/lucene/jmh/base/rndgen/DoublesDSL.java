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
 * A Class for creating Double Sources that will produce doubles within a set interval and will
 * shrink within this domain.
 */
public class DoublesDSL {

  /** Instantiates a new Doubles dsl. */
  public DoublesDSL() {
    /* TODO document why this constructor is empty */
  }

  /**
   * Generates Doubles inclusively bounded below by Double.NEGATIVE_INFINITY and above by
   * Double.POSITIVE_INFINITY.
   *
   * @return a Source of type Double
   */
  public RndGen<Double> all() {
    return Doubles.fromNegativeInfinityToPositiveInfinity();
  }

  /**
   * Generates Doubles inclusively bounded below by Double.NEGATIVE_INFINITY and above by a value
   * very close to zero on the negative side.
   *
   * @return a Source of type Double
   */
  public RndGen<Double> negative() {
    return Doubles.negative();
  }

  /**
   * Generates Doubles inclusively bounded below by a value very close to zero on the positive side
   * and above by Double.POSITIVE_INFINITY.
   *
   * @return a Source of type Double
   */
  public RndGen<Double> positive() {
    return Doubles.positive();
  }

  /**
   * Generates Doubles inclusively bounded below by zero and above by one.
   *
   * @return a Source of type Double
   */
  public RndGen<Double> fromZeroToOne() {
    return Doubles.fromZeroToOne();
  }

  /**
   * Generates Doubles inclusively between two bounds
   *
   * @param minInclusive minimum value to generate
   * @param maxInclusive maximum value to generate
   * @return a Gen of Doubles between minInclusive and maxInclusive
   */
  public RndGen<Double> between(double minInclusive, double maxInclusive) {
    return Doubles.between(minInclusive, maxInclusive);
  }

  /**
   * Starts a range
   *
   * @param startInclusive - lower bound of domain
   * @return start of range
   */
  public DoubleDomainBuilder from(final double startInclusive) {
    return new DoubleDomainBuilder(startInclusive);
  }

  /** The type Double domain builder. */
  public class DoubleDomainBuilder {

    private final double startInclusive;

    private DoubleDomainBuilder(double startInclusive) {
      this.startInclusive = startInclusive;
    }

    /**
     * Generates within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Double
     */
    public RndGen<Double> upToAndIncluding(final double endInclusive) {
      return between(startInclusive, endInclusive);
    }
  }
}
