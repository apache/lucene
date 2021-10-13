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
 * A Class for creating Long Sources that will produce Longs within a set interval and will shrink
 * within this domain.
 */
public class LongsDSL {

  /** Instantiates a new Longs dsl. */
  public LongsDSL() {
    /* TODO document why this constructor is empty */
  }

  /**
   * Constructs a LongDomainBuilder object with an inclusive lower bound
   *
   * @param startInclusive - lower bound of domain
   * @return a LongDomainBuilder
   */
  public LongDomainBuilder from(final long startInclusive) {
    return new LongDomainBuilder(startInclusive);
  }

  /**
   * Generates all possible Longs in Java bounded below by Long.MIN_VALUE and above by
   * Long.MAX_VALUE.
   *
   * @return a Source of type Long
   */
  public RndGen<Long> all() {
    return between(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /** The type Long domain builder. */
  public class LongDomainBuilder {

    private final long startInclusive;

    private LongDomainBuilder(long startInclusive) {
      this.startInclusive = startInclusive;
    }

    /**
     * Generates Longs within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Long
     */
    public RndGen<Long> upToAndIncluding(final long endInclusive) {
      return between(startInclusive, endInclusive);
    }

    /**
     * Generates Longs within the interval specified with an inclusive lower bound and exclusive
     * upper bound.
     *
     * @param endExclusive - exclusive upper bound of domain
     * @return a Source of type Long
     */
    public RndGen<Long> upTo(final long endExclusive) {
      return between(startInclusive, endExclusive - 1);
    }
  }

  /**
   * Generates Longs within the interval specified with an inclusive lower and upper bound.
   *
   * <p>The Source is weighted, so it is likely to generate the upper and lower limits of the domain
   * one or more times.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @return a Source of type Long
   */
  public RndGen<Long> between(final long startInclusive, final long endInclusive) {
    SourceDSL.checkArguments(
        startInclusive <= endInclusive,
        "There are no Long values to be generated between (%s) and (%s)",
        startInclusive,
        endInclusive);
    return Generate.longRange(startInclusive, endInclusive);
  }
}
