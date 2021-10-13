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

import java.util.Date;

/**
 * A Class for creating Date Sources that will produce Dates based on the number of milliseconds
 * since epoch
 */
public class DatesDSL {

  /** Instantiates a new Dates dsl. */
  public DatesDSL() {
    /* TODO document why this constructor is empty */
  }

  /**
   * All RndGen.
   *
   * @return the solr gen
   */
  public RndGen<Date> all() {
    return Dates.withMilliSecondsBetween(0, Long.MAX_VALUE);
  }

  /**
   * Generates Dates inclusively bounded between January 1, 1970, 00:00:00 GMT and new
   * Date(milliSecondsFromEpoch). The Source restricts Date generation, so that no Dates before 1970
   * can be created. The Source is weighted, so it is likely to produce new
   * Date(millisecondsFromEpoch) one or more times.
   *
   * @param millisecondsFromEpoch the number of milliseconds from the epoch such that Dates are
   *     generated within this interval.
   * @return a Source of type Date
   */
  public RndGen<Date> withMilliseconds(long millisecondsFromEpoch) {
    lowerBoundGEQZero(millisecondsFromEpoch);
    return Dates.withMilliSeconds(millisecondsFromEpoch);
  }

  /**
   * Generates Dates inclusively bounded between new Date(millisecondsFromEpochStartInclusive) and
   * new Date(millisecondsFromEpochEndInclusive).
   *
   * @param millisecondsFromEpochStartInclusive the number of milliseconds from epoch for the
   *     desired older Date
   * @param millisecondsFromEpochEndInclusive the number of milliseconds from epoch for the desired
   *     more recent Date
   * @return a source of Dates
   */
  public RndGen<Date> withMillisecondsBetween(
      long millisecondsFromEpochStartInclusive, long millisecondsFromEpochEndInclusive) {
    lowerBoundGEQZero(millisecondsFromEpochStartInclusive);
    maxGEQMin(millisecondsFromEpochStartInclusive, millisecondsFromEpochEndInclusive);
    return Dates.withMilliSecondsBetween(
        millisecondsFromEpochStartInclusive, millisecondsFromEpochEndInclusive);
  }

  private void lowerBoundGEQZero(long milliSecondsFromEpoch) {
    SourceDSL.checkArguments(
        milliSecondsFromEpoch >= 0,
        "A negative long (%s) is not an accepted number of milliseconds",
        milliSecondsFromEpoch);
  }

  private void maxGEQMin(long startInclusive, long endInclusive) {
    SourceDSL.checkArguments(
        startInclusive <= endInclusive,
        "Cannot have the maximum long (%s) smaller than the minimum long value (%s)",
        endInclusive,
        startInclusive);
  }

  /** The type Dates. */
  static class Dates {

    private Dates() {}

    /**
     * With milli seconds RndGen.
     *
     * @param milliSecondsFromEpoch the milli seconds from epoch
     * @return the solr gen
     */
    static RndGen<Date> withMilliSeconds(long milliSecondsFromEpoch) {
      return withMilliSecondsBetween(0, milliSecondsFromEpoch);
    }

    /**
     * With milli seconds between solr gen.
     *
     * @param milliSecondsFromEpochStartInclusive the milli seconds from epoch start inclusive
     * @param milliSecondsFromEpochEndInclusive the milli seconds from epoch end inclusive
     * @return the solr gen
     */
    static RndGen<Date> withMilliSecondsBetween(
        long milliSecondsFromEpochStartInclusive, long milliSecondsFromEpochEndInclusive) {
      return Generate.longRange(
              milliSecondsFromEpochStartInclusive, milliSecondsFromEpochEndInclusive)
          .map(Date::new);
    }
  }
}
