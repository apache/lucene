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
package org.apache.lucene.jmh.base.meter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute moving average
 * throughputs.
 */
public class Meter {

  private final ExponentialMovingAverages movingAverages;
  private final LongAdder count = new LongAdder();
  private final long startTime;
  private final Clock clock;

  /** Creates a new {@link Meter}. */
  public Meter() {
    this(Clock.defaultClock());
  }

  /**
   * Creates a new {@link Meter}.
   *
   * @param clock the clock to use for the meter ticks
   */
  public Meter(Clock clock) {
    this(new ExponentialMovingAverages(clock), clock);
  }

  /**
   * Instantiates a new Meter.
   *
   * @param movingAverages the moving averages
   * @param clock the clock
   */
  public Meter(ExponentialMovingAverages movingAverages, Clock clock) {
    this.movingAverages = movingAverages;
    this.clock = clock;
    this.startTime = this.clock.getTick();
  }

  /** Mark the occurrence of an event. */
  public void mark() {
    mark(1);
  }

  /**
   * Mark the occurrence of a given number of events.
   *
   * @param n the number of events
   */
  public void mark(long n) {
    movingAverages.tickIfNecessary();
    count.add(n);
    movingAverages.update(n);
  }

  /**
   * Gets count.
   *
   * @return the count
   */
  public long getCount() {
    return count.sum();
  }

  /**
   * Gets fifteen minute rate.
   *
   * @return the fifteen minute rate
   */
  public double getFifteenMinuteRate() {
    movingAverages.tickIfNecessary();
    return movingAverages.getM15Rate();
  }

  /**
   * Gets five minute rate.
   *
   * @return the five minute rate
   */
  public double getFiveMinuteRate() {
    movingAverages.tickIfNecessary();
    return movingAverages.getM5Rate();
  }

  /**
   * Gets mean rate.
   *
   * @return the mean rate
   */
  public double getMeanRate() {
    if (getCount() == 0) {
      return 0.0;
    } else {
      final double elapsed = clock.getTick() - startTime;
      return getCount() / elapsed * TimeUnit.SECONDS.toNanos(1);
    }
  }

  /**
   * Gets one minute rate.
   *
   * @return the one minute rate
   */
  public double getOneMinuteRate() {
    movingAverages.tickIfNecessary();
    return movingAverages.getM1Rate();
  }
}
