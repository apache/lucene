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
 */ package org.apache.lucene.jmh.base.meter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A triple (one, five and fifteen minutes) of exponentially-weighted moving average rates as needed
 * by {@link Meter}.
 *
 * <p>The rates have the same exponential decay factor as the fifteen-minute load average in the
 * {@code top} Unix command.
 */
public class ExponentialMovingAverages {

  private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);

  private final EWMA m1Rate = EWMA.oneMinuteEWMA();
  private final EWMA m5Rate = EWMA.fiveMinuteEWMA();
  private final EWMA m15Rate = EWMA.fifteenMinuteEWMA();

  private final AtomicLong lastTick;
  private final Clock clock;

  /** Creates a new {@link ExponentialMovingAverages}. */
  public ExponentialMovingAverages() {
    this(Clock.defaultClock());
  }

  /**
   * Creates a new {@link ExponentialMovingAverages}. @param clock the clock @param clock the
   * clock @param clock the clock @param clock the clock @param clock the clock
   *
   * @param clock the clock
   */
  public ExponentialMovingAverages(Clock clock) {
    this.clock = clock;
    this.lastTick = new AtomicLong(this.clock.getTick());
  }

  /**
   * Update.
   *
   * @param n the n
   */
  public void update(long n) {
    m1Rate.update(n);
    m5Rate.update(n);
    m15Rate.update(n);
  }

  /** Tick if necessary. */
  public void tickIfNecessary() {
    final long oldTick = lastTick.get();
    final long newTick = clock.getTick();
    final long age = newTick - oldTick;
    if (age > TICK_INTERVAL) {
      final long newIntervalStartTick = newTick - age % TICK_INTERVAL;
      if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
        final long requiredTicks = age / TICK_INTERVAL;
        for (long i = 0; i < requiredTicks; i++) {
          m1Rate.tick();
          m5Rate.tick();
          m15Rate.tick();
        }
      }
    }
  }

  /**
   * Gets m 1 rate.
   *
   * @return the m 1 rate
   */
  public double getM1Rate() {
    return m1Rate.getRate(TimeUnit.SECONDS);
  }

  /**
   * Gets m 5 rate.
   *
   * @return the m 5 rate
   */
  public double getM5Rate() {
    return m5Rate.getRate(TimeUnit.SECONDS);
  }

  /**
   * Gets m 15 rate.
   *
   * @return the m 15 rate
   */
  public double getM15Rate() {
    return m15Rate.getRate(TimeUnit.SECONDS);
  }
}
