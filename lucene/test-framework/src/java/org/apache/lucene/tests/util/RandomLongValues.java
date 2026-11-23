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
package org.apache.lucene.tests.util;

import java.util.Random;
import java.util.function.LongSupplier;

/**
 * A random distribution of {@code long} values with definite bounds: every value returned by {@link
 * #nextValue()} is within {@link #bounds()}.
 *
 * <p>The available {@link Distribution}s are chosen to exercise the different encodings of numeric
 * doc values. {@link #nextBound()} returns {@link Bounds} that are useful for range queries over
 * data from this distribution.
 */
public final class RandomLongValues {

  /** The shapes of data that can be generated. */
  public enum Distribution {
    /** Uniform values in a small range around zero. */
    SMALL_RANGE {
      @Override
      RandomLongValues create(Random random) {
        return uniform(random, -100, 10000);
      }
    },
    /** A single value. */
    CONSTANT {
      @Override
      RandomLongValues create(Random random) {
        long constant = randomAnchor(random);
        return uniform(random, constant, constant);
      }
    },
    /** Between 2 and 200 distinct, widely spaced values. */
    FEW_DISTINCT {
      @Override
      RandomLongValues create(Random random) {
        long[] distinct = new long[TestUtil.nextInt(random, 2, 200)];
        for (int i = 0; i < distinct.length; i++) {
          distinct[i] = random.nextBoolean() ? random.nextLong() : randomAnchor(random);
        }
        return oneOf(random, distinct);
      }
    },
    /** Uniform values just above {@link Long#MIN_VALUE}. */
    NEAR_MIN {
      @Override
      RandomLongValues create(Random random) {
        long width = TestUtil.nextLong(random, 0, 1 << random.nextInt(1, 20));
        return uniform(random, Long.MIN_VALUE, Long.MIN_VALUE + width);
      }
    },
    /** Uniform values just below {@link Long#MAX_VALUE}. */
    NEAR_MAX {
      @Override
      RandomLongValues create(Random random) {
        long width = TestUtil.nextLong(random, 0, 1 << random.nextInt(1, 20));
        return uniform(random, Long.MAX_VALUE - width, Long.MAX_VALUE);
      }
    },
    /** Only values at, or very close to, both ends of the {@code long} range. */
    EXTREMES {
      @Override
      RandomLongValues create(Random random) {
        return oneOf(
            random,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            Long.MIN_VALUE + random.nextInt(1, 3),
            Long.MAX_VALUE - random.nextInt(1, 3));
      }
    },
    /** Values of the form {@code base + k * gcd}, with a base that is often near an extreme. */
    GCD {
      @Override
      RandomLongValues create(Random random) {
        long gcd =
            switch (random.nextInt(4)) {
              case 0 -> random.nextInt(2, 100);
              case 1 -> TestUtil.nextLong(random, 2, 1L << 20);
              case 2 -> TestUtil.nextLong(random, 2, Long.MAX_VALUE);
              default -> 1L << random.nextInt(1, 62);
            };
        long base = randomAnchor(random);
        // unsigned arithmetic: MAX_VALUE - base may exceed MAX_VALUE
        long maxMultiplier = Long.divideUnsigned(Long.MAX_VALUE - base, gcd);
        long multiplierLimit = 1L << random.nextInt(4, 21);
        long multiplierBound =
            Long.compareUnsigned(maxMultiplier, multiplierLimit) < 0
                ? maxMultiplier
                : multiplierLimit;
        // multiplierBound * gcd <= MAX_VALUE - base, so neither expression below overflows
        return new RandomLongValues(
            random,
            base,
            base + multiplierBound * gcd,
            () -> base + TestUtil.nextLong(random, 0, multiplierBound) * gcd);
      }
    },
    /** Uniform values over the whole {@code long} range. */
    FULL_RANGE {
      @Override
      RandomLongValues create(Random random) {
        return uniform(random, Long.MIN_VALUE, Long.MAX_VALUE);
      }
    },
    /** Positive and negative powers of two, and their immediate neighbours. */
    POWERS_OF_TWO {
      @Override
      RandomLongValues create(Random random) {
        long[] pool = new long[63 * 5 * 2];
        int n = 0;
        for (int power = 0; power < 63; power++) {
          for (int offset = -2; offset <= 2; offset++) {
            long value = (1L << power) + offset;
            pool[n++] = value;
            pool[n++] = -value;
          }
        }
        return oneOf(random, pool);
      }
    };

    abstract RandomLongValues create(Random random);
  }

  /** Creates values from a randomly chosen {@link Distribution}. */
  public static RandomLongValues random(Random random) {
    Distribution[] all = Distribution.values();
    return of(random, all[random.nextInt(all.length)]);
  }

  /** Creates values from the given {@link Distribution}. */
  public static RandomLongValues of(Random random, Distribution distribution) {
    return distribution.create(random);
  }

  /** An inclusive range of {@code long} values. */
  public record Bounds(long min, long max) {
    public Bounds {
      if (min > max) {
        throw new IllegalArgumentException("min " + min + " is greater than max " + max);
      }
    }

    public boolean contains(long value) {
      return min <= value && value <= max;
    }

    public boolean contains(Bounds other) {
      return min <= other.min && other.max <= max;
    }

    public boolean overlaps(Bounds other) {
      return min <= other.max && other.min <= max;
    }
  }

  private final Random random;
  private final Bounds bounds;
  private final LongSupplier sampler;

  private RandomLongValues(Random random, long min, long max, LongSupplier sampler) {
    this.random = random;
    this.bounds = new Bounds(min, max);
    this.sampler = sampler;
  }

  private static RandomLongValues uniform(Random random, long min, long max) {
    return new RandomLongValues(random, min, max, () -> TestUtil.nextLong(random, min, max));
  }

  private static RandomLongValues oneOf(Random random, long... candidates) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long candidate : candidates) {
      min = Math.min(min, candidate);
      max = Math.max(max, candidate);
    }
    return new RandomLongValues(
        random, min, max, () -> candidates[random.nextInt(candidates.length)]);
  }

  private static long randomAnchor(Random random) {
    return switch (random.nextInt(6)) {
      case 0 -> Long.MIN_VALUE;
      case 1 -> Long.MIN_VALUE + random.nextInt(1000);
      case 2 -> 0;
      case 3 -> TestUtil.nextLong(random, -100, 100);
      case 4 -> Long.MAX_VALUE - random.nextInt(1000);
      default -> random.nextLong();
    };
  }

  /** The range that every value returned by {@link #nextValue()} is within. */
  public Bounds bounds() {
    return bounds;
  }

  /** Returns a random value from this distribution. */
  public long nextValue() {
    return sampler.getAsLong();
  }

  /**
   * Returns bounds that are useful for a range query over this distribution. They may contain, be
   * contained by, overlap, or be adjacent to or far from {@link #bounds()}.
   */
  public Bounds nextBound() {
    return switch (random.nextInt(6)) {
      case 0 -> nextContainingBound();
      case 1 -> nextContainedBound();
      case 2, 3 -> nextOverlappingBound();
      case 4 -> nextDisjointBound();
      default -> {
        long a = random.nextLong();
        long b = random.nextLong();
        yield new Bounds(Math.min(a, b), Math.max(a, b));
      }
    };
  }

  /** Returns bounds that contain all of {@link #bounds()}, and may be equal to it. */
  public Bounds nextContainingBound() {
    return new Bounds(nextAtOrBelow(bounds.min()), nextAtOrAbove(bounds.max()));
  }

  /** Returns bounds that are within {@link #bounds()}, and may be equal to it. */
  public Bounds nextContainedBound() {
    long a = nextPointWithin();
    long b = nextPointWithin();
    return new Bounds(Math.min(a, b), Math.max(a, b));
  }

  /** Returns bounds that share at least one value with {@link #bounds()}. */
  public Bounds nextOverlappingBound() {
    long shared = nextPointWithin();
    return new Bounds(nextAtOrBelow(shared), nextAtOrAbove(shared));
  }

  /** Returns bounds that share no value with {@link #bounds()}, if there is room for them. */
  private Bounds nextDisjointBound() {
    boolean roomBelow = bounds.min() != Long.MIN_VALUE;
    boolean roomAbove = bounds.max() != Long.MAX_VALUE;
    if (roomBelow && (roomAbove == false || random.nextBoolean())) {
      long upper = bounds.min() - 1;
      return new Bounds(nextAtOrBelow(upper), upper);
    }
    if (roomAbove) {
      long lower = bounds.max() + 1;
      return new Bounds(lower, nextAtOrAbove(lower));
    }
    return nextOverlappingBound();
  }

  private long nextPointWithin() {
    return switch (random.nextInt(5)) {
      case 0 -> bounds.min();
      case 1 -> bounds.max();
      case 2, 3 -> nextValue();
      default -> TestUtil.nextLong(random, bounds.min(), bounds.max());
    };
  }

  private long nextAtOrBelow(long value) {
    return switch (random.nextInt(4)) {
      case 0 -> value;
      case 1 -> value == Long.MIN_VALUE ? value : value - 1;
      case 2 -> Long.MIN_VALUE;
      default -> TestUtil.nextLong(random, Long.MIN_VALUE, value);
    };
  }

  private long nextAtOrAbove(long value) {
    return switch (random.nextInt(4)) {
      case 0 -> value;
      case 1 -> value == Long.MAX_VALUE ? value : value + 1;
      case 2 -> Long.MAX_VALUE;
      default -> TestUtil.nextLong(random, value, Long.MAX_VALUE);
    };
  }
}
