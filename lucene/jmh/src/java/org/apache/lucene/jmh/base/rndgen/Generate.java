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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.fst.PairOutputs.Pair;

/** The type RndGenerate. */
public class Generate {

  private Generate() {}

  /**
   * Constant RndGen.
   *
   * @param <T> the type parameter
   * @param constant the constant
   * @return the RndGen
   */
  public static <T> RndGen<T> constant(T constant) {
    return new RndGen<>() {
      @Override
      public T gen(RandomnessSource in) {
        return constant;
      }
    };
  }

  /**
   * Range RndGen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the RndGen
   */
  public static RndGen<Integer> range(final int startInclusive, final int endInclusive) {
    return new RndGen<Integer>() {
      {
        this.start = startInclusive;
        this.end = endInclusive;
      }

      @Override
      public Integer gen(RandomnessSource in) {

        int val = (int) (in.withDistribution(this.distribution)).next(startInclusive, endInclusive);
        super.processRndValue(val, in);
        return val;
      }
    }.describedAs("Range:" + startInclusive + ':' + endInclusive);
  }

  /**
   * Long range RndGen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the RndGen
   */
  public static RndGen<Long> longRange(final long startInclusive, final long endInclusive) {
    return new RndGen<>() {
      {
        this.start = startInclusive;
        this.end = endInclusive;
      }

      @Override
      public Long gen(RandomnessSource in) {
        long val = in.next(startInclusive, endInclusive);
        super.processRndValue(val, in);
        return val;
      }
    };
  }

  /**
   * Int arrays RndGen.
   *
   * @param sizes the sizes
   * @param contents the contents
   * @return the RndGen
   */
  public static RndGen<int[]> intArrays(RndGen<Integer> sizes, RndGen<Integer> contents) {
    return new RndGen<>() {

      @Override
      public int[] gen(RandomnessSource td) {
        int size = sizes.generate(td);
        int[] is = new int[size];
        for (int i = 0; i != size; i++) {
          is[i] = contents.generate(td);
        }
        return is;
      }
    };
  }

  /**
   * Code points RndGen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the RndGen
   */
  public static RndGen<Integer> codePoints(int startInclusive, int endInclusive) {

    SourceDSL.checkArguments(
        startInclusive >= Character.MIN_CODE_POINT,
        "(%s) is less than the minimum codepoint (%s)",
        startInclusive,
        Character.MIN_CODE_POINT);

    SourceDSL.checkArguments(
        endInclusive <= Character.MAX_CODE_POINT,
        "%s is greater than the maximum codepoint (%s)",
        endInclusive,
        Character.MAX_CODE_POINT);

    return Generate.range(startInclusive, endInclusive);
  }

  /**
   * Returns a generator that provides a value from a generator chosen with probability in
   * proportion to the weight supplied in the {@link Pair}. Shrinking is towards the first non-zero
   * weight in the list. At least one generator must have a positive weight and non-positive
   * generators will never be chosen.
   *
   * @param <T> Type to generate
   * @param mandatory Generator to sample from
   * @param others Other generators to sample
   * @return A gen of T
   */
  @SafeVarargs
  public static <T> RndGen<T> frequency(
      Pair<Integer, RndGen<T>> mandatory, Pair<Integer, RndGen<T>>... others) {

    return frequency(FrequencyGen.makeGeneratorList(mandatory, others));
  }

  /**
   * Returns a generator that provides a value from a generator chosen with probability in
   * proportion to the weight supplied in the {@link Pair}. Shrinking is towards the first non-zero
   * weight in the list. At least one generator must have a positive weight and non-positive
   * generators will never be chosen.
   *
   * @param <T> Type to generate
   * @param weightedGens pairs of weight and generators to sample in proportion to their weighting
   * @return A gen of T
   */
  public static <T> RndGen<T> frequency(List<Pair<Integer, RndGen<T>>> weightedGens) {
    return FrequencyGen.fromList(weightedGens);
  }

  /**
   * Booleans RndGen.
   *
   * @return the RndGen
   */
  public static RndGen<Boolean> booleans() {
    return Generate.pick(Arrays.asList(false, true));
  }

  /**
   * Randomly returns one of the supplied values
   *
   * @param <T> type of value to generate *
   * @param ts Values to pick from
   * @return A Gen of T
   */
  public static <T> RndGen<T> pick(List<T> ts) {
    RndGen<Integer> index = range(0, ts.size() - 1);
    return new RndGen<T>() {
      @Override
      public T gen(RandomnessSource in) {
        return ts.get(index.generate(in));
      }
    };
  }

  /**
   * Returns a generator that provides a value from a random generator provided.
   *
   * @param <T> Type to generate
   * @param mandatory Generator to sample from
   * @param others Other generators to sample from with equal weighting
   * @return A gen of T
   */
  @SafeVarargs
  public static <T> RndGen<T> oneOf(RndGen<T> mandatory, RndGen<T>... others) {
    RndGen<T>[] generators = ArrayUtil.copyOfSubArray(others, 0, others.length + 1);
    generators[generators.length - 1] = mandatory;
    RndGen<Integer> index = range(0, generators.length - 1);
    return new RndGen<T>() {
      @Override
      public T gen(RandomnessSource in) {
        return generators[(index.generate(in))].generate(in);
      }
    };
  }

  /**
   * The type Frequency gen.
   *
   * @param <T> the type parameter
   */
  static class FrequencyGen<T> extends RndGen<T> {
    private final NavigableMap<Integer, RndGen<T>> weightedMap;
    private final RndGen<Integer> indexGen;

    private FrequencyGen(RndGen<Integer> indexGen, NavigableMap<Integer, RndGen<T>> weightedMap) {
      this.weightedMap = weightedMap;
      this.indexGen = indexGen;
    }

    /**
     * Make generator list list.
     *
     * @param <T> the type parameter
     * @param mandatory the mandatory
     * @param others the others
     * @return the list
     */
    static <T> List<Pair<Integer, RndGen<T>>> makeGeneratorList(
        Pair<Integer, RndGen<T>> mandatory, Pair<Integer, RndGen<T>>[] others) {
      List<Pair<Integer, RndGen<T>>> ts = new ArrayList<>(others.length + 1);
      ts.add(mandatory);
      Collections.addAll(ts, others);
      return ts;
    }

    /**
     * From list RndGenerate . frequency gen.
     *
     * @param <T> the type parameter
     * @param ts the ts
     * @return the RndGenerate . frequency gen
     */
    /* First the generator normalizes the weights and their total by the greatest common factor
     * to keep integers small and improve the chance of moving to a new generator as the
     * index generator shrinks.
     *
     * Then assigns each generator a range of integers according to their normalized weight
     * For example, with three normalized weighted generators {3, g1}, {4, g2}, {5, g3},
     * total 12 it assigns [0, 2] to g1, [3, 6] to g2, [7, 11] to g3.
     *
     * At generation time, the generator picks an integer between [0, total weight) and finds
     * the generator responsible for the range.
     */
    static <T> FrequencyGen<T> fromList(List<Pair<Integer, RndGen<T>>> ts) {
      if (ts.isEmpty()) {
        throw new IllegalArgumentException("List of generators must not be empty");
      }
      /* Calculate the total unadjusted weights, and the largest common factor
       * between all the weights and the total, so they can be reduced to the smallest.
       * It ignores non-positive weights to make it easy to disable generators while developing
       * properties.
       */
      long unadjustedTotalWeights = 0;
      long commonFactor = 0;
      for (Pair<Integer, RndGen<T>> pair : ts) {
        int weight = pair.output1;
        if (weight <= 0) continue;

        if (unadjustedTotalWeights == 0) {
          commonFactor = weight;
        } else {
          commonFactor = gcd(commonFactor, weight);
        }
        unadjustedTotalWeights += weight;
      }
      if (unadjustedTotalWeights == 0) {
        throw new IllegalArgumentException("At least one generator must have a positive weight");
      }
      commonFactor = gcd(commonFactor, unadjustedTotalWeights);

      /* Build a tree map with the key as the first integer assigned in the range,
       * floorEntry will pick the 'the greatest key less than or equal to the given key',
       * which will find the right generator for the range.
       */
      NavigableMap<Integer, RndGen<T>> weightedMap = new TreeMap<>();
      int nextStart = 0;
      for (Pair<Integer, RndGen<T>> pair : ts) {
        int weight = pair.output1;
        if (weight <= 0) continue;
        try {
          weightedMap.put(nextStart, pair.output2);
        } catch (ClassCastException e) {
          throw new ClassCastException(
              "Class cast exception for "
                  + pair.output1
                  + ','
                  + pair.output2
                  + ' '
                  + e.getMessage());
        }
        nextStart += weight / commonFactor;
      }
      final int upperRange = (int) (unadjustedTotalWeights / commonFactor) - 1;

      RndGen<Integer> indexGen = range(0, upperRange);

      return new FrequencyGen<>(indexGen, weightedMap);
    }

    @Override
    public T gen(RandomnessSource prng) {
      return weightedMap.floorEntry(indexGen.generate(prng)).getValue().generate(prng);
    }

    @Override
    public String asString(T t) {
      return weightedMap.get(0).asString(t);
    }

    private static long gcd(long a, long b) {
      return BigInteger.valueOf(a).gcd(BigInteger.valueOf(b)).longValue();
    }
  }
}
