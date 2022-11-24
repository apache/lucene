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
package org.apache.lucene.util;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BaseSortTestCase.Entry;
import org.apache.lucene.util.BaseSortTestCase.Strategy;

/**
 * Benchmark for {@link Selector} implementations.
 *
 * <p>Run the static {@link #main(String[])} method to start the benchmark.
 */
public class SelectorBenchmark {

  private static final int ARRAY_LENGTH = 20000;
  private static final int RUNS = 10;
  private static final int LOOPS = 800;

  private enum SelectorFactory {
    INTRO_SELECTOR(
        "IntroSelector",
        (arr, s) -> {
          return new IntroSelector() {

            Entry pivot;

            @Override
            protected void swap(int i, int j) {
              ArrayUtil.swap(arr, i, j);
            }

            @Override
            protected void setPivot(int i) {
              pivot = arr[i];
            }

            @Override
            protected int comparePivot(int j) {
              return pivot.compareTo(arr[j]);
            }
          };
        }),
    ;
    final String name;
    final Builder builder;

    SelectorFactory(String name, Builder builder) {
      this.name = name;
      this.builder = builder;
    }

    interface Builder {
      Selector build(Entry[] arr, Strategy strategy);
    }
  }

  public static void main(String[] args) throws Exception {
    assert false : "Disable assertions to run the benchmark";
    Random random = new Random(System.nanoTime());
    long seed = random.nextLong();

    System.out.println("SEED: " + seed);
    System.out.println("WARMUP");
    benchmarkSelectors(Strategy.RANDOM, random, seed);
    System.out.println();

    for (Strategy strategy : Strategy.values()) {
      System.out.println(strategy);
      benchmarkSelectors(strategy, random, seed);
    }
  }

  private static void benchmarkSelectors(Strategy strategy, Random random, long seed) {
    for (SelectorFactory selectorFactory : SelectorFactory.values()) {
      System.out.printf(Locale.ROOT, "  %-15s...", selectorFactory.name);
      random.setSeed(seed);
      benchmarkSelector(strategy, selectorFactory, random);
      System.out.println();
    }
  }

  private static void benchmarkSelector(
      Strategy strategy, SelectorFactory selectorFactory, Random random) {
    for (int i = 0; i < RUNS; i++) {
      Entry[] original = createArray(strategy, random);
      Entry[] clone = original.clone();
      Selector selector = selectorFactory.builder.build(clone, strategy);
      long startTimeNs = System.nanoTime();
      int k = random.nextInt(clone.length);
      int kIncrement = random.nextInt(clone.length / 14) * 2 + 1;
      for (int j = 0; j < LOOPS; j++) {
        System.arraycopy(original, 0, clone, 0, original.length);
        selector.select(0, clone.length, k);
        k += kIncrement;
        if (k >= clone.length) {
          k -= clone.length;
        }
      }
      System.out.printf(
          Locale.ROOT, "%5d", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs));
    }
  }

  private static Entry[] createArray(Strategy strategy, Random random) {
    Entry[] arr = new Entry[ARRAY_LENGTH];
    for (int i = 0; i < arr.length; ++i) {
      strategy.set(arr, i, random);
    }
    return arr;
  }
}
