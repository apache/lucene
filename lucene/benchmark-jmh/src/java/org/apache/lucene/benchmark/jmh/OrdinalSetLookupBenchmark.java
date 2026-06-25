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
package org.apache.lucene.benchmark.jmh;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.util.LongBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures per-lookup cost of LongBitSet.get() vs LongHashSet.contains() for ordinal membership
 * testing, as used by DocValuesRangeIterator.forOrdinalSet() on the per-doc hot path.
 *
 * <p>The ordinal space (ordCount) represents total unique values in a segment. The match count
 * represents how many ordinals a MultiTermQuery actually matches. Lookups simulate the per-doc check
 * where the doc's ordinal is tested against the set.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, warmups = 1)
public class OrdinalSetLookupBenchmark {

  private static final int LOOKUP_COUNT = 4096;

  @Param({"1000", "100000", "10000000"})
  int ordCount;

  @Param({"10", "100", "1000"})
  int matchCount;

  private LongBitSet bitSet;
  private LongHashSet hashSet;
  private long[] lookups;

  @Setup(Level.Trial)
  public void setup() {
    Random rng = new Random(42);

    long[] matchingOrds = new long[matchCount];
    for (int i = 0; i < matchCount; i++) {
      matchingOrds[i] = (long) (rng.nextDouble() * ordCount);
    }

    bitSet = new LongBitSet(ordCount);
    hashSet = new LongHashSet(matchCount);
    for (long ord : matchingOrds) {
      bitSet.set(ord);
      hashSet.add(ord);
    }

    lookups = new long[LOOKUP_COUNT];
    for (int i = 0; i < LOOKUP_COUNT; i++) {
      if (rng.nextInt(4) == 0 && matchCount > 0) {
        lookups[i] = matchingOrds[rng.nextInt(matchCount)];
      } else {
        lookups[i] = (long) (rng.nextDouble() * ordCount);
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(LOOKUP_COUNT)
  public int bitSetLookup() {
    int hits = 0;
    for (long ord : lookups) {
      if (bitSet.get(ord)) {
        hits++;
      }
    }
    return hits;
  }

  @Benchmark
  @OperationsPerInvocation(LOOKUP_COUNT)
  public int hashSetLookup() {
    int hits = 0;
    for (long ord : lookups) {
      if (hashSet.contains(ord)) {
        hits++;
      }
    }
    return hits;
  }
}
