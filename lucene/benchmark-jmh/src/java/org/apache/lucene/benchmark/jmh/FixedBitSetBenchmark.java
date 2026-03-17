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

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.FixedBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g", "-XX:+AlwaysPreTouch"})
public class FixedBitSetBenchmark {

  // Includes sizes below/at/above the vectorization threshold (64 longs = 4096 bits).
  @Param({"512", "1024", "4096", "65536", "1048576"})
  public int numBits;

  @Param({"0.01", "0.10", "0.50"})
  public double density;

  private FixedBitSet a;

  @Setup(Level.Trial)
  public void setup() {
    a = new FixedBitSet(numBits);
    fillRandom(a, 0xC0FFEE, density);
  }

  @Benchmark
  public int cardinalityScalar() {
    return a.cardinality();
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int cardinalityVector() {
    return a.cardinality();
  }

  private static void fillRandom(FixedBitSet set, long seed, double density) {
    SplittableRandom rnd = new SplittableRandom(seed);
    long[] bits = set.getBits();

    if (density <= 0d) {
      return;
    }
    if (density >= 1d) {
      for (int i = 0; i < bits.length; i++) {
        bits[i] = -1L;
      }
    } else if (density == 0.5d) {
      for (int i = 0; i < bits.length; i++) {
        bits[i] = rnd.nextLong();
      }
    } else {
      for (int i = 0; i < bits.length; i++) {
        long w = 0;
        for (int bit = 0; bit < Long.SIZE; bit++) {
          if (rnd.nextDouble() < density) {
            w |= 1L << bit;
          }
        }
        bits[i] = w;
      }
    }

    int lastBits = set.length() & 63;
    if (lastBits != 0) {
      bits[bits.length - 1] &= (1L << lastBits) - 1;
    }
  }
}
