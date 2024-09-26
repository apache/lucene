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

import java.util.concurrent.ThreadLocalRandom;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class FixedBitSetBenchmark {
  private FixedBitSet fixedBitSetA;
  private FixedBitSet fixedBitSetB;

  @Param({"1024", "10240", "102400", "1024000", "10240000", "102400000"})
  int size;

  @Setup(Level.Iteration)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    fixedBitSetA = new FixedBitSet(size);
    fixedBitSetB = new FixedBitSet(size);

    for (int i = 0; i < size; ++i) {
      if (random.nextBoolean()) {
        fixedBitSetA.set(i);
      }
      if (random.nextBoolean()) {
        fixedBitSetB.set(i);
      }
    }
  }

  @Benchmark
  public void fixedBitSetAnd() {
    fixedBitSetA.and(fixedBitSetB);
  }

  @Benchmark
  @Fork(
      value = 3,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public void fixedBitSetAndVector() {
    fixedBitSetA.and(fixedBitSetB);
  }

  @Benchmark
  public void fixedBitSetXor() {
    fixedBitSetA.xor(fixedBitSetB);
  }

  @Benchmark
  @Fork(
      value = 3,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public void fixedBitSetXorVector() {
    fixedBitSetA.xor(fixedBitSetB);
  }
}
