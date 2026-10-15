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
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
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

/**
 * Byte vector kernels measured in a JVM where the other variants have already been compiled. Each
 * kernel is called from its own compiled method, as the vector scorers call them, so the result
 * does not depend on how many kernels C2 can inline into one benchmark method.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"},
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class VectorUtilMixingBenchmark {

  @Param({"1024"})
  int size;

  /** "none": measure each kernel in a fresh JVM; "all": compile every variant first. */
  @Param({"none", "all"})
  String pollute;

  byte[] bytesA;
  byte[] bytesB;
  long sink;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    bytesA = new byte[size];
    bytesB = new byte[size];
    random.nextBytes(bytesA);
    random.nextBytes(bytesB);
    if (pollute.equals("all")) {
      // run each variant long enough for C2 to compile it, one after the other; a few calls
      // only reach the interpreter and do not reproduce problems that need compiled code
      for (int round = 0; round < 2; round++) {
        sink += warmDotProduct(100_000);
        sink += warmUint8DotProduct(100_000);
        sink += warmSquareDistance(100_000);
        sink += warmUint8SquareDistance(100_000);
      }
    }
  }

  private long warmDotProduct(int n) {
    long sum = 0;
    for (int i = 0; i < n; i++) sum += VectorUtil.dotProduct(bytesA, bytesB);
    return sum;
  }

  private long warmUint8DotProduct(int n) {
    long sum = 0;
    for (int i = 0; i < n; i++) sum += VectorUtil.uint8DotProduct(bytesA, bytesB);
    return sum;
  }

  private long warmSquareDistance(int n) {
    long sum = 0;
    for (int i = 0; i < n; i++) sum += VectorUtil.squareDistance(bytesA, bytesB);
    return sum;
  }

  private long warmUint8SquareDistance(int n) {
    long sum = 0;
    for (int i = 0; i < n; i++) sum += VectorUtil.uint8SquareDistance(bytesA, bytesB);
    return sum;
  }

  @Benchmark
  public int dotProduct() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @Benchmark
  public int uint8DotProduct() {
    return VectorUtil.uint8DotProduct(bytesA, bytesB);
  }

  @Benchmark
  public int squareDistance() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  public int uint8SquareDistance() {
    return VectorUtil.uint8SquareDistance(bytesA, bytesB);
  }

  /** All four kernels per call, each from its own compiled method. */
  @Benchmark
  public int allKernels() {
    return callDotProduct()
        + callUint8DotProduct()
        + callSquareDistance()
        + callUint8SquareDistance();
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private int callDotProduct() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private int callUint8DotProduct() {
    return VectorUtil.uint8DotProduct(bytesA, bytesB);
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private int callSquareDistance() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private int callUint8SquareDistance() {
    return VectorUtil.uint8SquareDistance(bytesA, bytesB);
  }
}
