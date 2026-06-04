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
import org.apache.lucene.util.quantization.HadamardRotation;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class HadamardRotationBenchmark {

  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  int size;

  private float[] input;
  private float[] output;
  private float[] scratch;
  private HadamardRotation rotation;

  @Setup(Level.Iteration)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    input = new float[size];
    output = new float[size];
    scratch = new float[size];
    for (int i = 0; i < size; i++) {
      input[i] = random.nextFloat() * 2 - 1;
    }
    rotation = HadamardRotation.forDimension(size);
  }

  @Benchmark
  public float[] rotate() {
    rotation.rotate(input, output);
    return output;
  }

  @Benchmark
  public float[] inverseRotate() {
    rotation.inverseRotate(input, output, scratch);
    return output;
  }
}
