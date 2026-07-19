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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CompiledAutomatonBenchmark {

  @Param({"5", "60"})
  public int numTransitions;

  private CompiledAutomaton compiled;
  private BytesRef[] inputs;
  private BytesRefBuilder output;
  private int index;

  @Setup
  public void setup() throws IOException {
    Automaton.Builder builder = new Automaton.Builder();
    int state0 = builder.createState();
    int destState = builder.createState();
    builder.setAccept(destState, true);
    // Add transitions to state 0 with sorted labels
    for (int i = 0; i < numTransitions; i++) {
      builder.addTransition(state0, destState, i * 2, i * 2);
    }
    Automaton automaton = builder.finish();
    compiled = new CompiledAutomaton(automaton);

    Random rand = new Random(42);
    inputs = new BytesRef[1000];
    for (int i = 0; i < inputs.length; i++) {
      // Pick a random odd label that falls within our transitions' ranges but always misses
      int label = rand.nextInt(numTransitions) * 2 + 1;
      inputs[i] = new BytesRef(new byte[]{(byte) label});
    }
    output = new BytesRefBuilder();
    index = 0;
  }

  @Benchmark
  public BytesRef benchmarkFloor() {
    BytesRef input = inputs[index];
    index = (index + 1) % inputs.length;
    return compiled.floor(input, output);
  }
}
