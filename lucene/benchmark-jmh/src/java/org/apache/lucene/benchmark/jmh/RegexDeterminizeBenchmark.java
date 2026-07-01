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

import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
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

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class RegexDeterminizeBenchmark {

  @Param({
    "^(a)?a",
    "((a|b)?b)+",
    "(aaa)?aaa",
    "^(a(b(c)?)?)?abc",
    "(a+)+",
    "(a*)+",
    "(b+)+",
    "(|f)?+",
    "(y+)*",
    "(foo|foobar)*",
    "(aa+|bb+)+"
  })
  public String regexPattern;

  @Param({"10000"})
  public int workLimit;

  private Automaton automaton;

  @Setup(Level.Trial)
  public void setup() {
    RegExp regex = new RegExp(regexPattern);
    automaton = regex.toAutomaton();
  }

  @Benchmark
  public Automaton determinize() {
    return Operations.determinize(automaton, workLimit);
  }
}
