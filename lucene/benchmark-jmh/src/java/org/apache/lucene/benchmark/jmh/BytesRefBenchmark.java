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
import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@Fork(1)
@Threads(4)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class BytesRefBenchmark {

  @Param({"10", "100", "1000"})
  private int length;

  @Param({"ASCII", "ISO_8859_1", "UTF_8_BMP", "UTF_16"})
  private Type type;

  private String string;
  private CharSequence charSequence;

  @Setup
  public void before() {
    string = generateCodepoints(length, type.maxCodePoint);
    charSequence = string;
  }

  @Benchmark
  public BytesRef bytesRefCharSequence() {
    return new BytesRef(charSequence);
  }

  @Benchmark
  public BytesRef bytesRefString() {
    return new BytesRef(string);
  }

  private static String generateCodepoints(int size, int maxCodePoint) {
    Random r = new Random(0);
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.appendCodePoint(r.nextInt(maxCodePoint));
    }
    return sb.toString();
  }

  public enum Type {
    ASCII(128),
    ISO_8859_1(256),
    UTF_8_BMP(0xFFFF),
    UTF_16(0x10FFFF),
    ;

    private final int maxCodePoint;

    Type(int maxCodepoint) {
      this.maxCodePoint = maxCodepoint;
    }
  }

  public static void main(String[] _args) throws Exception {
    new Runner(
            new OptionsBuilder()
                .include(BytesRefBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build())
        .run();
  }
}
