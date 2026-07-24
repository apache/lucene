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
import org.apache.lucene.util.UnicodeUtil;
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

/** Compares Character.toLowerCase (current Lucene default) against flat table case folding. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class CaseFoldBenchmark {

  private static final int TOKEN_COUNT = 256;
  private static final int TOKEN_LENGTH = 8;

  @Param({"english", "german", "russian", "greek", "mixed"})
  String distribution;

  private char[][] tokens;

  @Setup(Level.Trial)
  public void setup() {
    Random rng = new Random(42);
    tokens = new char[TOKEN_COUNT][];
    for (int t = 0; t < TOKEN_COUNT; t++) {
      char[] buf = new char[TOKEN_LENGTH];
      for (int i = 0; i < TOKEN_LENGTH; i++) {
        buf[i] = generateChar(rng);
      }
      tokens[t] = buf;
    }
  }

  private static char nextAsciiLetter(Random rng) {
    return (char) ('A' + rng.nextInt(26) + (rng.nextBoolean() ? 32 : 0));
  }

  private char generateChar(Random rng) {
    return switch (distribution) {
      case "english" -> nextAsciiLetter(rng);
      case "german" -> {
        if (rng.nextInt(4) == 0) {
          yield "äöüßÄÖÜ".charAt(rng.nextInt(7));
        }
        yield nextAsciiLetter(rng);
      }
      case "russian" -> (char) (0x0410 + rng.nextInt(64));
      case "greek" -> {
        int base = rng.nextBoolean() ? 0x0391 : 0x03B1;
        yield (char) (base + rng.nextInt(25));
      }
      case "mixed" -> {
        if (rng.nextBoolean()) {
          yield nextAsciiLetter(rng);
        }
        yield (char) (0x0410 + rng.nextInt(64));
      }
      default -> throw new IllegalArgumentException(distribution);
    };
  }

  @Benchmark
  public int baseline() {
    int sum = 0;
    for (char[] token : tokens) {
      for (int i = 0; i < token.length; ) {
        int cp = Character.codePointAt(token, i, token.length);
        sum += Character.toLowerCase(cp);
        i += Character.charCount(cp);
      }
    }
    return sum;
  }

  @Benchmark
  public int foldTable() {
    int sum = 0;
    for (char[] token : tokens) {
      for (int i = 0; i < token.length; ) {
        int cp = Character.codePointAt(token, i, token.length);
        sum += UnicodeUtil.foldCase(cp);
        i += Character.charCount(cp);
      }
    }
    return sum;
  }
}
