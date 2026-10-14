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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for {@code CharacterUtils.toLowerCase}. Compares the generic Unicode codepoint path
 * (codePointAt + toLowerCase + toChars per char) against an optimized ASCII fast path that uses
 * arithmetic bit manipulation for ASCII and only falls back to Unicode for non-ASCII characters.
 * Token lengths range from 3 to 10 characters.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class LowerCaseBenchmark {

  @Param({"1000"})
  int tokenCount;

  @Param({"english", "ascii75", "ascii50", "ascii25", "allNonAscii"})
  String distribution;

  private char[][] workBuffers;

  private int[] workLengths;

  private char[][] masterBuffers;

  private int[] masterLengths;

  @Setup(Level.Trial)
  public void setup() {
    Random rng = new Random(42);

    masterBuffers = new char[tokenCount][];
    masterLengths = new int[tokenCount];

    for (int t = 0; t < tokenCount; t++) {
      int len = 3 + rng.nextInt(8);
      masterBuffers[t] = generateToken(rng, len);
      masterLengths[t] = masterBuffers[t].length;
    }

    workBuffers = new char[tokenCount][];
    workLengths = new int[tokenCount];
    for (int t = 0; t < tokenCount; t++) {
      workBuffers[t] = new char[masterBuffers[t].length];
      workLengths[t] = masterLengths[t];
    }
  }

  private char[] generateToken(Random rng, int len) {
    return switch (distribution) {
      case "english" -> generateByAsciiPercent(rng, len, 0.98);
      case "ascii75" -> generateByAsciiPercent(rng, len, 0.75);
      case "ascii50" -> generateByAsciiPercent(rng, len, 0.50);
      case "ascii25" -> generateByAsciiPercent(rng, len, 0.25);
      case "allNonAscii" -> randomWithNonAscii(rng, len);
      default -> throw new IllegalArgumentException("Unknown distribution: " + distribution);
    };
  }

  private static char[] generateByAsciiPercent(Random rng, int len, double asciiRatio) {
    if (rng.nextDouble() < asciiRatio) {
      return rng.nextBoolean() ? randomLowercaseAscii(rng, len) : randomMixedCaseAscii(rng, len);
    } else {
      return randomWithNonAscii(rng, len);
    }
  }

  @Setup(Level.Invocation)
  public void resetBuffers() {
    for (int t = 0; t < tokenCount; t++) {
      System.arraycopy(masterBuffers[t], 0, workBuffers[t], 0, masterLengths[t]);
    }
  }

  @Benchmark
  public void baseline(Blackhole bh) {
    for (int t = 0; t < tokenCount; t++) {
      toLowerCaseBaseline(workBuffers[t], 0, workLengths[t]);
      bh.consume(workBuffers[t]);
    }
  }

  @Benchmark
  public void asciiFastPath(Blackhole bh) {
    for (int t = 0; t < tokenCount; t++) {
      toLowerCaseAsciiFastPath(workBuffers[t], 0, workLengths[t]);
      bh.consume(workBuffers[t]);
    }
  }

  static void toLowerCaseBaseline(char[] buffer, int offset, int limit) {
    for (int i = offset; i < limit; ) {
      i +=
          Character.toChars(
              Character.toLowerCase(Character.codePointAt(buffer, i, limit)), buffer, i);
    }
  }

  static void toLowerCaseAsciiFastPath(char[] buffer, int offset, int limit) {
    for (int i = offset; i < limit; i++) {
      char c = buffer[i];
      if (c > 127) {
        for (; i < limit; ) {
          i +=
              Character.toChars(
                  Character.toLowerCase(Character.codePointAt(buffer, i, limit)), buffer, i);
        }
        return;
      }
      if (c >= 'A' && c <= 'Z') {
        buffer[i] = (char) (c | 32);
      }
    }
  }

  private static char[] randomLowercaseAscii(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] = (char) ('a' + rng.nextInt(26));
    }
    return buf;
  }

  private static char[] randomMixedCaseAscii(Random rng, int len) {
    char[] buf = new char[len];
    buf[0] = (char) ('A' + rng.nextInt(26));
    for (int i = 1; i < len; i++) {
      buf[i] = (char) ('a' + rng.nextInt(26));
    }
    return buf;
  }

  private static char[] randomWithNonAscii(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      double r = rng.nextDouble();
      if (r < 0.4) {
        buf[i] = (char) ('a' + rng.nextInt(26));
      } else if (r < 0.7) {
        char c;
        do {
          c = (char) (0x00C0 + rng.nextInt(31));
        } while (c == 0x00D7);
        buf[i] = c;
      } else if (r < 0.9) {
        char c;
        do {
          c = (char) (0x00E0 + rng.nextInt(31));
        } while (c == 0x00F7);
        buf[i] = c;
      } else {
        buf[i] = (char) (0x4E00 + rng.nextInt(100));
      }
    }
    return buf;
  }
}
