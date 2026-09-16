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
import org.apache.lucene.analysis.CharacterUtils;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class CaseFoldBenchmark {

  @Param({"256"})
  int tokenCount;

  @Param({
    "english",
    "german",
    "russian",
    "turkish",
    "greek",
    "armenian",
    "japanese",
    "cherokee",
    "mixed"
  })
  String distribution;

  private char[][] workBuffers;
  private int[] workLengths;
  private char[][] masterBuffers;
  private int[] masterLengths;

  private static final char[] RUSSIAN_UPPER = new char[32];
  private static final char[] RUSSIAN_LOWER = new char[32];
  private static final char[] TURKISH_SPECIAL = {
    'İ', 'ı', 'Ş', 'ş', 'Ğ', 'ğ', 'Ç', 'ç', 'Ö', 'ö', 'Ü', 'ü'
  };
  private static final char[] ARMENIAN_UPPER = new char[38];
  private static final char[] ARMENIAN_LOWER = new char[38];
  private static final char[] JAPANESE_KATAKANA = new char[86];

  static {
    for (int i = 0; i < 32; i++) {
      RUSSIAN_UPPER[i] = (char) (0x0410 + i);
      RUSSIAN_LOWER[i] = (char) (0x0430 + i);
    }
    for (int i = 0; i < 38; i++) {
      ARMENIAN_UPPER[i] = (char) (0x0531 + i);
      ARMENIAN_LOWER[i] = (char) (0x0561 + i);
    }
    for (int i = 0; i < 86; i++) {
      JAPANESE_KATAKANA[i] = (char) (0x30A1 + i);
    }
  }

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
      case "english" -> generateAsciiDominant(rng, len, 0.98);
      case "german" -> generateGerman(rng, len);
      case "russian" -> generateRussian(rng, len);
      case "turkish" -> generateTurkish(rng, len);
      case "greek" -> generateGreek(rng, len);
      case "armenian" -> generateArmenian(rng, len);
      case "japanese" -> generateJapanese(rng, len);
      case "cherokee" -> generateCherokee(rng, len);
      case "mixed" -> generateMixed(rng, len);
      default -> throw new IllegalArgumentException(distribution);
    };
  }

  @Setup(Level.Invocation)
  public void resetBuffers() {
    for (int t = 0; t < tokenCount; t++) {
      System.arraycopy(masterBuffers[t], 0, workBuffers[t], 0, masterLengths[t]);
    }
  }

  @Benchmark
  public void simpleCaseFold(Blackhole bh) {
    for (int t = 0; t < tokenCount; t++) {
      CharacterUtils.simpleCaseFold(workBuffers[t], 0, workLengths[t]);
      bh.consume(workBuffers[t]);
    }
  }

  private static char[] generateAsciiDominant(Random rng, int len, double asciiRatio) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      if (rng.nextDouble() < asciiRatio) {
        buf[i] =
            rng.nextBoolean() ? (char) ('a' + rng.nextInt(26)) : (char) ('A' + rng.nextInt(26));
      } else {
        buf[i] = (char) (0x00C0 + rng.nextInt(30)); // Latin Extended
      }
    }
    return buf;
  }

  private static char[] generateGerman(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      double r = rng.nextDouble();
      if (r < 0.80) {
        buf[i] =
            rng.nextBoolean() ? (char) ('a' + rng.nextInt(26)) : (char) ('A' + rng.nextInt(26));
      } else if (r < 0.95) {
        char[] umlauts = {'ä', 'ö', 'ü', 'Ä', 'Ö', 'Ü', 'ß'};
        buf[i] = umlauts[rng.nextInt(umlauts.length)];
      } else {
        buf[i] = (char) (0x00C0 + rng.nextInt(30)); // Latin Extended
      }
    }
    return buf;
  }

  private static char[] generateRussian(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      if (rng.nextDouble() < 0.45) {
        buf[i] = RUSSIAN_UPPER[rng.nextInt(RUSSIAN_UPPER.length)];
      } else {
        buf[i] = RUSSIAN_LOWER[rng.nextInt(RUSSIAN_LOWER.length)];
      }
    }
    return buf;
  }

  private static char[] generateTurkish(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      double r = rng.nextDouble();
      if (r < 0.70) {
        buf[i] =
            rng.nextBoolean() ? (char) ('a' + rng.nextInt(26)) : (char) ('A' + rng.nextInt(26));
      } else if (r < 0.90) {
        buf[i] = TURKISH_SPECIAL[rng.nextInt(TURKISH_SPECIAL.length)];
      } else {
        buf[i] = (char) (0x00C0 + rng.nextInt(30)); // Latin Extended
      }
    }
    return buf;
  }

  private static char[] generateGreek(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      double r = rng.nextDouble();
      if (r < 0.45) {
        buf[i] = (char) (0x0391 + rng.nextInt(25)); // Greek uppercase
      } else if (r < 0.90) {
        buf[i] = (char) (0x03B1 + rng.nextInt(25)); // Greek lowercase
      } else {
        char[] specials = {'ς', 'ϐ', 'ϑ', 'ϕ', 'ϖ', 'ϰ', 'ϱ', 'ϵ', 'µ'}; // fold exceptions
        buf[i] = specials[rng.nextInt(specials.length)];
      }
    }
    return buf;
  }

  private static char[] generateArmenian(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] =
          rng.nextBoolean()
              ? ARMENIAN_UPPER[rng.nextInt(ARMENIAN_UPPER.length)]
              : ARMENIAN_LOWER[rng.nextInt(ARMENIAN_LOWER.length)];
    }
    return buf;
  }

  private static char[] generateJapanese(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] = JAPANESE_KATAKANA[rng.nextInt(JAPANESE_KATAKANA.length)];
    }
    return buf;
  }

  private static char[] generateCherokee(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      if (rng.nextBoolean()) {
        buf[i] = (char) (0x13A0 + rng.nextInt(86)); // Cherokee uppercase
      } else {
        buf[i] = (char) (0xAB70 + rng.nextInt(80)); // Cherokee small
      }
    }
    return buf;
  }

  private static char[] generateMixed(Random rng, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      double r = rng.nextDouble();
      if (r < 0.40) {
        buf[i] =
            rng.nextBoolean() ? (char) ('a' + rng.nextInt(26)) : (char) ('A' + rng.nextInt(26));
      } else if (r < 0.60) {
        buf[i] = (char) (0x00C0 + rng.nextInt(60)); // Latin Extended
      } else if (r < 0.80) {
        buf[i] = (char) (0x0391 + rng.nextInt(50)); // Greek
      } else {
        buf[i] = (char) (0x0410 + rng.nextInt(64)); // Cyrillic
      }
    }
    return buf;
  }
}
