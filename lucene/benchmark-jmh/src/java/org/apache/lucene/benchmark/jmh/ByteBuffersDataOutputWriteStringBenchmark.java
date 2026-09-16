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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.lucene.store.ByteBuffersDataOutput;
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
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class ByteBuffersDataOutputWriteStringBenchmark {

  private static final int STRING_POOL_SIZE = 8192;

  @Param({
    "ascii_1",
    "ascii_10",
    "ascii_20",
    "ascii_30",
    "ascii_40",
    "ascii_medium",
    "ascii_long",
    "ascii_vlarge",
    "cjk_1",
    "cjk_10",
    "cjk_20",
    "cjk_30",
    "cjk_40",
    "cjk_medium",
    "cjk_long",
    "cjk_vlarge",
    "latin_ext_1",
    "latin_ext_10",
    "latin_ext_20",
    "latin_ext_30",
    "latin_ext_40",
    "latin_ext_medium",
    "latin_ext_long",
    "latin_ext_vlarge",
    "mixed"
  })
  public String stringType;

  /** Target bytes to write per invocation. */
  @Param({"81920", "491520", "2097152"})
  public int targetBytes;

  /** Pre-generated strings to write. */
  private String[] testStrings;

  /** Number of strings to write per invocation to reach targetBytes total output. */
  private int stringsPerInvocation;

  private ByteBuffersDataOutput reusableOutput;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    testStrings = new String[STRING_POOL_SIZE];

    Supplier<String> generator =
        switch (stringType) {
          case "ascii_1" -> () -> randomAscii(random, 1);
          case "ascii_10" -> () -> randomAscii(random, 8 + random.nextInt(5));
          case "ascii_20" -> () -> randomAscii(random, 18 + random.nextInt(5));
          case "ascii_30" -> () -> randomAscii(random, 28 + random.nextInt(5));
          case "ascii_40" -> () -> randomAscii(random, 38 + random.nextInt(5));
          case "ascii_medium" -> () -> randomAscii(random, 50 + random.nextInt(100));
          case "ascii_long" -> () -> randomAscii(random, 900 + random.nextInt(250));
          case "ascii_vlarge" -> () -> randomAscii(random, 7000 + random.nextInt(2400));
          case "cjk_1" -> () -> randomCjk(random, 1);
          case "cjk_10" -> () -> randomCjk(random, 8 + random.nextInt(5));
          case "cjk_20" -> () -> randomCjk(random, 18 + random.nextInt(5));
          case "cjk_30" -> () -> randomCjk(random, 28 + random.nextInt(5));
          case "cjk_40" -> () -> randomCjk(random, 38 + random.nextInt(5));
          case "cjk_medium" -> () -> randomCjk(random, 50 + random.nextInt(100));
          case "cjk_long" -> () -> randomCjk(random, 400 + random.nextInt(200));
          case "cjk_vlarge" -> () -> randomCjk(random, 5500 + random.nextInt(1000));
          case "latin_ext_1" -> () -> randomLatinExtended(random, 1);
          case "latin_ext_10" -> () -> randomLatinExtended(random, 8 + random.nextInt(5));
          case "latin_ext_20" -> () -> randomLatinExtended(random, 18 + random.nextInt(5));
          case "latin_ext_30" -> () -> randomLatinExtended(random, 28 + random.nextInt(5));
          case "latin_ext_40" -> () -> randomLatinExtended(random, 38 + random.nextInt(5));
          case "latin_ext_medium" -> () -> randomLatinExtended(random, 50 + random.nextInt(100));
          case "latin_ext_long" -> () -> randomLatinExtended(random, 400 + random.nextInt(200));
          case "latin_ext_vlarge" -> () -> randomLatinExtended(random, 5500 + random.nextInt(1000));
          case "mixed" ->
              () -> {
                // Varying lengths
                int roll = random.nextInt(100);
                if (roll < 50) {
                  return randomAscii(random, 3 + random.nextInt(30));
                } else if (roll < 65) {
                  return randomAscii(random, 50 + random.nextInt(100));
                } else if (roll < 75) {
                  return randomAscii(random, 500 + random.nextInt(500));
                } else if (roll < 85) {
                  return randomCjk(random, 5 + random.nextInt(20));
                } else if (roll < 95) {
                  return randomLatinExtended(random, 20 + random.nextInt(60));
                } else {
                  return randomCjk(random, 200 + random.nextInt(300));
                }
              };
          default -> throw new IllegalArgumentException("Unknown stringType: " + stringType);
        };

    long totalBytes = 0;
    for (int i = 0; i < STRING_POOL_SIZE; i++) {
      testStrings[i] = generator.get();
      totalBytes += testStrings[i].getBytes(StandardCharsets.UTF_8).length;
    }
    int avgBytesPerString = Math.max(1, (int) (totalBytes / STRING_POOL_SIZE));

    stringsPerInvocation = targetBytes / avgBytesPerString;

    reusableOutput = ByteBuffersDataOutput.newResettableInstance();
  }

  private ByteBuffersDataOutput getOutput() {
    reusableOutput.reset();
    return reusableOutput;
  }

  @Benchmark
  public void writeString(Blackhole bh) {
    ByteBuffersDataOutput output = getOutput();
    for (int i = 0; i < stringsPerInvocation; i++) {
      output.writeString(testStrings[i % STRING_POOL_SIZE]);
    }
    bh.consume(output.size());
  }

  private String randomAscii(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      chars[i] = (char) (32 + random.nextInt(95));
    }
    return new String(chars);
  }

  /**
   * Generates realistic CJK text: ~90% CJK Unified Ideographs (3-byte UTF-8), ~9% ASCII digits
   * (1-byte), ~1% surrogate pairs (emoji, rare CJK-B characters, 4-byte UTF-8).
   */
  private String randomCjk(Random random, int length) {
    char[] chars = new char[length + 1]; // +1 room for potential surrogate pair expansion
    int pos = 0;
    for (int i = 0; i < length && pos < chars.length - 1; i++) {
      int roll = random.nextInt(100);
      if (roll < 90) {
        // CJK Unified Ideographs: U+4E00–U+9FFF (3 bytes in UTF-8)
        chars[pos++] = (char) (0x4E00 + random.nextInt(0x9FFF - 0x4E00));
      } else if (roll < 99) {
        // ASCII digits
        chars[pos++] = (char) (0x30 + random.nextInt(10)); // 0-9
      } else {
        // Surrogate pair, 4 bytes UTF-8
        if (pos < chars.length - 1) {
          chars[pos++] = (char) (0xD800 + random.nextInt(0x400)); // high surrogate
          chars[pos++] = (char) (0xDC00 + random.nextInt(0x400)); // low surrogate
        } else {
          chars[pos++] = (char) (0x4E00 + random.nextInt(0x9FFF - 0x4E00));
        }
      }
    }
    return new String(chars, 0, pos);
  }

  private String randomLatinExtended(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      int roll = random.nextInt(100);
      if (roll < 80) {
        // 2-byte UTF-8
        chars[i] = (char) (0x0080 + random.nextInt(0x0700));
      } else if (roll < 95) {
        // ASCII
        chars[i] = (char) (0x41 + random.nextInt(26)); // A-Z
      } else {
        // 3-byte UTF-8
        chars[i] = (char) (0x2000 + random.nextInt(0xBFF));
      }
    }
    return new String(chars);
  }
}
