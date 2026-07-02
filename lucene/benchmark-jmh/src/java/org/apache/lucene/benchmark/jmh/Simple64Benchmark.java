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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.Simple64;
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

/** Benchmark comparing Simple64 vs VInt for encoding/decoding suffix lengths. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class Simple64Benchmark {
  @Param({"40"})
  int blockSize;

  @Param({"SMALL_1_8", "MID_1_64", "LARGE_1_200", "MIXED"})
  String distribution;

  // input
  int[] suffixLengths;

  // Simple64
  long[] encodedWords;
  int encodedWordCount;
  byte[] encodedWordBytes;
  int encodedWordBytesLength;
  int[] decodeOutSimple64;
  long[] decodeWordBuffer;

  // VInt
  byte[] encodedVInt;
  int encodedVIntLength;
  int[] decodeOutVInt;

  // encode output buffers
  long[] encodeOutWords;
  byte[] encodeOutWordBytes;
  byte[] encodeOutVIntBytes;
  ByteArrayDataOutput wordOut;
  ByteArrayDataOutput vintOut;
  ByteArrayDataInput wordIn;
  ByteArrayDataInput vintIn;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random rng = new Random(42);

    suffixLengths = new int[blockSize];
    for (int i = 0; i < blockSize; i++) {
      suffixLengths[i] = nextSuffixLength(rng);
    }

    // pre-encode Simple64
    encodedWords = new long[blockSize + 1];
    encodedWordCount = Simple64.encodeAll(suffixLengths, 0, blockSize, encodedWords, 0);
    decodeOutSimple64 = new int[blockSize + Simple64.COUNTS[0]];
    decodeWordBuffer = new long[blockSize + 1];
    encodeOutWords = new long[blockSize + 1];

    encodedWordBytes = new byte[(blockSize + 1) * Long.BYTES];
    ByteArrayDataOutput encodedWordOut = new ByteArrayDataOutput(encodedWordBytes);
    for (int i = 0; i < encodedWordCount; i++) {
      encodedWordOut.writeLong(encodedWords[i]);
    }
    encodedWordBytesLength = encodedWordOut.getPosition();

    // pre-encode VInt
    encodedVInt = new byte[blockSize * 5];
    ByteArrayDataOutput out = new ByteArrayDataOutput(encodedVInt);
    for (int i = 0; i < blockSize; i++) {
      out.writeVInt(suffixLengths[i]);
    }
    encodedVIntLength = out.getPosition();
    decodeOutVInt = new int[blockSize];

    encodeOutWordBytes = new byte[(blockSize + 1) * Long.BYTES];
    encodeOutVIntBytes = new byte[blockSize * 5];
    wordOut = new ByteArrayDataOutput(encodeOutWordBytes);
    vintOut = new ByteArrayDataOutput(encodeOutVIntBytes);
    wordIn = new ByteArrayDataInput(encodedWordBytes, 0, encodedWordBytesLength);
    vintIn = new ByteArrayDataInput(encodedVInt, 0, encodedVIntLength);
  }

  // ---- Simple64 ----

  @Benchmark
  public void encodeSimple64Words(Blackhole bh) {
    int n = Simple64.encodeAll(suffixLengths, 0, blockSize, encodeOutWords, 0);
    bh.consume(n);
    bh.consume(encodeOutWords[0]);
    bh.consume(encodeOutWords[n - 1]);
  }

  @Benchmark
  public void decodeSimple64Words(Blackhole bh) {
    Simple64.decodeAll(encodedWords, 0, decodeOutSimple64, 0, blockSize);
    bh.consume(checksum(decodeOutSimple64, blockSize));
  }

  @Benchmark
  public void encodeSimple64Bytes(Blackhole bh) throws IOException {
    int n = Simple64.encodeAll(suffixLengths, 0, blockSize, encodeOutWords, 0);
    wordOut.reset(encodeOutWordBytes);
    for (int i = 0; i < n; i++) {
      wordOut.writeLong(encodeOutWords[i]);
    }
    bh.consume(wordOut.getPosition());
    bh.consume(encodeOutWordBytes[0]);
    bh.consume(encodeOutWordBytes[wordOut.getPosition() - 1]);
  }

  @Benchmark
  public void decodeSimple64Bytes(Blackhole bh) throws IOException {
    wordIn.reset(encodedWordBytes, 0, encodedWordBytesLength);
    for (int i = 0; i < encodedWordCount; i++) {
      decodeWordBuffer[i] = wordIn.readLong();
    }
    Simple64.decodeAll(decodeWordBuffer, 0, decodeOutSimple64, 0, blockSize);
    bh.consume(checksum(decodeOutSimple64, blockSize));
  }

  // ---- VInt ----

  @Benchmark
  public void encodeVInt(Blackhole bh) throws IOException {
    vintOut.reset(encodeOutVIntBytes);
    for (int i = 0; i < blockSize; i++) {
      vintOut.writeVInt(suffixLengths[i]);
    }
    bh.consume(vintOut.getPosition());
    bh.consume(encodeOutVIntBytes[0]);
    bh.consume(encodeOutVIntBytes[vintOut.getPosition() - 1]);
  }

  @Benchmark
  public void decodeVInt(Blackhole bh) throws IOException {
    vintIn.reset(encodedVInt, 0, encodedVIntLength);
    for (int i = 0; i < blockSize; i++) {
      decodeOutVInt[i] = vintIn.readVInt();
    }
    bh.consume(checksum(decodeOutVInt, blockSize));
  }

  private int nextSuffixLength(Random rng) {
    return switch (distribution) {
      case "SMALL_1_8" -> rng.nextInt(8) + 1;
      case "MID_1_64" -> rng.nextInt(64) + 1;
      case "LARGE_1_200" -> rng.nextInt(200) + 1;
      case "MIXED" -> {
        int bucket = rng.nextInt(10);
        if (bucket < 7) {
          yield rng.nextInt(8) + 1;
        } else if (bucket < 9) {
          yield rng.nextInt(64) + 1;
        }
        yield rng.nextInt(200) + 1;
      }
      default -> throw new IllegalArgumentException("Unknown distribution: " + distribution);
    };
  }

  private int checksum(int[] values, int length) {
    int sum = 0;
    for (int i = 0; i < length; i++) {
      sum = 31 * sum + values[i];
    }
    return sum;
  }
}
