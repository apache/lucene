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

  @Param({"200"})
  int maxSuffix;

  // input
  int[] suffixLengths;

  // Simple64
  long[] encodedLongs;
  int encodedLongCount;
  int[] decodeOutSimple64;

  // VInt
  byte[] encodedVInt;
  int encodedVIntLength;
  int[] decodeOutVInt;

  // encode output buffers
  long[] encodeOutLongs;
  byte[] encodeOutBytes;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random rng = new Random(42);

    suffixLengths = new int[blockSize];
    for (int i = 0; i < blockSize; i++) {
      suffixLengths[i] = rng.nextInt(maxSuffix) + 1;
    }

    // pre-encode Simple64
    encodedLongs = new long[blockSize + 1];
    encodedLongCount = Simple64.encodeAll(suffixLengths, 0, blockSize, encodedLongs, 0);
    decodeOutSimple64 = new int[blockSize + Simple64.COUNTS[0]];
    encodeOutLongs = new long[blockSize + 1];

    // pre-encode VInt
    encodedVInt = new byte[blockSize * 5];
    ByteArrayDataOutput out = new ByteArrayDataOutput(encodedVInt);
    for (int i = 0; i < blockSize; i++) {
      out.writeVInt(suffixLengths[i]);
    }
    encodedVIntLength = out.getPosition();
    decodeOutVInt = new int[blockSize];
    encodeOutBytes = new byte[blockSize * 5];
  }

  // ---- Simple64 ----

  @Benchmark
  public void encodeSimple64(Blackhole bh) {
    int n = Simple64.encodeAll(suffixLengths, 0, blockSize, encodeOutLongs, 0);
    bh.consume(n);
  }

  @Benchmark
  public void decodeSimple64(Blackhole bh) {
    Simple64.decodeAll(encodedLongs, 0, decodeOutSimple64, 0, blockSize);
    bh.consume(decodeOutSimple64[0]);
  }

  // ---- VInt ----

  @Benchmark
  public void encodeVInt(Blackhole bh) throws IOException {
    ByteArrayDataOutput out = new ByteArrayDataOutput(encodeOutBytes);
    for (int i = 0; i < blockSize; i++) {
      out.writeVInt(suffixLengths[i]);
    }
    bh.consume(out.getPosition());
  }

  @Benchmark
  public void decodeVInt(Blackhole bh) throws IOException {
    ByteArrayDataInput in = new ByteArrayDataInput(encodedVInt, 0, encodedVIntLength);
    for (int i = 0; i < blockSize; i++) {
      decodeOutVInt[i] = in.readVInt();
    }
    bh.consume(decodeOutVInt[0]);
  }
}
