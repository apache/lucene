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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** NO COMMIT: remove before merge */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
public class Decode21Benchmark {

  private static final int SIZE = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

  private Directory dir;
  private IndexInput in;
  private int[] docs;
  private int[] scratch;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Path path = Files.createTempDirectory("innerloop");
    dir = MMapDirectory.open(path);
    try (IndexOutput out = dir.createOutput("docs", IOContext.DEFAULT)) {
      Random random = new Random(0L);
      for (int i = 0; i < SIZE; i++) {
        out.writeInt(random.nextInt());
      }
    }
    docs = new int[SIZE];
    scratch = new int[SIZE];
    in = dir.openInput("docs", IOContext.DEFAULT);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    in.seek(0);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    IOUtils.close(in, dir);
  }

  private int count(int iter) {
    return iter % 20 == 0 ? SIZE - 1 : SIZE;
  }

  @Benchmark
  public void decode21Scalar(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decode21Scalar(in, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private void decode21Scalar(IndexInput in, int count, int[] docIDs) throws IOException {
    int i = 0;
    for (; i < count - 8; i += 9) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i] = (int) (l1 >>> 42);
      docIDs[i + 1] = (int) ((l1 >>> 21) & 0x1FFFFFL);
      docIDs[i + 2] = (int) (l1 & 0x1FFFFFL);
      docIDs[i + 3] = (int) (l2 >>> 42);
      docIDs[i + 4] = (int) ((l2 >>> 21) & 0x1FFFFFL);
      docIDs[i + 5] = (int) (l2 & 0x1FFFFFL);
      docIDs[i + 6] = (int) (l3 >>> 42);
      docIDs[i + 7] = (int) ((l3 >>> 21) & 0x1FFFFFL);
      docIDs[i + 8] = (int) (l3 & 0x1FFFFFL);
    }
    for (; i < count - 2; i += 3) {
      long packedLong = in.readLong();
      docIDs[i] = (int) (packedLong >>> 42);
      docIDs[i + 1] = (int) ((packedLong >>> 21) & 0x1FFFFFL);
      docIDs[i + 2] = (int) (packedLong & 0x1FFFFFL);
    }
    for (; i < count; i++) {
      docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
    }
  }

  @Benchmark
  public void decode21Vector(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decode21Vector(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static void decode21Vector(IndexInput in, int count, int[] docIDs, int[] scratch)
      throws IOException {
    int oneThird = count / 3;
    int numBytes = oneThird << 1;
    in.readInts(scratch, 0, numBytes);
    if (count == BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
      // Same format, but enabling the JVM to specialize the decoding logic for the default number
      // of points per node proved to help on benchmarks
      decode21(
          docIDs,
          scratch,
          BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3,
          (BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3) * 2);
    } else {
      decode21(docIDs, scratch, oneThird, numBytes);
    }
    // Now read the remaining values
    for (int i = oneThird * 3; i < count; i++) {
      docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
    }
  }

  @Benchmark
  public void decode21VectorFloorToMultipleOf8(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      decode21VectorFloorToMultipleOf8(in, count, docs, scratch);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private static int floorToMultipleOf8(int n) {
    return n & 0xFFFFFFF8;
  }

  private static void decode21VectorFloorToMultipleOf8(
      IndexInput in, int count, int[] docIDs, int[] scratch) throws IOException {
    // Compiler can only vectorize the remainder loop when the loop bound is a multiple of 8.
    int oneThird = floorToMultipleOf8(count / 3);
    int numBytes = oneThird << 1;
    in.readInts(scratch, 0, numBytes);
    if (count == BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE) {
      // Same format, but enabling the JVM to specialize the decoding logic for the default number
      // of points per node proved to help on benchmarks
      decode21(
          docIDs,
          scratch,
          floorToMultipleOf8(BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3),
          floorToMultipleOf8(BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE / 3) * 2);
    } else {
      decode21(docIDs, scratch, oneThird, numBytes);
    }
    // Now read the remaining values
    int i = oneThird * 3;
    for (; i < count - 2; i += 3) {
      long l = in.readLong();
      docIDs[i] = (int) (l >>> 42);
      docIDs[i + 1] = (int) ((l >>> 21) & 0x1FFFFFL);
      docIDs[i + 2] = (int) (l & 0x1FFFFFL);
    }
    for (; i < count; i++) {
      docIDs[i] = (in.readShort() & 0xFFFF) | (in.readByte() & 0xFF) << 16;
    }
  }

  private static void decode21(int[] docIds, int[] scratch, int oneThird, int numInts) {
    for (int i = 0; i < numInts; ++i) {
      docIds[i] = scratch[i] >>> 11;
    }
    for (int i = 0; i < oneThird; i++) {
      docIds[i + numInts] = (scratch[i] & 0x7FF) | ((scratch[i + oneThird] & 0x7FF) << 11);
    }
  }
}
