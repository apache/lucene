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
import org.apache.lucene.util.bkd.BKDWriter;
import org.apache.lucene.util.bkd.DocIdsWriter;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
public class BKDCodecBenchmark {

  private static final int SIZE = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

  @Param({"16", "24"})
  public int bpv;

  // It is important to make count variable, like what will happen in real BKD leaves. If
  // countVariable=false, {@link #current} will run as fast as {@link #currentVector}.
  @Param({"true", "false"})
  public boolean countVariable;

  private Directory dir;
  private DocIdsWriter legacy;
  private IndexInput legacyIn;
  private DocIdsWriter vector;
  private IndexInput vectorIn;
  private int[] docs;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Path path = Files.createTempDirectory("bkd");
    dir = MMapDirectory.open(path);
    docs = new int[SIZE];
    legacy = new DocIdsWriter(SIZE, BKDWriter.VERSION_META_FILE);
    legacyIn = writeDocIds("legacy", docs, legacy);
    vector = new DocIdsWriter(SIZE, BKDWriter.VERSION_VECTORIZED_BPV24);
    vectorIn = writeDocIds("current", docs, vector);
  }

  private IndexInput writeDocIds(String file, int[] docs, DocIdsWriter writer) throws IOException {
    try (IndexOutput out = dir.createOutput(file, IOContext.DEFAULT)) {
      Random r = new Random(0);
      // avoid cluster encoding
      docs[0] = 1;
      docs[1] = (1 << bpv) - 1;
      for (int i = 2; i < SIZE; ++i) {
        docs[i] = r.nextInt(1 << bpv);
      }
      writer.writeDocIds(docs, 0, SIZE, out);
    }
    return dir.openInput(file, IOContext.DEFAULT);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    legacyIn.seek(0);
    vectorIn.seek(0);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    IOUtils.close(legacyIn, vectorIn, dir);
  }

  private int count(int iter) {
    if (countVariable) {
      return iter % 20 == 0 ? SIZE - 1 : SIZE;
    } else {
      return SIZE;
    }
  }

  @Benchmark
  public void legacy(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      legacy.readInts(legacyIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  @Benchmark
  public void current(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      vector.readInts(vectorIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public void currentVector(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      vector.readInts(vectorIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  @Benchmark
  public void innerLoop(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      read(vectorIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  private final int[] scratch = new int[SIZE];

  private void read(IndexInput in, int count, int[] values) throws IOException {
    switch (bpv) {
      case 16 -> readInts16(in, count, values);
      case 24 -> readInts24(in, values, scratch, count);
      default -> throw new RuntimeException();
    }
  }

  private static void readInts16(IndexInput in, int count, int[] docIds) throws IOException {
    final int min = in.readVInt();
    int k = 0;
    for (int bound = count - 511; k < bound; k += 512) {
      in.readInts(docIds, k, 256);
      // Can be inlined to make offsets consistent so that loop get auto-vectorized.
      inner16(k, docIds, 256, min);
    }
    for (int bound = count - 127; k < bound; k += 128) {
      in.readInts(docIds, k, 64);
      inner16(k, docIds, 64, min);
    }
    for (int bound = count - 31; k < bound; k += 32) {
      in.readInts(docIds, k, 16);
      inner16(k, docIds, 16, min);
    }
    for (; k < count; k++) {
      docIds[k] = Short.toUnsignedInt(in.readShort());
    }
  }

  private static void inner16(int k, int[] docIds, int half, int min) {
    for (int i = k; i < k + half; ++i) {
      final int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[i + half] = (l & 0xFFFF) + min;
    }
  }

  private static void readInts24(IndexInput in, int[] docIds, int[] scratch, int count)
      throws IOException {
    int k = 0;
    for (int bound = count - 511; k < bound; k += 512) {
      in.readInts(scratch, k, 384);
      shift(k, docIds, scratch, 384);
      // Can be inlined to make offsets consistent so that loop get auto-vectorized.
      remainder24(k, docIds, scratch, 128, 256, 384);
    }
    for (int bound = count - 127; k < bound; k += 128) {
      in.readInts(scratch, k, 96);
      shift(k, docIds, scratch, 96);
      remainder24(k, docIds, scratch, 32, 64, 96);
    }
    for (int bound = count - 31; k < bound; k += 32) {
      in.readInts(scratch, k, 24);
      shift(k, docIds, scratch, 24);
      remainder24(k, docIds, scratch, 8, 16, 24);
    }
    for (; k < count; ++k) {
      docIds[k] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  private static void shift(int k, int[] docIds, int[] scratch, int halfAndQuarter) {
    for (int i = k; i < k + halfAndQuarter; i++) {
      docIds[i] = scratch[i] >>> 8;
    }
  }

  private static void remainder24(
      int k, int[] docIds, int[] scratch, int quarter, int half, int halfAndQuarter) {
    for (int i = k; i < k + quarter; i++) {
      docIds[i + halfAndQuarter] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarter] & 0xFF) << 8)
              | (scratch[i + half] & 0xFF);
    }
  }
}
