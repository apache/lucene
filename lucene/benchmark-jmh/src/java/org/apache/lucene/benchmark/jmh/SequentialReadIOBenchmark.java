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
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.IndexInput;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Sequential read benchmark, single-threaded, like a whole-file scan. Each benchmark invocation
 * issues {@code readsPerOp} reads of {@code readSize} bytes, going forward from where the previous
 * invocation stopped, and wraps around at the end of the file. See {@link AbstractReadIOBenchmark}
 * for the strategies, how to run, and what to watch out for.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(
    value = 2,
    jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
@Threads(1)
public class SequentialReadIOBenchmark extends AbstractReadIOBenchmark {

  @Param({"16384"})
  public int readSize;

  @Param({"128"})
  public int readsPerOp;

  private long seqPosition = 0;

  private long maxOffset;

  @State(Scope.Thread)
  public static class ThreadState extends BaseThreadState {
    @Setup(Level.Trial)
    public void setup(SequentialReadIOBenchmark bench) throws IOException {
      init(bench, bench.readSize);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      cleanup();
    }
  }

  @Setup(Level.Trial)
  public void checkAndInit() {
    maxOffset = FILE_SIZE - readSize;
  }

  @Override
  protected int readSize() {
    return readSize;
  }

  // ======== mmap NORMAL ========

  @Benchmark
  public void mmap(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapInput, ts.heapBuf, bh);
  }

  // ======== mmap SEQUENTIAL ========

  @Benchmark
  public void mmapSequential(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapSequentialInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM ========

  @Benchmark
  public void mmapRandom(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM + batched prefetch ========

  @Benchmark
  public void mmapRandomBatchedPrefetch(ThreadState ts, Blackhole bh) throws IOException {
    batchedPrefetch(ts.mmapRandomInput);
    doSequentialReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  // ======== NIOFSDirectory (FileChannel positioned reads) ========

  @Benchmark
  public void fileChannelNIOFS(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.niofsInput, ts.heapBuf, bh);
  }

  // ======== FFI pread ========

  @Benchmark
  public void ffiPread(ThreadState ts, Blackhole bh) throws IOException {
    MemorySegment buf = ts.ffiBuf;
    long offset = seqPosition;
    for (int i = 0; i < readsPerOp; i++) {
      bh.consume(pread(buf, readSize, offset));
      offset += readSize;
      if (offset > maxOffset) {
        offset = 0;
      }
    }
    seqPosition = offset;
  }

  // ======== DirectIODirectory (O_DIRECT) ========

  @Benchmark
  public void directIO(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.directIOInput, ts.heapBuf, bh);
  }

  private void doSequentialReads(IndexInput input, byte[] dst, Blackhole bh) throws IOException {
    long offset = seqPosition;
    for (int i = 0; i < readsPerOp; i++) {
      input.seek(offset);
      input.readBytes(dst, 0, readSize);
      bh.consume(dst[0]);
      offset += readSize;
      if (offset > maxOffset) {
        offset = 0;
      }
    }
    seqPosition = offset;
  }

  private void batchedPrefetch(IndexInput input) throws IOException {
    long prefetchOffset = seqPosition;
    for (int i = 0; i < readsPerOp; i++) {
      input.prefetch(prefetchOffset, readSize);
      prefetchOffset += readSize;
      if (prefetchOffset > maxOffset) {
        prefetchOffset = 0;
      }
    }
  }
}
