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
 * Benchmark comparing sequential (whole-file scan) I/O strategies. Single-threaded. Each op reads
 * {@code readsPerOp} pages sequentially forward from the current file position, wrapping at EOF.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 1, time = 3)
@Fork(
    value = 1,
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
    public void tearDown() throws IOException {
      cleanup();
    }
  }

  @Setup(Level.Trial)
  public void checkAndInit() {
    if (directIOFd >= 0 && readSize % PAGE_SIZE != 0) {
      throw new IllegalArgumentException(
          "readSize ("
              + readSize
              + ") must be a multiple of PAGE_SIZE ("
              + PAGE_SIZE
              + ") for O_DIRECT.");
    }
    maxOffset = FILE_SIZE - readSize;
  }

  // ======== mmap NORMAL ========

  @Benchmark
  public void mmap(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapInput, ts.heapBuf, bh);
  }

  // ======== mmap SEQUENTIAL ========

  @Benchmark
  public void mmapMadvSequential(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapSequentialInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM ========

  @Benchmark
  public void mmapMadvRandom(ThreadState ts, Blackhole bh) throws IOException {
    doSequentialReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM + batched prefetch ========

  @Benchmark
  public void mmapMadvRandomBatchedPrefetch(ThreadState ts, Blackhole bh) throws IOException {
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
  public void ffiPread(ThreadState ts, Blackhole bh) {
    MemorySegment buf = ts.ffiBuf;
    long offset = seqPosition;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long n = (long) PREAD.invokeExact(preadFd, buf, (long) readSize, offset);
        bh.consume(n);
        offset += readSize;
        if (offset > maxOffset) {
          offset = 0;
        }
      }
    } catch (Throwable t) {
      throw new RuntimeException("FFI Pread failed", t);
    }
    seqPosition = offset;
  }

  // ======== FFI pread + O_DIRECT ========

  @Benchmark
  public void ffiPreadDirectIO(ThreadState ts, Blackhole bh) {
    if (directIOFd < 0) {
      bh.consume(0);
      return;
    }
    MemorySegment buf = ts.ffiDirectIOBuf;
    long offset = (seqPosition / PAGE_SIZE) * PAGE_SIZE;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long n = (long) PREAD.invokeExact(directIOFd, buf, (long) readSize, offset);
        bh.consume(n);
        offset += readSize;
        if (offset > maxOffset) {
          offset = 0;
        }
      }
    } catch (Throwable t) {
      throw new RuntimeException("FFI Direct IO read failed", t);
    }
    seqPosition = offset;
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
