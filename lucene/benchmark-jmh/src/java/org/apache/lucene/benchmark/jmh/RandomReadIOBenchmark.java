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
import java.util.concurrent.ThreadLocalRandom;
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

/** Benchmark comparing random read I/O strategies under varying concurrency and memory pressure. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@Fork(
    value = 2,
    jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
public class RandomReadIOBenchmark extends AbstractReadIOBenchmark {

  @Param({"16384"})
  public int readSize;

  @Param({"16"})
  public int readsPerOp;

  private long maxOffset;
  private long maxAlignedOffset;

  @State(Scope.Thread)
  public static class ThreadState extends BaseThreadState {
    @Setup(Level.Trial)
    public void setup(RandomReadIOBenchmark bench) throws IOException {
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
    maxAlignedOffset = (maxOffset / PAGE_SIZE) * PAGE_SIZE;
  }

  // ======== mmap NORMAL ========

  @Benchmark
  @Threads(1)
  public void mmap_T01(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmap_T04(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmap_T08(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmap_T16(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM ========

  @Benchmark
  @Threads(1)
  public void mmapRandom_T01(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapRandom_T04(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapRandom_T08(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapRandom_T16(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  // ======== mmap NORMAL + batched MADV_WILLNEED ========

  @Benchmark
  @Threads(1)
  public void mmapBatchedPrefetch_T01(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapBatchedPrefetch_T04(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapBatchedPrefetch_T08(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapBatchedPrefetch_T16(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts.heapBuf, bh);
  }

  // ======== mmap RANDOM + batched MADV_WILLNEED ========

  @Benchmark
  @Threads(1)
  public void mmapRandomBatchedPrefetch_T01(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapRandomBatchedPrefetch_T04(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapRandomBatchedPrefetch_T08(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapRandomBatchedPrefetch_T16(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts.heapBuf, bh);
  }

  // ======== FFI pread ========

  @Benchmark
  @Threads(1)
  public void ffiPread_T01(ThreadState ts, Blackhole bh) {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void ffiPread_T04(ThreadState ts, Blackhole bh) {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void ffiPread_T08(ThreadState ts, Blackhole bh) {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void ffiPread_T16(ThreadState ts, Blackhole bh) {
    doFfiReads(ts, bh);
  }

  // ======== NIOFSDirectory (FileChannel positioned reads) ========

  @Benchmark
  @Threads(1)
  public void fileChannelNIOFS_T01(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.niofsInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(4)
  public void fileChannelNIOFS_T04(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.niofsInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(8)
  public void fileChannelNIOFS_T08(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.niofsInput, ts.heapBuf, bh);
  }

  @Benchmark
  @Threads(16)
  public void fileChannelNIOFS_T16(ThreadState ts, Blackhole bh) throws IOException {
    doIndexInputReads(ts.niofsInput, ts.heapBuf, bh);
  }

  // ======== FFI pread + O_DIRECT ========

  @Benchmark
  @Threads(1)
  public void ffiPreadDirectIO_T01(ThreadState ts, Blackhole bh) {
    doFfiDirectIoReads(ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void ffiPreadDirectIO_T04(ThreadState ts, Blackhole bh) {
    doFfiDirectIoReads(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void ffiPreadDirectIO_T08(ThreadState ts, Blackhole bh) {
    doFfiDirectIoReads(ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void ffiPreadDirectIO_T16(ThreadState ts, Blackhole bh) {
    doFfiDirectIoReads(ts, bh);
  }

  private void doIndexInputReads(IndexInput input, byte[] dst, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    for (int i = 0; i < readsPerOp; i++) {
      long offset = rng.nextLong(maxOffset);
      input.seek(offset);
      input.readBytes(dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doBatchedPrefetchReads(IndexInput input, byte[] dst, Blackhole bh)
      throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    long[] offsets = new long[readsPerOp];
    for (int i = 0; i < readsPerOp; i++) {
      offsets[i] = rng.nextLong(maxOffset);
    }
    for (int i = 0; i < readsPerOp; i++) {
      input.prefetch(offsets[i], readSize);
    }
    for (int i = 0; i < readsPerOp; i++) {
      input.seek(offsets[i]);
      input.readBytes(dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doFfiReads(ThreadState ts, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = ts.ffiBuf;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long offset = rng.nextLong(maxOffset);
        long n = (long) PREAD.invokeExact(preadFd, buf, (long) readSize, offset);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException("FFI Pread failed", t);
    }
  }

  private void doFfiDirectIoReads(ThreadState ts, Blackhole bh) {
    if (directIOFd < 0) {
      bh.consume(0);
      return;
    }
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = ts.ffiDirectIOBuf;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long offset = (rng.nextLong(maxAlignedOffset / PAGE_SIZE)) * PAGE_SIZE;
        long n = (long) PREAD.invokeExact(directIOFd, buf, (long) readSize, offset);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException("FFI Direct IO read failed", t);
    }
  }
}
