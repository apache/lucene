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

/**
 * Random read benchmark. Each benchmark invocation issues {@code readsPerOp} reads of {@code
 * readSize} bytes at uniformly random offsets, and every strategy runs with 1, 4, 8 and 16 threads
 * to see how it scales with concurrency. See {@link AbstractReadIOBenchmark} for the strategies,
 * how to run, and what to watch out for.
 */
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

    long[] prefetchOffsets;

    @Setup(Level.Trial)
    public void setup(RandomReadIOBenchmark bench) throws IOException {
      init(bench, bench.readSize);
      prefetchOffsets = new long[bench.readsPerOp];
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      cleanup();
    }
  }

  @Setup(Level.Trial)
  public void checkAndInit() {
    maxOffset = FILE_SIZE - readSize;
    maxAlignedOffset = (maxOffset / PAGE_SIZE) * PAGE_SIZE;
  }

  @Override
  protected int readSize() {
    return readSize;
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

  // ======== mmap NORMAL + batched prefetch ========

  @Benchmark
  @Threads(1)
  public void mmapBatchedPrefetch_T01(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapBatchedPrefetch_T04(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapBatchedPrefetch_T08(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapBatchedPrefetch_T16(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapInput, ts, bh);
  }

  // ======== mmap RANDOM + batched prefetch ========

  @Benchmark
  @Threads(1)
  public void mmapRandomBatchedPrefetch_T01(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapRandomBatchedPrefetch_T04(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapRandomBatchedPrefetch_T08(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapRandomBatchedPrefetch_T16(ThreadState ts, Blackhole bh) throws IOException {
    doBatchedPrefetchReads(ts.mmapRandomInput, ts, bh);
  }

  // ======== FFI pread ========

  @Benchmark
  @Threads(1)
  public void ffiPread_T01(ThreadState ts, Blackhole bh) throws IOException {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void ffiPread_T04(ThreadState ts, Blackhole bh) throws IOException {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void ffiPread_T08(ThreadState ts, Blackhole bh) throws IOException {
    doFfiReads(ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void ffiPread_T16(ThreadState ts, Blackhole bh) throws IOException {
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

  // ======== DirectIODirectory (O_DIRECT, page-aligned offsets) ========

  @Benchmark
  @Threads(1)
  public void directIO_T01(ThreadState ts, Blackhole bh) throws IOException {
    doDirectIOReads(ts, bh);
  }

  @Benchmark
  @Threads(4)
  public void directIO_T04(ThreadState ts, Blackhole bh) throws IOException {
    doDirectIOReads(ts, bh);
  }

  @Benchmark
  @Threads(8)
  public void directIO_T08(ThreadState ts, Blackhole bh) throws IOException {
    doDirectIOReads(ts, bh);
  }

  @Benchmark
  @Threads(16)
  public void directIO_T16(ThreadState ts, Blackhole bh) throws IOException {
    doDirectIOReads(ts, bh);
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

  private void doBatchedPrefetchReads(IndexInput input, ThreadState ts, Blackhole bh)
      throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    long[] offsets = ts.prefetchOffsets;
    byte[] dst = ts.heapBuf;
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

  private void doFfiReads(ThreadState ts, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = ts.ffiBuf;
    for (int i = 0; i < readsPerOp; i++) {
      bh.consume(pread(buf, readSize, rng.nextLong(maxOffset)));
    }
  }

  private void doDirectIOReads(ThreadState ts, Blackhole bh) throws IOException {
    IndexInput input = ts.directIOInput;
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = ts.heapBuf;
    for (int i = 0; i < readsPerOp; i++) {
      long offset = rng.nextLong(maxAlignedOffset / PAGE_SIZE) * PAGE_SIZE;
      input.seek(offset);
      input.readBytes(dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }
}
