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
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark comparing sequential read I/O strategies: mmap (normal, MADV_RANDOM, MADV_RANDOM +
 * MADV_WILLNEED), FileChannel, FFI pread, and O_DIRECT — under varying concurrency and memory
 * pressure. Each operation picks a random starting offset, then reads 16 consecutive 16KB blocks
 * sequentially forward (256KB total per op).
 *
 * <p>Run with:
 *
 * <pre>
 *   dd if=/dev/urandom of=/path/to/bench-16G.dat bs=1M count=16384
 *   BENCH_FILE=/path/to/bench-16G.dat
 *   BENCH_FILE_SIZE_MIB=16384
 *   BENCH_DROP_CACHES=false
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh.jar SequentialReadIOBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 3,
    jvmArgsPrepend = {"--enable-native-access=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
public class SequentialReadIOBenchmark extends AbstractReadIOBenchmark {

  /** Must leave room for READS_PER_OP sequential reads from the starting offset. */
  private static final long MAX_START_OFFSET = FILE_SIZE - ((long) READ_SIZE * READS_PER_OP);

  private static final long MAX_ALIGNED_START = (MAX_START_OFFSET / ALIGNMENT) * ALIGNMENT;
  private static final long TOTAL_READ_PER_OP = (long) READ_SIZE * READS_PER_OP;

  @Override
  protected String benchmarkName() {
    return "SequentialReadIOBenchmark";
  }

  @Override
  protected void printExtraConfig() {
    System.out.println("[bench]   bytesPerOp:   " + TOTAL_READ_PER_OP + " bytes (sequential)");
  }

  // ======== mmap — no madvise ========

  @Benchmark
  @Threads(1)
  public void mmap_T01(ThreadBuffers tb, Blackhole bh) {
    doMmapNormalReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmap_T04(ThreadBuffers tb, Blackhole bh) {
    doMmapNormalReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmap_T08(ThreadBuffers tb, Blackhole bh) {
    doMmapNormalReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmap_T16(ThreadBuffers tb, Blackhole bh) {
    doMmapNormalReads(tb, bh);
  }

  // ======== mmap + MADV_RANDOM ========

  @Benchmark
  @Threads(1)
  public void mmapMadvRandom_T01(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapMadvRandom_T04(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapMadvRandom_T08(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapMadvRandom_T16(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomReads(tb, bh);
  }

  // ======== mmap + MADV_RANDOM + MADV_WILLNEED ========

  @Benchmark
  @Threads(1)
  public void mmapMadvRandomWillneed_T01(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomWillneedReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapMadvRandomWillneed_T04(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomWillneedReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapMadvRandomWillneed_T08(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomWillneedReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapMadvRandomWillneed_T16(ThreadBuffers tb, Blackhole bh) {
    doMmapMadvRandomWillneedReads(tb, bh);
  }

  // ======== FileChannel + DirectByteBuffer ========

  @Benchmark
  @Threads(1)
  public void fileChannelDirect_T01(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void fileChannelDirect_T04(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void fileChannelDirect_T08(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void fileChannelDirect_T16(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  // ======== FileChannel + HeapByteBuffer ========

  @Benchmark
  @Threads(1)
  public void fileChannelHeap_T01(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelHeapReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void fileChannelHeap_T04(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelHeapReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void fileChannelHeap_T08(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelHeapReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void fileChannelHeap_T16(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelHeapReads(tb, bh);
  }

  // ======== FFI pread ========

  @Benchmark
  @Threads(1)
  public void ffiPread_T01(ThreadBuffers tb, Blackhole bh) {
    doFfiReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void ffiPread_T04(ThreadBuffers tb, Blackhole bh) {
    doFfiReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void ffiPread_T08(ThreadBuffers tb, Blackhole bh) {
    doFfiReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void ffiPread_T16(ThreadBuffers tb, Blackhole bh) {
    doFfiReads(tb, bh);
  }

  // ======== FFI pread + O_DIRECT ========

  @Benchmark
  @Threads(1)
  public void ffiPreadDirectIO_T01(ThreadBuffers tb, Blackhole bh) {
    doFfiDirectIoReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void ffiPreadDirectIO_T04(ThreadBuffers tb, Blackhole bh) {
    doFfiDirectIoReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void ffiPreadDirectIO_T08(ThreadBuffers tb, Blackhole bh) {
    doFfiDirectIoReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void ffiPreadDirectIO_T16(ThreadBuffers tb, Blackhole bh) {
    doFfiDirectIoReads(tb, bh);
  }

  // ---- Implementation: random start, then sequential forward reads ----

  private void doMmapNormalReads(ThreadBuffers tb, Blackhole bh) {
    long startOffset = ThreadLocalRandom.current().nextLong(MAX_START_OFFSET);
    byte[] dst = tb.heapBuf.array();
    for (int i = 0; i < READS_PER_OP; i++) {
      MemorySegment.copy(
          mmapSegmentNormal,
          ValueLayout.JAVA_BYTE,
          startOffset + (long) i * READ_SIZE,
          dst,
          0,
          READ_SIZE);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomReads(ThreadBuffers tb, Blackhole bh) {
    long startOffset = ThreadLocalRandom.current().nextLong(MAX_START_OFFSET);
    byte[] dst = tb.heapBuf.array();
    for (int i = 0; i < READS_PER_OP; i++) {
      MemorySegment.copy(
          mmapSegmentMadvRandom,
          ValueLayout.JAVA_BYTE,
          startOffset + (long) i * READ_SIZE,
          dst,
          0,
          READ_SIZE);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomWillneedReads(ThreadBuffers tb, Blackhole bh) {
    long startOffset = ThreadLocalRandom.current().nextLong(MAX_START_OFFSET);
    byte[] dst = tb.heapBuf.array();
    try {
      // Prefetch the entire sequential range up front
      MemorySegment slice = mmapSegmentMadvRandom.asSlice(startOffset, TOTAL_READ_PER_OP);
      int rc = (int) POSIX_MADVISE.invokeExact(slice, TOTAL_READ_PER_OP, MADV_WILLNEED);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    for (int i = 0; i < READS_PER_OP; i++) {
      MemorySegment.copy(
          mmapSegmentMadvRandom,
          ValueLayout.JAVA_BYTE,
          startOffset + (long) i * READ_SIZE,
          dst,
          0,
          READ_SIZE);
      bh.consume(dst[0]);
    }
  }

  private void doFileChannelDirectReads(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ByteBuffer buf = tb.directBuf;
    long startOffset = rng.nextLong(MAX_START_OFFSET);
    for (int i = 0; i < READS_PER_OP; i++) {
      buf.clear();
      int n = fileChannel.read(buf, startOffset + (long) i * READ_SIZE);
      bh.consume(n);
    }
  }

  private void doFileChannelHeapReads(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ByteBuffer buf = tb.heapBuf;
    long startOffset = rng.nextLong(MAX_START_OFFSET);
    for (int i = 0; i < READS_PER_OP; i++) {
      buf.clear();
      int n = fileChannel.read(buf, startOffset + (long) i * READ_SIZE);
      bh.consume(n);
    }
  }

  private void doFfiReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = tb.ffiBuf;
    long startOffset = rng.nextLong(MAX_START_OFFSET);
    try {
      for (int i = 0; i < READS_PER_OP; i++) {
        long n =
            (long)
                PREAD.invokeExact(
                    nativeFd, buf, (long) READ_SIZE, startOffset + (long) i * READ_SIZE);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private void doFfiDirectIoReads(ThreadBuffers tb, Blackhole bh) {
    if (directIoFd < 0) {
      // O_DIRECT not available on this filesystem — skip silently
      bh.consume(0);
      return;
    }
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = tb.ffiDirectIoBuf;
    // Align start offset for O_DIRECT
    long startOffset = (rng.nextLong(MAX_ALIGNED_START / ALIGNMENT)) * ALIGNMENT;
    try {
      for (int i = 0; i < READS_PER_OP; i++) {
        long n =
            (long)
                PREAD.invokeExact(
                    directIoFd, buf, (long) READ_SIZE, startOffset + (long) i * READ_SIZE);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
