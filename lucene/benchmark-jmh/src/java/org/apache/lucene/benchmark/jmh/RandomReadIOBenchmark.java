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
 * Benchmark comparing random read I/O strategies: mmap (normal, MADV_RANDOM, MADV_RANDOM +
 * MADV_WILLNEED), FileChannel, FFI pread, and O_DIRECT — under varying concurrency and memory
 * pressure. Each operation picks a random offset reading 4KB data for 16 times (64KB total per op).
 *
 * <p>Run with:
 *
 * <pre>
 *   dd if=/dev/urandom of=/path/to/bench-16G.dat bs=1M count=16384
 *   BENCH_FILE=/path/to/bench-16G.dat
 *   BENCH_FILE_SIZE_MIB=16384
 *   BENCH_DROP_CACHES=false
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh.jar RandomReadIOBenchmark
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
public class RandomReadIOBenchmark extends AbstractReadIOBenchmark {

  private static final long MAX_OFFSET = FILE_SIZE - READ_SIZE;
  private static final long MAX_ALIGNED_OFFSET = (MAX_OFFSET / ALIGNMENT) * ALIGNMENT;

  @Override
  protected String benchmarkName() {
    return "RandomReadIOBenchmark";
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

  // ---- Implementation: each read picks an independent random offset ----

  private void doMmapNormalReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf.array();
    for (int i = 0; i < READS_PER_OP; i++) {
      long offset = rng.nextLong(MAX_OFFSET);
      MemorySegment.copy(mmapSegmentNormal, ValueLayout.JAVA_BYTE, offset, dst, 0, READ_SIZE);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf.array();
    for (int i = 0; i < READS_PER_OP; i++) {
      long offset = rng.nextLong(MAX_OFFSET);
      MemorySegment.copy(mmapSegmentMadvRandom, ValueLayout.JAVA_BYTE, offset, dst, 0, READ_SIZE);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomWillneedReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf.array();
    try {
      for (int i = 0; i < READS_PER_OP; i++) {
        long offset = rng.nextLong(MAX_OFFSET);
        MemorySegment slice = mmapSegmentMadvRandom.asSlice(offset, READ_SIZE);
        int rc = (int) POSIX_MADVISE.invokeExact(slice, (long) READ_SIZE, MADV_WILLNEED);
        MemorySegment.copy(mmapSegmentMadvRandom, ValueLayout.JAVA_BYTE, offset, dst, 0, READ_SIZE);
        bh.consume(dst[0]);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private void doFileChannelDirectReads(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ByteBuffer buf = tb.directBuf;
    for (int i = 0; i < READS_PER_OP; i++) {
      long offset = rng.nextLong(MAX_OFFSET);
      buf.clear();
      int n = fileChannel.read(buf, offset);
      bh.consume(n);
    }
  }

  private void doFileChannelHeapReads(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ByteBuffer buf = tb.heapBuf;
    for (int i = 0; i < READS_PER_OP; i++) {
      long offset = rng.nextLong(MAX_OFFSET);
      buf.clear();
      int n = fileChannel.read(buf, offset);
      bh.consume(n);
    }
  }

  private void doFfiReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = tb.ffiBuf;
    try {
      for (int i = 0; i < READS_PER_OP; i++) {
        long offset = rng.nextLong(MAX_OFFSET);
        long n = (long) PREAD.invokeExact(nativeFd, buf, (long) READ_SIZE, offset);
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
    try {
      for (int i = 0; i < READS_PER_OP; i++) {
        // O_DIRECT requires aligned offset; generate random aligned position
        long offset = (rng.nextLong(MAX_ALIGNED_OFFSET / ALIGNMENT)) * ALIGNMENT;
        long n = (long) PREAD.invokeExact(directIoFd, buf, (long) READ_SIZE, offset);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
