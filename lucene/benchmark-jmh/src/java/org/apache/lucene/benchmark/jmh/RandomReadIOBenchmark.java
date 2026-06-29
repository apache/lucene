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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
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

  @Param({"4096"})
  public int readSize;

  @Param({"16"})
  public int readsPerOp;

  private long maxOffset;
  private long maxAlignedOffset;

  @Setup(Level.Trial)
  public void validateParams() {
    validateReadSize(readSize);
    validateReadsPerOp(readsPerOp);
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

  // ======== mmap NORMAL + batched MADV_WILLNEED ========

  @Benchmark
  @Threads(1)
  public void mmapBatchedPrefetch_T01(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapBatchedPrefetch_T04(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapBatchedPrefetch_T08(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapBatchedPrefetch_T16(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapBatchedPrefetch(tb, bh);
  }

  // ======== mmap RANDOM + batched MADV_WILLNEED ========

  @Benchmark
  @Threads(1)
  public void mmapMadvRandomBatchedPrefetch_T01(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapMadvRandomBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void mmapMadvRandomBatchedPrefetch_T04(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapMadvRandomBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void mmapMadvRandomBatchedPrefetch_T08(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapMadvRandomBatchedPrefetch(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void mmapMadvRandomBatchedPrefetch_T16(ThreadBuffers tb, Blackhole bh) throws IOException {
    doMmapMadvRandomBatchedPrefetch(tb, bh);
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

  // ======== FileChannel + DirectByteBuffer ========

  @Benchmark
  @Threads(1)
  public void fileChannelDirectBuffer_T01(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(4)
  public void fileChannelDirectBuffer_T04(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(8)
  public void fileChannelDirectBuffer_T08(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
  }

  @Benchmark
  @Threads(16)
  public void fileChannelDirectBuffer_T16(ThreadBuffers tb, Blackhole bh) throws IOException {
    doFileChannelDirectReads(tb, bh);
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

  // ======== Implementation ========

  private void doMmapNormalReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf;
    for (int i = 0; i < readsPerOp; i++) {
      long offset = rng.nextLong(maxOffset);
      MemorySegment.copy(mmapSegmentNormal, ValueLayout.JAVA_BYTE, offset, dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf;
    for (int i = 0; i < readsPerOp; i++) {
      long offset = rng.nextLong(maxOffset);
      MemorySegment.copy(mmapSegmentRandom, ValueLayout.JAVA_BYTE, offset, dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doMmapBatchedPrefetch(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf;
    long[] offsets = tb.offsets;
    for (int i = 0; i < readsPerOp; i++) {
      offsets[i] = rng.nextLong(maxOffset);
    }
    for (int i = 0; i < readsPerOp; i++) {
      long offsetInPage = (mmapSegmentNormal.address() + offsets[i]) % PAGE_SIZE;
      long alignedOffset = offsets[i] - offsetInPage;
      long alignedLength = readSize + offsetInPage;
      MemorySegment slice = mmapSegmentNormal.asSlice(alignedOffset, alignedLength);
      madvise(slice, POSIX_MADV_WILLNEED);
    }
    for (int i = 0; i < readsPerOp; i++) {
      MemorySegment.copy(mmapSegmentNormal, ValueLayout.JAVA_BYTE, offsets[i], dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doMmapMadvRandomBatchedPrefetch(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    byte[] dst = tb.heapBuf;
    long[] offsets = tb.offsets;
    for (int i = 0; i < readsPerOp; i++) {
      offsets[i] = rng.nextLong(maxOffset);
    }
    for (int i = 0; i < readsPerOp; i++) {
      long offsetInPage = (mmapSegmentRandom.address() + offsets[i]) % PAGE_SIZE;
      long alignedOffset = offsets[i] - offsetInPage;
      long alignedLength = readSize + offsetInPage;
      MemorySegment slice = mmapSegmentRandom.asSlice(alignedOffset, alignedLength);
      madvise(slice, POSIX_MADV_WILLNEED);
    }
    for (int i = 0; i < readsPerOp; i++) {
      MemorySegment.copy(mmapSegmentRandom, ValueLayout.JAVA_BYTE, offsets[i], dst, 0, readSize);
      bh.consume(dst[0]);
    }
  }

  private void doFileChannelDirectReads(ThreadBuffers tb, Blackhole bh) throws IOException {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    ByteBuffer buf = tb.directBuf;
    for (int i = 0; i < readsPerOp; i++) {
      long offset = rng.nextLong(maxOffset);
      buf.clear().limit(readSize);
      int n = fileChannel.read(buf, offset);
      bh.consume(n);
    }
  }

  private void doFfiReads(ThreadBuffers tb, Blackhole bh) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = tb.ffiBuf;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long offset = rng.nextLong(maxOffset);
        long n = (long) PREAD.invokeExact(preadFd, buf, (long) readSize, offset);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private void doFfiDirectIoReads(ThreadBuffers tb, Blackhole bh) {
    if (directIOFd < 0) {
      bh.consume(0);
      return;
    }
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    MemorySegment buf = tb.ffiDirectIOBuf;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long offset = (rng.nextLong(maxAlignedOffset / PAGE_SIZE)) * PAGE_SIZE;
        long n = (long) PREAD.invokeExact(directIOFd, buf, (long) readSize, offset);
        bh.consume(n);
      }
    } catch (Throwable t) {
      throw new RuntimeException("pread failed", t);
    }
  }
}
