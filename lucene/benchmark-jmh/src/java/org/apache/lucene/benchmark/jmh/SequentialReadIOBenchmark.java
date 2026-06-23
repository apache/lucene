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
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
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

  @Param({"4096"})
  public int readSize;

  @Param({"64"})
  public int readsPerOp;

  /** Sliding prefetch window size. */
  private static final long PREFETCH_WINDOW = 2 * 1024 * 1024;

  /** Current sequential scan position — advances across JMH invocations, wraps at EOF. */
  private long seqPosition = 0;

  // ======== mmap NORMAL ========

  @Benchmark
  public void mmap(ThreadBuffers tb, Blackhole bh) {
    byte[] dst = tb.heapBuf.array();
    long offset = seqPosition;
    for (int i = 0; i < readsPerOp; i++) {
      MemorySegment.copy(mmapSegmentNormal, ValueLayout.JAVA_BYTE, offset, dst, 0, readSize);
      bh.consume(dst[0]);
      offset += readSize;
      if (offset >= FILE_SIZE) {
        offset = 0;
      }
    }
    seqPosition = offset;
  }

  // ======== mmap NORMAL + sliding 2MB WILLNEED prefetch window ========

  @Benchmark
  public void mmapBatchedPrefetch(ThreadBuffers tb, Blackhole bh) {
    byte[] dst = tb.heapBuf.array();
    long offset = seqPosition;
    try {
      long prefetchOff = offset + (long) readsPerOp * readSize;
      if (prefetchOff + PREFETCH_WINDOW <= FILE_SIZE) {
        MemorySegment slice = mmapSegmentNormal.asSlice(prefetchOff, PREFETCH_WINDOW);
        int rc = (int) POSIX_MADVISE.invokeExact(slice, PREFETCH_WINDOW, MADV_WILLNEED);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    for (int i = 0; i < readsPerOp; i++) {
      MemorySegment.copy(mmapSegmentNormal, ValueLayout.JAVA_BYTE, offset, dst, 0, readSize);
      bh.consume(dst[0]);
      offset += readSize;
      if (offset >= FILE_SIZE) {
        offset = 0;
      }
    }
    seqPosition = offset;
  }

  // ======== FFI pread ========

  @Benchmark
  public void ffiPread(ThreadBuffers tb, Blackhole bh) {
    MemorySegment buf = tb.ffiBuf;
    long offset = seqPosition;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long n = (long) PREAD.invokeExact(nativeFd, buf, (long) readSize, offset);
        bh.consume(n);
        offset += readSize;
        if (offset >= FILE_SIZE) {
          offset = 0;
        }
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    seqPosition = offset;
  }

  // ======== FileChannel + DirectByteBuffer ========

  @Benchmark
  public void fileChannelDirectBuffer(ThreadBuffers tb, Blackhole bh) throws IOException {
    ByteBuffer buf = tb.directBuf;
    long offset = seqPosition;
    for (int i = 0; i < readsPerOp; i++) {
      buf.clear().limit(readSize);
      int n = fileChannel.read(buf, offset);
      bh.consume(n);
      offset += readSize;
      if (offset >= FILE_SIZE) offset = 0;
    }
    seqPosition = offset;
  }

  // ======== FFI pread + O_DIRECT ========

  @Benchmark
  public void ffiPreadDirectIO(ThreadBuffers tb, Blackhole bh) {
    if (directIoFd < 0) {
      bh.consume(0);
      return;
    }
    MemorySegment buf = tb.ffiDirectIoBuf;
    long offset = (seqPosition / ALIGNMENT) * ALIGNMENT;
    try {
      for (int i = 0; i < readsPerOp; i++) {
        long n = (long) PREAD.invokeExact(directIoFd, buf, (long) readSize, offset);
        bh.consume(n);
        offset += readSize;
        if (offset >= FILE_SIZE) offset = 0;
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    seqPosition = offset;
  }
}
