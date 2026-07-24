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

import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import org.apache.lucene.store.BufferedChecksum;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark to measure BufferedChecksum performance with realistic Lucene usage patterns.
 *
 * <p>Tests two real Lucene code paths:
 *
 * <ul>
 *   <li>SmallWrites (byte-at-a-time): Models {@code BufferedChecksumIndexInput.readByte()} which
 *       calls {@code digest.update(b)} for each byte. This is the hot path for reading postings
 *       (VInt/VLong encoding), field infos, segment metadata, and any DataInput that reads one byte
 *       at a time.
 *   <li>BulkWrite (single array): Models {@code BufferedChecksumIndexInput.readBytes()} which calls
 *       {@code digest.update(b, off, len)} for a chunk. Also models the write side where
 *       OutputStreamIndexOutput flushes its buffer through CheckedOutputStream. This covers vectors,
 *       doc values, stored fields, and any bulk read/write path.
 * </ul>
 *
 * <p>dataSize represents the total bytes checksummed per operation. Fine-grained values around
 * typical Lucene data sizes and the buffer boundary (1024) give the most useful signal.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 1, time = 3)
@Measurement(iterations = 1, time = 3)
@Fork(value = 1)
public class BufferedChecksumBenchmark {

  // Fine-grained sizes around the DIRECT_THRESHOLD boundary (64-384) to find the
  // exact crossover where direct native CRC32 beats arraycopy + deferred flush.
  // Also includes typical Lucene data sizes and buffer boundaries.
  @Param({"1", "4", "8", "32", "64", "96", "128", "160", "192", "224", "256", "320", "384", "512", "768", "1024", "1536", "2048", "4096"})
  private int dataSize;

  // Current default is 1024, test with 2048 to evaluate buffer size increase
  @Param({"1024", "2048"})
  private int bufferSize;

  /** Total bytes to accumulate in realistic benchmarks before calling getValue(). */
  private static final int TOTAL_BYTES = 1024 * 1024; // 1MB

  private byte[] data;

  /** Number of update() calls to reach ~1MB in realistic benchmarks. */
  private int iterations;

  private BufferedChecksum reusedBufferedCRC32C;
  private BufferedChecksum reusedBufferedCRC32;
  private CRC32C reusedDirectCRC32C;
  private CRC32 reusedDirectCRC32;

  @Setup(Level.Trial)
  public void setup() {
    data = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      data[i] = (byte) i;
    }
    iterations = Math.max(1, TOTAL_BYTES / dataSize);
  }

  @Setup(Level.Iteration)
  public void setupReused() {
    reusedBufferedCRC32C = new BufferedChecksum(new CRC32C(), bufferSize);
    reusedBufferedCRC32 = new BufferedChecksum(new CRC32(), bufferSize);
    reusedDirectCRC32C = new CRC32C();
    reusedDirectCRC32 = new CRC32();
  }

  // ---- Byte-at-a-time: models BufferedChecksumIndexInput.readByte() path ----

  @Benchmark
  public long bufferedCRC32CSmallWrites() {
    reusedBufferedCRC32C.reset();
    for (int i = 0; i < data.length; i++) {
      reusedBufferedCRC32C.update(data[i]);
    }
    return reusedBufferedCRC32C.getValue();
  }

  @Benchmark
  public long bufferedCRC32SmallWrites() {
    reusedBufferedCRC32.reset();
    for (int i = 0; i < data.length; i++) {
      reusedBufferedCRC32.update(data[i]);
    }
    return reusedBufferedCRC32.getValue();
  }

  @Benchmark
  public long directCRC32CSmallWrites() {
    reusedDirectCRC32C.reset();
    for (int i = 0; i < data.length; i++) {
      reusedDirectCRC32C.update(data[i]);
    }
    return reusedDirectCRC32C.getValue();
  }

  @Benchmark
  public long directCRC32SmallWrites() {
    reusedDirectCRC32.reset();
    for (int i = 0; i < data.length; i++) {
      reusedDirectCRC32.update(data[i]);
    }
    return reusedDirectCRC32.getValue();
  }

  // ---- Bulk write: models BufferedChecksumIndexInput.readBytes() / OutputStreamIndexOutput flush ----

  @Benchmark
  public long bufferedCRC32CBulkWrite() {
    reusedBufferedCRC32C.reset();
    reusedBufferedCRC32C.update(data, 0, data.length);
    return reusedBufferedCRC32C.getValue();
  }

  @Benchmark
  public long bufferedCRC32BulkWrite() {
    reusedBufferedCRC32.reset();
    reusedBufferedCRC32.update(data, 0, data.length);
    return reusedBufferedCRC32.getValue();
  }

  @Benchmark
  public long directCRC32CBulkWrite() {
    reusedDirectCRC32C.reset();
    reusedDirectCRC32C.update(data, 0, data.length);
    return reusedDirectCRC32C.getValue();
  }

  @Benchmark
  public long directCRC32BulkWrite() {
    reusedDirectCRC32.reset();
    reusedDirectCRC32.update(data, 0, data.length);
    return reusedDirectCRC32.getValue();
  }

  // ---- Realistic: N updates then one getValue() — models real Lucene checksum pattern ----

  @Benchmark
  public long bufferedCRC32RealisticBulkWrite() {
    reusedBufferedCRC32.reset();
    for (int i = 0; i < iterations; i++) {
      reusedBufferedCRC32.update(data, 0, data.length);
    }
    return reusedBufferedCRC32.getValue();
  }

  @Benchmark
  public long bufferedCRC32CRealisticBulkWrite() {
    reusedBufferedCRC32C.reset();
    for (int i = 0; i < iterations; i++) {
      reusedBufferedCRC32C.update(data, 0, data.length);
    }
    return reusedBufferedCRC32C.getValue();
  }

  @Benchmark
  public long directCRC32RealisticBulkWrite() {
    reusedDirectCRC32.reset();
    for (int i = 0; i < iterations; i++) {
      reusedDirectCRC32.update(data, 0, data.length);
    }
    return reusedDirectCRC32.getValue();
  }

  @Benchmark
  public long directCRC32CRealisticBulkWrite() {
    reusedDirectCRC32C.reset();
    for (int i = 0; i < iterations; i++) {
      reusedDirectCRC32C.update(data, 0, data.length);
    }
    return reusedDirectCRC32C.getValue();
  }
}
