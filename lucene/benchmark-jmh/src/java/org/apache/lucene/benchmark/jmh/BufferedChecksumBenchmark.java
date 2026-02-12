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
 * <p>Tests reused instances (real-world scenario) where BufferedChecksum objects are created
 * once per IndexOutput and reused across many writes.
 *
 * <p>Key scenarios:
 * <ul>
 *   <li>Small writes: Postings with VInt encoding (1-5 bytes per write, many writes)
 *   <li>Bulk writes: Vectors, doc values, stored fields (single large write)
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 1, time = 3)
@Measurement(iterations = 1, time = 3)
@Fork(value = 1)
public class BufferedChecksumBenchmark {

  // Realistic Lucene write sizes?
  // - Postings/VInt: 1-5 bytes (many small writes)
  // - Norms: 1-8 bytes
  // - Metadata/Field infos: 20-200 bytes
  // - Doc values: 100-1000 bytes
  // - Vectors (embeddings): 384, 768, 1536 bytes (common dimensions × 4 bytes/float)
  // - Stored fields/compressed blocks: 500-4000 bytes
  @Param({"5", "64", "128", "384", "768", "1536", "4096"})
  private int dataSize;

  // Current default is 1024, test with larger sizes
  @Param({"1024", "2048"})
  private int bufferSize;

  @Param({"1", "5"})
  private int smallWriteSize;

  private byte[] data;

  // Reused instances to measure real-world performance
  private BufferedChecksum reusedBuffered;
  private CRC32C reusedDirect;

  @Setup(Level.Trial)
  public void setup() {
    data = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      data[i] = (byte) i;
    }
  }

  @Setup(Level.Iteration)
  public void setupReused() {
    reusedBuffered = new BufferedChecksum(new CRC32C(), bufferSize);
    reusedDirect = new CRC32C();
  }


  @Benchmark
  public long bufferedSmallWrites() {
    reusedBuffered.reset();
    for (int i = 0; i < data.length; i += smallWriteSize) {
      int len = Math.min(smallWriteSize, data.length - i);
      reusedBuffered.update(data, i, len);
    }
    return reusedBuffered.getValue();
  }

  @Benchmark
  public long bufferedBulkWrite() {
    reusedBuffered.reset();
    reusedBuffered.update(data, 0, data.length);
    return reusedBuffered.getValue();
  }

  @Benchmark
  public long directSmallWrites() {
    reusedDirect.reset();
    for (int i = 0; i < data.length; i += smallWriteSize) {
      int len = Math.min(smallWriteSize, data.length - i);
      reusedDirect.update(data, i, len);
    }
    return reusedDirect.getValue();
  }

  @Benchmark
  public long directBulkWrite() {
    reusedDirect.reset();
    reusedDirect.update(data, 0, data.length);
    return reusedDirect.getValue();
  }
}
