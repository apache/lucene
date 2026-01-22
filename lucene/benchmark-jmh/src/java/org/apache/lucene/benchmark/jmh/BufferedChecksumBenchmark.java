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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1)
public class BufferedChecksumBenchmark {

  @Param({"1024", "4096", "8192", "65536"})
  private int dataSize;

  @Param({"1024", "2048", "4096"})
  private int bufferSize;

  private byte[] data;

  @Setup
  public void setup() {
    data = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      data[i] = (byte) i;
    }
  }

  @Benchmark
  public long bufferedCRC32SmallUpdates() {
    BufferedChecksum checksum = new BufferedChecksum(new CRC32(), bufferSize);
    for (int i = 0; i < data.length; i++) {
      checksum.update(data[i]); // Single byte updates
    }
    return checksum.getValue();
  }

  @Benchmark
  public long bufferedCRC32BulkUpdate() {
    BufferedChecksum checksum = new BufferedChecksum(new CRC32(), bufferSize);
    checksum.update(data, 0, data.length);
    return checksum.getValue();
  }

  // CRC32C benchmarks
  @Benchmark
  public long bufferedCRC32CSmallUpdates() {
    BufferedChecksum checksum = new BufferedChecksum(new CRC32C(), bufferSize);
    for (int i = 0; i < data.length; i++) {
      checksum.update(data[i]);
    }
    return checksum.getValue();
  }

  @Benchmark
  public long bufferedCRC32CBulkUpdate() {
    BufferedChecksum checksum = new BufferedChecksum(new CRC32C(), bufferSize);
    checksum.update(data, 0, data.length);
    return checksum.getValue();
  }
}
