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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.lucene101.ForDeltaUtil;
import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.codecs.lucene101.PostingIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class PostingIndexInputBenchmark {

  private Path path;
  private Directory dir;
  private IndexInput in;
  private PostingIndexInput postingIn;
  private final ForUtil forUtil = new ForUtil();
  private final ForDeltaUtil forDeltaUtil = new ForDeltaUtil();
  private final int[] values = new int[ForUtil.BLOCK_SIZE];

  @Param({"2", "3", "4", "5", "6", "7", "8", "9", "10"})
  public int bpv;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("forUtil");
    dir = MMapDirectory.open(path);
    try (IndexOutput out = dir.createOutput("docs", IOContext.DEFAULT)) {
      Random r = new Random(0);
      // Write enough random data to not reach EOF while decoding
      for (int i = 0; i < 100; ++i) {
        out.writeLong(r.nextLong());
      }
    }
    in = dir.openInput("docs", IOContext.DEFAULT);
    postingIn = new PostingIndexInput(in, forUtil, forDeltaUtil);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (dir != null) {
      dir.deleteFile("docs");
    }
    IOUtils.close(in, dir);
    in = null;
    dir = null;
    Files.deleteIfExists(path);
  }

  @Benchmark
  public void decode(Blackhole bh) throws IOException {
    in.seek(3); // random unaligned offset
    postingIn.decode(bpv, values);
    bh.consume(values);
  }

  @Benchmark
  public void decodeAndPrefixSum(Blackhole bh) throws IOException {
    in.seek(3); // random unaligned offset
    postingIn.decodeAndPrefixSum(bpv, 100, values);
    bh.consume(values);
  }
}
