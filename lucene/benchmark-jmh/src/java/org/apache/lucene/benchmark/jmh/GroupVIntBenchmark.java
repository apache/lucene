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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.GroupVIntUtil;
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
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 1,
    jvmArgsPrepend = {"--add-modules=jdk.unsupported"})
public class GroupVIntBenchmark {

  // Cumulative frequency for each number of bits per value used by doc deltas of tail postings on
  // wikibigall.
  private static final float[] CUMULATIVE_FREQUENCY_BY_BITS_REQUIRED =
      new float[] {
        0.0f,
        0.01026574f,
        0.021453038f,
        0.03342156f,
        0.046476692f,
        0.060890317f,
        0.07644147f,
        0.093718216f,
        0.11424741f,
        0.13989712f,
        0.17366524f,
        0.22071244f,
        0.2815692f,
        0.3537585f,
        0.43655503f,
        0.52308f,
        0.6104675f,
        0.7047371f,
        0.78155357f,
        0.8671179f,
        0.9740598f,
        1.0f
      };

  final int maxSize = 256;
  final long[] docs = new long[maxSize];
  final long[] values = new long[maxSize];

  IndexInput byteBufferGVIntIn;
  IndexInput nioGVIntIn;
  IndexInput byteBufferVIntIn;
  ByteBuffersDataInput byteBuffersGVIntIn;

  ByteArrayDataInput byteArrayVIntIn;
  ByteArrayDataInput byteArrayGVIntIn;

  // benchmark for write
  ByteBuffersDataOutput byteBuffersGVIntOut = new ByteBuffersDataOutput();

  @Param({"64"})
  public int size;

  void initArrayInput(long[] docs) throws Exception {
    byte[] gVIntBytes = new byte[Integer.BYTES * maxSize * 2];
    byte[] vIntBytes = new byte[Integer.BYTES * maxSize * 2];
    ByteArrayDataOutput vIntOut = new ByteArrayDataOutput(vIntBytes);
    ByteArrayDataOutput out = new ByteArrayDataOutput(gVIntBytes);
    out.writeGroupVInts(docs, docs.length);
    for (long v : docs) {
      vIntOut.writeVInt((int) v);
    }
    byteArrayVIntIn = new ByteArrayDataInput(vIntBytes);
    byteArrayGVIntIn = new ByteArrayDataInput(gVIntBytes);
  }

  void initNioInput(long[] docs) throws Exception {
    Directory dir = new NIOFSDirectory(Files.createTempDirectory("groupvintdata"));
    IndexOutput out = dir.createOutput("gvint", IOContext.DEFAULT);
    out.writeGroupVInts(docs, docs.length);
    out.close();
    nioGVIntIn = dir.openInput("gvint", IOContext.DEFAULT);
  }

  void initByteBuffersInput(long[] docs) throws Exception {
    ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
    buffer.writeGroupVInts(docs, docs.length);
    byteBuffersGVIntIn = buffer.toDataInput();
  }

  void initByteBufferInput(long[] docs) throws Exception {
    Directory dir = new MMapDirectory(Files.createTempDirectory("groupvintdata"));
    IndexOutput vintOut = dir.createOutput("vint", IOContext.DEFAULT);
    IndexOutput gvintOut = dir.createOutput("gvint", IOContext.DEFAULT);

    gvintOut.writeGroupVInts(docs, docs.length);
    for (long v : docs) {
      vintOut.writeVInt((int) v);
    }
    vintOut.close();
    gvintOut.close();
    byteBufferGVIntIn = dir.openInput("gvint", IOContext.DEFAULT);
    byteBufferVIntIn = dir.openInput("vint", IOContext.DEFAULT);
  }

  private void readGroupVIntsBaseline(DataInput in, long[] dst, int limit) throws IOException {
    int i;
    for (i = 0; i <= limit - 4; i += 4) {
      GroupVIntUtil.readGroupVInt(in, dst, i);
    }
    for (; i < limit; ++i) {
      dst[i] = in.readVInt();
    }
  }

  @Setup(Level.Trial)
  public void init() throws Exception {
    Random r = new Random(0);
    for (int i = 0; i < maxSize; ++i) {
      float randomFloat = r.nextFloat();
      // Reproduce the distribution of the number of bits per values that we're observing for tail
      // postings on wikibigall.
      int numBits = 1 + Arrays.binarySearch(CUMULATIVE_FREQUENCY_BY_BITS_REQUIRED, randomFloat);
      if (numBits < 0) {
        numBits = -numBits;
      }
      docs[i] = r.nextInt(1 << (numBits - 1), 1 << numBits);
    }
    initByteBufferInput(docs);
    initArrayInput(docs);
    initNioInput(docs);
    initByteBuffersInput(docs);
  }

  @Benchmark
  public void benchMMapDirectoryInputs_readVInt(Blackhole bh) throws IOException {
    byteBufferVIntIn.seek(0);
    for (int i = 0; i < size; i++) {
      values[i] = byteBufferVIntIn.readVInt();
    }
    bh.consume(values);
  }

  @Benchmark
  public void benchMMapDirectoryInputs_readGroupVInt(Blackhole bh) throws IOException {
    byteBufferGVIntIn.seek(0);
    GroupVIntUtil.readGroupVInts(byteBufferGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchMMapDirectoryInputs_readGroupVIntBaseline(Blackhole bh) throws IOException {
    byteBufferGVIntIn.seek(0);
    this.readGroupVIntsBaseline(byteBufferGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchByteArrayDataInput_readVInt(Blackhole bh) {
    byteArrayVIntIn.rewind();
    for (int i = 0; i < size; i++) {
      values[i] = byteArrayVIntIn.readVInt();
    }
    bh.consume(values);
  }

  @Benchmark
  public void benchByteArrayDataInput_readGroupVInt(Blackhole bh) throws IOException {
    byteArrayGVIntIn.rewind();
    GroupVIntUtil.readGroupVInts(byteArrayGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchNIOFSDirectoryInputs_readGroupVInt(Blackhole bh) throws IOException {
    nioGVIntIn.seek(0);
    GroupVIntUtil.readGroupVInts(nioGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchNIOFSDirectoryInputs_readGroupVIntBaseline(Blackhole bh) throws IOException {
    nioGVIntIn.seek(0);
    this.readGroupVIntsBaseline(nioGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchByteBuffersIndexInput_readGroupVInt(Blackhole bh) throws IOException {
    byteBuffersGVIntIn.seek(0);
    GroupVIntUtil.readGroupVInts(byteBuffersGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void benchByteBuffersIndexInput_readGroupVIntBaseline(Blackhole bh) throws IOException {
    byteBuffersGVIntIn.seek(0);
    this.readGroupVIntsBaseline(byteBuffersGVIntIn, values, size);
    bh.consume(values);
  }

  @Benchmark
  public void bench_writeGroupVInt(Blackhole bh) throws IOException {
    byteBuffersGVIntOut.reset();
    byteBuffersGVIntOut.writeGroupVInts(docs, size);
  }
}
