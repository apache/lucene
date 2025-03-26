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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SparseFixedBitSet;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules=jdk.incubator.vector"
    })
public class AdvanceSparseDISIBenchmark {
  private Path path1;
  private Path path2;
  private Path path3;
  private Path path4;
  private Directory dir1;
  private Directory dir2;
  private Directory dir3;
  private Directory dir4;
  private IndexInput in1;
  private IndexInput in2;
  private IndexInput in3;
  private IndexInput in4;
  private IndexedDISI disi1;
  private IndexedDISI disi2;
  private IndexedDISI disi3;
  private IndexedDISI disi4;
  private static final int DOC_INTERVAL = 10;
  private List<Integer> targets = new ArrayList<>();

  @Setup(Level.Trial)
  public void setup() throws Exception {
    final int B = 65536;
    int maxDoc = B * 2;
    BitSet set = new SparseFixedBitSet(maxDoc);
    for (int i = 0; i < 4000; i++) {
      set.set(i);
      if (i % DOC_INTERVAL == 0) {
        targets.add(i);
      }
    }

    for (int i = 0; i < 4000; i++) {
      set.set(i + B);
      if (i % DOC_INTERVAL == 0) {
        targets.add(i + B);
      }
    }

    path1 = Files.createTempDirectory("disi1");
    path2 = Files.createTempDirectory("disi2");
    path3 = Files.createTempDirectory("disi3");
    path4 = Files.createTempDirectory("disi4");
    dir1 = MMapDirectory.open(path1);
    dir2 = MMapDirectory.open(path2);
    dir3 = MMapDirectory.open(path3);
    dir4 = MMapDirectory.open(path4);

    final int cardinality = set.cardinality();
    final byte denseRankPower = 9; // Not tested here so fixed to isolate factors
    long length1;
    int jumpTableEntryCount1;
    try (IndexOutput out = dir1.createOutput("foo1", IOContext.DEFAULT)) {
      jumpTableEntryCount1 =
          IndexedDISI.writeBitSet(new BitSetIterator(set, cardinality), out, denseRankPower);
      length1 = out.getFilePointer();
    }

    in1 = dir1.openInput("foo1", IOContext.DEFAULT);
    disi1 = new IndexedDISI(in1, 0L, length1, jumpTableEntryCount1, denseRankPower, cardinality);

    long length2;
    int jumpTableEntryCount2;
    try (IndexOutput out = dir2.createOutput("foo2", IOContext.DEFAULT)) {
      jumpTableEntryCount2 =
          IndexedDISI.writeBitSet(new BitSetIterator(set, cardinality), out, denseRankPower);
      length2 = out.getFilePointer();
    }

    in2 = dir2.openInput("foo2", IOContext.DEFAULT);
    disi2 = new IndexedDISI(in2, 0L, length2, jumpTableEntryCount2, denseRankPower, cardinality);

    long length3;
    int jumpTableEntryCount3;
    try (IndexOutput out = dir3.createOutput("foo3", IOContext.DEFAULT)) {
      jumpTableEntryCount3 =
          IndexedDISI.writeBitSet(new BitSetIterator(set, cardinality), out, denseRankPower);
      length3 = out.getFilePointer();
    }

    in3 = dir3.openInput("foo3", IOContext.DEFAULT);
    disi3 = new IndexedDISI(in3, 0L, length3, jumpTableEntryCount3, denseRankPower, cardinality);

    long length4;
    int jumpTableEntryCount4;
    try (IndexOutput out = dir4.createOutput("foo4", IOContext.DEFAULT)) {
      jumpTableEntryCount4 =
          IndexedDISI.writeBitSet(new BitSetIterator(set, cardinality), out, denseRankPower);
      length4 = out.getFilePointer();
    }

    in4 = dir4.openInput("foo4", IOContext.DEFAULT);
    disi4 = new IndexedDISI(in4, 0L, length4, jumpTableEntryCount4, denseRankPower, cardinality);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (dir1 != null) {
      dir1.deleteFile("foo1");
    }
    IOUtils.close(in1, dir1);
    in1 = null;
    dir1 = null;
    Files.deleteIfExists(path1);

    if (dir2 != null) {
      dir2.deleteFile("foo2");
    }
    IOUtils.close(in2, dir2);
    in2 = null;
    dir2 = null;
    Files.deleteIfExists(path2);

    if (dir3 != null) {
      dir3.deleteFile("foo3");
    }
    IOUtils.close(in3, dir3);
    in3 = null;
    dir3 = null;
    Files.deleteIfExists(path3);

    if (dir4 != null) {
      dir4.deleteFile("foo4");
    }
    IOUtils.close(in4, dir4);
    in4 = null;
    dir4 = null;
    Files.deleteIfExists(path4);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void advance() throws IOException {
    for (int target : targets) {
      disi1.advance(target);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void advanceBinarySearch() throws IOException {
    for (int target : targets) {
      disi2.advanceBinarySearch(target);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void advanceExact() throws IOException {
    for (int target : targets) {
      disi3.advanceExact(target);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void advanceExactBinarySearch() throws IOException {
    for (int target : targets) {
      disi4.advanceExactBinarySearch(target);
    }
  }

  public static void main(String[] args) throws Exception {
    //     For debugging.
    AdvanceSparseDISIBenchmark advanceSparseDISIBenchmark = new AdvanceSparseDISIBenchmark();
    advanceSparseDISIBenchmark.setup();
    advanceSparseDISIBenchmark.advance();
    advanceSparseDISIBenchmark.advanceBinarySearch();
    advanceSparseDISIBenchmark.advanceExact();
    advanceSparseDISIBenchmark.advanceExactBinarySearch();
    advanceSparseDISIBenchmark.tearDown();
  }
}
