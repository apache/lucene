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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDConfig;
import org.apache.lucene.util.bkd.BKDWriter;
import org.apache.lucene.util.bkd.DocIdsWriter;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
public class BKDCodecBenchmark {

  private static final int SIZE = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

  @Param({"16", "24"})
  public int bpv;

  private Directory dir;
  private DocIdsWriter legacy;
  private IndexInput legacyIn;
  private DocIdsWriter vector;
  private IndexInput vectorIn;
  private int[] docs;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    Path path = Files.createTempDirectory("bkd");
    dir = MMapDirectory.open(path);
    docs = new int[SIZE];
    legacy = new DocIdsWriter(SIZE, BKDWriter.VERSION_META_FILE);
    legacyIn = writeDocIds("legacy", docs, legacy);
    vector = new DocIdsWriter(SIZE, BKDWriter.VERSION_VECTORIZED_DOCID);
    vectorIn = writeDocIds("current", docs, vector);
  }

  private IndexInput writeDocIds(String file, int[] docs, DocIdsWriter writer) throws IOException {
    try (IndexOutput out = dir.createOutput(file, IOContext.DEFAULT)) {
      Random r = new Random(0);
      // avoid cluster encoding
      docs[0] = 1;
      docs[1] = (1 << bpv) - 1;
      for (int i = 2; i < SIZE; ++i) {
        docs[i] = r.nextInt(1 << bpv);
      }
      writer.writeDocIds(docs, 0, SIZE, out);
    }
    return dir.openInput(file, IOContext.DEFAULT);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    legacyIn.seek(0);
    vectorIn.seek(0);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws IOException {
    IOUtils.close(legacyIn, vectorIn, dir);
  }

  private int count(int iter) {
    return iter % 20 == 0 ? SIZE - 1 : SIZE;
  }

  @Benchmark
  public void scalar(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      legacy.readInts(legacyIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }

  @Benchmark
  public void vector(Blackhole bh) throws IOException {
    for (int i = 0; i <= 100; i++) {
      int count = count(i);
      vector.readInts(vectorIn, count, docs);
      bh.consume(docs);
      setupInvocation();
    }
  }
}
