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
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
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

/**
 * Benchmarks bulk retrieval of dense numeric doc values via {@link NumericDocValues#longValues}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class NumericDocValuesBulkDecodeBenchmark {

  private Directory dir;
  private DirectoryReader reader;
  private NumericDocValues values;
  private Path path;
  private int[] docs;
  private long[] valueBuffer;
  private int nextStart;

  @Param({"1000000"})
  public int docCount;

  @Param({"8", "16", "24", "32", "40", "48", "56", "64"})
  public int bitsPerValue;

  @Param({"128", "1024"})
  public int batchSize;

  @Param({"contiguous", "strided"})
  public String accessPattern;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("numericDocValuesBulkDecode");
    dir = MMapDirectory.open(path);

    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Random random = new Random(0);
    long mask = bitsPerValue == Long.SIZE ? -1L : (1L << bitsPerValue) - 1;
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      long value = bitsPerValue == Long.SIZE ? random.nextLong() : random.nextLong() & mask;
      doc.add(new NumericDocValuesField("field", value));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = DirectoryReader.open(writer);
    writer.close();

    values = reader.leaves().get(0).reader().getNumericDocValues("field");
    docs = new int[batchSize];
    valueBuffer = new long[batchSize];
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    if (Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.delete(p);
                  } catch (IOException _) {
                  }
                });
      }
    }
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
  public long longValuesDefaultProvider() throws IOException {
    return readBatch();
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsAppend = {
        "--add-modules",
        "jdk.incubator.vector",
        "-Xmx2g",
        "-Xms2g",
        "-XX:+AlwaysPreTouch"
      })
  public long longValuesPanamaProvider() throws IOException {
    return readBatch();
  }

  private long readBatch() throws IOException {
    final int step = accessPattern.equals("contiguous") ? 1 : 2;
    final int maxStart = docCount - (batchSize - 1) * step - 1;
    if (nextStart > maxStart) {
      nextStart = 0;
    }
    for (int i = 0, doc = nextStart; i < batchSize; i++, doc += step) {
      docs[i] = doc;
    }
    nextStart += batchSize * step;

    values.longValues(batchSize, docs, valueBuffer, 0);
    long checksum = 0;
    for (int i = 0; i < batchSize; i++) {
      checksum += valueBuffer[i];
    }
    return checksum;
  }
}
