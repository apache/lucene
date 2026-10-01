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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
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
 * Benchmarks bulk retrieval of dense binary doc values via {@link BinaryDocValues#binaryValues}.
 * Compares the per-doc default with the Lucene90 codec override that reads directly from the data
 * slice.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BinaryDocValuesBulkDecodeBenchmark {

  private Directory dir;
  private DirectoryReader reader;
  private BinaryDocValues values;
  private Path path;
  private int[] docs;
  private BytesRef[] valueBuffer;
  private int nextStart;

  @Param({"1000000"})
  public int docCount;

  @Param({"8", "32", "128"})
  public int valueLength;

  @Param({"128", "1024"})
  public int batchSize;

  @Param({"fixed", "variable"})
  public String encoding;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("binaryDocValuesBulkDecode");
    dir = MMapDirectory.open(path);

    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Random random = new Random(0);
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      int len;
      if (encoding.equals("fixed")) {
        len = valueLength;
      } else {
        len = 1 + random.nextInt(valueLength);
      }
      byte[] bytes = new byte[len];
      random.nextBytes(bytes);
      doc.add(new BinaryDocValuesField("field", new BytesRef(bytes)));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = DirectoryReader.open(writer);
    writer.close();

    values = reader.leaves().get(0).reader().getBinaryDocValues("field");
    docs = new int[batchSize];
    valueBuffer = new BytesRef[batchSize];
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
  public int binaryValuesBulk() throws IOException {
    return readBatchBulk();
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
  public int binaryValuesPerDoc() throws IOException {
    return readBatchPerDoc();
  }

  private int readBatchBulk() throws IOException {
    final int maxStart = docCount - batchSize;
    if (nextStart > maxStart) {
      nextStart = 0;
    }
    for (int i = 0; i < batchSize; i++) {
      docs[i] = nextStart + i;
    }
    nextStart += batchSize;

    values.binaryValues(batchSize, docs, valueBuffer);
    int checksum = 0;
    for (int i = 0; i < batchSize; i++) {
      checksum += valueBuffer[i].length;
    }
    return checksum;
  }

  private int readBatchPerDoc() throws IOException {
    final int maxStart = docCount - batchSize;
    if (nextStart > maxStart) {
      nextStart = 0;
    }

    int checksum = 0;
    for (int i = 0; i < batchSize; i++) {
      int doc = nextStart + i;
      values.advanceExact(doc);
      BytesRef ref = BytesRef.deepCopyOf(values.binaryValue());
      checksum += ref.length;
    }
    nextStart += batchSize;
    return checksum;
  }
}
