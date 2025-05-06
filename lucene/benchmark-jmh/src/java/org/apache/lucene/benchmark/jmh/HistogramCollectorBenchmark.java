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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.plain.histograms.HistogramCollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
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

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 3)
public class HistogramCollectorBenchmark {
  Directory dir;
  IndexReader reader;
  Path path;

  @Setup(Level.Trial)
  public void setup(BenchmarkParams params) throws Exception {
    path = Files.createTempDirectory("forUtil");
    Directory dir = MMapDirectory.open(path);
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    Random r = new Random(0);

    for (int i = 0; i < params.docCount; i++) {
      Document doc = new Document();
      long value = r.nextInt(0, params.docCount);
      if (params.pointEnabled) {
        // Adding indexed point field to verify multi range collector
        doc.add(new LongPoint("f", value));
      } else {
        doc.add(NumericDocValuesField.indexedField("f", value));
      }
      w.addDocument(doc);
    }
    // Force merging into single segment for testing more documents in segment scenario
    w.forceMerge(1, true);
    reader = DirectoryReader.open(w);
    w.close();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    if (dir != null) {
      dir.close();
      dir = null;
    }

    // Clean up the segment files before next run
    if (Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException _) {
                    // Do nothing
                  }
                });
      }
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkParams {
    // Test with both point enabled and disabled
    @Param({"true", "false"})
    public boolean pointEnabled;

    @Param({"500000", "5000000"})
    public int docCount;

    @Param({"5000", "25000"})
    public long bucketWidth;
  }

  @Benchmark
  public void collectHistogram(BenchmarkParams params) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.search(
        new MatchAllDocsQuery(), new HistogramCollectorManager("f", params.bucketWidth, 10000));
  }
}
