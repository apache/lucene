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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.cutters.LongValueFacetCutter;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
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

/** JMH benchmark for {@link LongValueFacetCutter} throughput. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 3)
public class LongValueFacetCutterBenchmark {
  Directory dir;
  IndexReader reader;
  IndexSearcher searcher;
  Path path;

  @Setup(Level.Trial)
  public void setup(BenchmarkParams params) throws Exception {
    path = Files.createTempDirectory("longValueFacetCutter");
    dir = MMapDirectory.open(path);
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    Random r = new Random(42);

    for (int i = 0; i < params.docCount; i++) {
      Document doc = new Document();
      // Indexed point for range query filtering
      doc.add(new LongPoint("id", i));
      if (params.multiValued) {
        int numValues = r.nextInt(1, 4);
        for (int v = 0; v < numValues; v++) {
          doc.add(new SortedNumericDocValuesField("f", r.nextInt(0, params.cardinality)));
        }
      } else {
        doc.add(new NumericDocValuesField("f", r.nextInt(0, params.cardinality)));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1, true);
    reader = DirectoryReader.open(w);
    searcher = new IndexSearcher(reader);
    w.close();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    if (dir != null) {
      dir.close();
      dir = null;
    }
    if (Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.delete(p);
                  } catch (IOException _) {
                    // ignore
                  }
                });
      }
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkParams {
    @Param({"100000", "1000000"})
    public int docCount;

    @Param({"100", "10000"})
    public int cardinality;

    @Param({"false", "true"})
    public boolean multiValued;
  }

  /** Facet count over all documents. */
  @Benchmark
  public CountFacetRecorder matchAll(BenchmarkParams params) throws IOException {
    LongValueFacetCutter cutter = new LongValueFacetCutter("f");
    CountFacetRecorder recorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);
    searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);
    return recorder;
  }

  /** Facet count over ~10% of documents filtered by a point range query. */
  @Benchmark
  public CountFacetRecorder filteredRange(BenchmarkParams params) throws IOException {
    long lower = params.docCount / 4;
    long upper = lower + params.docCount / 10;
    LongValueFacetCutter cutter = new LongValueFacetCutter("f");
    CountFacetRecorder recorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);
    searcher.search(LongPoint.newRangeQuery("id", lower, upper), collectorManager);
    return recorder;
  }
}
