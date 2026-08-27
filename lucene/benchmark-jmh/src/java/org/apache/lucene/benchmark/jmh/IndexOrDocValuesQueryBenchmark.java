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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
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
 * Benchmarks the points-vs-DV decision in IndexOrDocValuesQuery (LUCENE-7897 penalty). The 8x
 * penalty predates DocValuesSkipper (2017). With block-level skipping, DV is competitive.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 2, warmups = 1)
public class IndexOrDocValuesQueryBenchmark {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private Path path;

  private BooleanQuery crossover10Query;
  private BooleanQuery crossover20Query;
  private BooleanQuery crossover30Query;
  private BooleanQuery dvFavorableQuery;
  private BooleanQuery pointsFavorableQuery;

  @Param({"1000000", "10000000"})
  public int docCount;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("idvqBench");
    dir = MMapDirectory.open(path);

    IndexWriterConfig iwc = new IndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);

    int numBuckets = 100;
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new LongField("timestamp", i, Field.Store.NO));
      doc.add(new KeywordField("bucket", "b" + (i % numBuckets), Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();
    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);

    Query range80 = LongField.newRangeQuery("timestamp", 0, docCount * 4L / 5);

    // 10% lead + 80% range. 8x→points, 4x/2x→DV.
    crossover10Query = buildConjunction(range80, 10, numBuckets);

    // 20% lead + 80% range. 8x/4x→points, 2x→DV.
    // indexCost=800K, leadCost=200K. 8x: 100K<=200K→pts. 4x: 200K<=200K→pts. 2x: 400K>200K→DV.
    crossover20Query = buildConjunction(range80, 20, numBuckets);

    // 30% lead + 80% range. 8x/4x/2x→points.
    // indexCost=800K, leadCost=300K. 2x: 400K>300K→DV. So 2x still DV here.
    // Actually 2x threshold = 400K > 300K → DV. Need 1x for points: 800K > 300K → DV.
    // So 30% lead is still DV at 2x. This tests DV with more docs to check.
    crossover30Query = buildConjunction(range80, 30, numBuckets);

    // DV favorable: 1% lead + 80% range. Both 8x and 4x choose DV.
    dvFavorableQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("bucket", "b0")), Occur.FILTER)
            .add(range80, Occur.FILTER)
            .build();

    // Points favorable: 50% lead + 5% range. Both choose points.
    Query range5 = LongField.newRangeQuery("timestamp", 0, docCount / 20);
    BooleanQuery.Builder lead50 = new BooleanQuery.Builder();
    for (int i = 0; i < 50; i++) {
      lead50.add(new TermQuery(new Term("bucket", "b" + i)), Occur.SHOULD);
    }
    pointsFavorableQuery =
        new BooleanQuery.Builder()
            .add(lead50.build(), Occur.FILTER)
            .add(range5, Occur.FILTER)
            .build();
  }

  private static BooleanQuery buildConjunction(Query range, int leadBuckets, int totalBuckets) {
    BooleanQuery.Builder lead = new BooleanQuery.Builder();
    for (int i = 0; i < leadBuckets; i++) {
      lead.add(new TermQuery(new Term("bucket", "b" + i)), Occur.SHOULD);
    }
    return new BooleanQuery.Builder()
        .add(lead.build(), Occur.FILTER)
        .add(range, Occur.FILTER)
        .build();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }

  @Benchmark
  public TopDocs crossover10() throws IOException {
    return searcher.search(crossover10Query, 10);
  }

  @Benchmark
  public TopDocs crossover20() throws IOException {
    return searcher.search(crossover20Query, 10);
  }

  @Benchmark
  public TopDocs crossover30() throws IOException {
    return searcher.search(crossover30Query, 10);
  }

  @Benchmark
  public TopDocs dvFavorable() throws IOException {
    return searcher.search(dvFavorableQuery, 10);
  }

  @Benchmark
  public TopDocs pointsFavorable() throws IOException {
    return searcher.search(pointsFavorableQuery, 10);
  }
}
