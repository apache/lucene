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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
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
 * Benchmarks filtered top-score disjunctions that use {@link
 * org.apache.lucene.search.MaxScoreBulkScorer}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, warmups = 1)
public class MaxScoreFilterBenchmark {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private Path path;
  private Query query;

  @Param({"1000000"})
  public int docCount;

  @Param({"100", "1000"})
  public int essentialInterval;

  @Param({"1", "10", "1000"})
  public int filterInterval;

  @Param({"2"})
  public int lowInterval;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    path = Files.createTempDirectory("maxScoreFilterBench");
    dir = MMapDirectory.open(path);

    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < docCount; ++i) {
        Document doc = new Document();
        if (matchesFilter(i, filterInterval)) {
          doc.add(new StringField("filter", "yes", Field.Store.NO));
        }
        if (i % essentialInterval == 0) {
          doc.add(new StringField("foo", "high", Field.Store.NO));
        }
        if (i % lowInterval == 1) {
          doc.add(new StringField("foo", "low", Field.Store.NO));
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    query = buildQuery();
  }

  private static boolean matchesFilter(int doc, int interval) {
    if (interval == 1) {
      return true;
    }
    int h = doc;
    h ^= h >>> 16;
    h *= 0x7feb352d;
    h ^= h >>> 15;
    h *= 0x846ca68b;
    h ^= h >>> 16;
    return (h & 0x7fffffff) % interval == 0;
  }

  private static Query buildQuery() {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new BoostQuery(new TermQuery(new Term("foo", "high")), 4f), Occur.SHOULD);
    builder.add(new BoostQuery(new TermQuery(new Term("foo", "low")), 1f), Occur.SHOULD);
    builder.add(new TermQuery(new Term("filter", "yes")), Occur.FILTER);
    return builder.build();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
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
  public TopDocs searchTopScores() throws IOException {
    return searcher.search(query, new TopScoreDocCollectorManager(10, 1));
  }
}
