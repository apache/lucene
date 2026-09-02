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
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for MaxScoreBulkScorer filter bitset optimization. Measures throughput of scoring a
 * disjunction with a dense filter at the BulkScorer level, avoiding IndexSearcher overhead.
 *
 * <p>Designed to stress the old leap-frog filter path: many dense, staggered clauses so the
 * priority queue is large and shuffles frequently, combined with a dense filter so the old path
 * does many filter.advance() calls per inner window.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class MaxScoreBulkScorerFilterBenchmark {

  /** Number of scoring SHOULD clauses */
  @Param({"10", "20"})
  public int numClauses;

  /** Filter density: 1.0 = match-all, 0.8 = 80% dense */
  @Param({"1.0", "0.8"})
  public double filterDensity;

  private Directory dir;
  private IndexReader reader;
  private Weight weightWithFilter;
  private LeafReaderContext leaf;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    dir = new MMapDirectory(java.nio.file.Files.createTempDirectory("benchmark"));
    int numDocs = 500_000;
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        // Dense, heavily staggered clauses: each term ~15% density with unique period/offset
        // so multiple clauses often match the same doc, forcing PQ activity.
        for (int t = 0; t < numClauses; t++) {
          int period = 7 + t * 11; // 7, 18, 29, 40, ... ~14% avg density
          int offset = (t * 7919) % period;
          if ((i + offset) % period == 0) {
            doc.add(new TextField("text", "word" + t, Field.Store.NO));
          }
        }
        // Evenly distributed filter
        if (filterDensity >= 1.0) {
          doc.add(new StringField("filter", "yes", Field.Store.NO));
        } else {
          int filterThreshold = (int) (filterDensity * 1000);
          if ((i * 7919) % 1000 < filterThreshold) {
            doc.add(new StringField("filter", "yes", Field.Store.NO));
          } else {
            doc.add(new StringField("filter", "no", Field.Store.NO));
          }
        }
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(dir);
    leaf = reader.leaves().get(0);

    BooleanQuery.Builder bqWithFilter = new BooleanQuery.Builder();
    bqWithFilter.setMinimumNumberShouldMatch(1);
    for (int t = 0; t < numClauses; t++) {
      bqWithFilter.add(new TermQuery(new Term("text", "word" + t)), BooleanClause.Occur.SHOULD);
    }
    if (filterDensity >= 1.0) {
      bqWithFilter.add(
          new org.apache.lucene.search.MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
    } else {
      bqWithFilter.add(new TermQuery(new Term("filter", "yes")), BooleanClause.Occur.FILTER);
    }

    org.apache.lucene.search.IndexSearcher searcher =
        new org.apache.lucene.search.IndexSearcher(reader);
    weightWithFilter =
        searcher.createWeight(searcher.rewrite(bqWithFilter.build()), ScoreMode.TOP_SCORES, 1f);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    dir.close();
  }

  @Benchmark
  public void benchmarkWithFilter(Blackhole bh) throws IOException {
    BulkScorer scorer = weightWithFilter.bulkScorer(leaf);
    scorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            bh.consume(doc);
          }
        },
        leaf.reader().getLiveDocs(),
        0,
        leaf.reader().maxDoc());
  }
}
