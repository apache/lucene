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
import java.util.concurrent.TimeUnit;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
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
 * Benchmarks dense interleaved MUST_NOT term disjunctions over a MatchAllDocsQuery.
 *
 * <p>Most documents are indexed with every prohibited term except one rotating hole, which keeps
 * term posting lists dense enough that {@code DisjunctionDISIApproximation} splits some clauses
 * into linear-scanned iterators. Periodic pass-through documents have no prohibited terms, so the
 * query returns a small non-zero hit count while repeatedly invoking {@code ReqExclBulkScorer} and
 * the exclusion disjunction's {@code docIDRunEnd()} path. The default setup builds 10M documents
 * and force-merges to one segment to match the target regression scenario.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 1,
    warmups = 1,
    jvmArgsAppend = {"-Xmx4g", "-Xms4g", "-XX:+AlwaysPreTouch"})
public class MustNotBooleanQueryBenchmark {

  private static final String EXCLUDED_FIELD = "excluded";
  private static final String EXCLUDED_TERM_PREFIX = "term";
  private static final int PASS_THROUGH_INTERVAL = 128;

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private Path path;
  private Query query;
  private TotalHitCountCollectorManager collectorManager;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"10000000"})
    public int docCount;

    @Param({"3", "7", "11"})
    public int mustNotTermCount;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws IOException {
    path = Files.createTempDirectory("mustNotBooleanQuery");
    dir = MMapDirectory.open(path);

    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int docID = 0; docID < params.docCount; docID++) {
        writer.addDocument(document(docID, params.mustNotTermCount));
      }
      writer.forceMerge(1);
      reader = DirectoryReader.open(writer);
    }

    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    query = query(params.mustNotTermCount);
    collectorManager = new TotalHitCountCollectorManager(searcher.getSlices());
    int expectedHitCount = (params.docCount - 1) / PASS_THROUGH_INTERVAL + 1;
    int actualHitCount = searcher.search(query, collectorManager);
    if (actualHitCount != expectedHitCount) {
      throw new AssertionError("expected " + expectedHitCount + " hits but got " + actualHitCount);
    }
  }

  private static Document document(int docID, int mustNotTermCount) {
    Document doc = new Document();
    if (docID % PASS_THROUGH_INTERVAL == 0) {
      return doc;
    }
    int missingTerm = docID % mustNotTermCount;
    for (int termOrd = 0; termOrd < mustNotTermCount; termOrd++) {
      if (termOrd != missingTerm) {
        doc.add(new StringField(EXCLUDED_FIELD, term(termOrd), Field.Store.NO));
      }
    }
    return doc;
  }

  private static Query query(int mustNotTermCount) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(MatchAllDocsQuery.INSTANCE, Occur.FILTER);
    for (int termOrd = 0; termOrd < mustNotTermCount; termOrd++) {
      builder.add(new TermQuery(new Term(EXCLUDED_FIELD, term(termOrd))), Occur.MUST_NOT);
    }
    return builder.build();
  }

  private static String term(int termOrd) {
    return EXCLUDED_TERM_PREFIX + termOrd;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    try {
      IOUtils.close(reader, dir);
    } finally {
      reader = null;
      dir = null;
      if (path != null) {
        IOUtils.rm(path);
        path = null;
      }
    }
  }

  @Benchmark
  public int searchMustNot() throws IOException {
    return searcher.search(query, collectorManager);
  }
}
