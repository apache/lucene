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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
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
 * Compares MUST_NOT execution with sparse required queries.
 *
 * <p>The term, point and index-or-doc-values required queries match the same document IDs. The
 * prohibited index-or-doc-values query matches all documents except one every 128 documents.
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
public class SparseRequiredMustNotBenchmark {

  private static final int DOC_COUNT = 10_000_000;
  private static final int EXCLUDED_INTERVAL = 10;
  private static final String REQUIRED_TERM_FIELD = "required_term";
  private static final String REQUIRED_NUMERIC_FIELD_PREFIX = "required_numeric_";
  private static final String EXCLUDED_NUMERIC_FIELD = "excluded_numeric";

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private Path path;
  private Query query;
  private TotalHitCountCollectorManager collectorManager;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"TERM", "POINT", "IODV"})
    public String requiredType;

    // Required percentage of documents to match
    @Param({"10", "5", "1", "0.5", "0.1", "0.01", "0.001"})
    public double requiredPercentage;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws IOException {
    path = Files.createTempDirectory("sparseRequiredMustNot");
    dir = MMapDirectory.open(path);
    int requiredInterval = interval(params.requiredPercentage);
    String requiredValue = Double.toString(params.requiredPercentage);
    int expectedHitCount = 0;
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int docID = 0; docID < DOC_COUNT; docID++) {
        boolean required = docID % requiredInterval == 0;
        boolean excluded = docID % EXCLUDED_INTERVAL != 0;
        writer.addDocument(document(required, excluded, requiredValue));
        if (required && !excluded) {
          expectedHitCount++;
        }
      }
      writer.forceMerge(1);
      reader = DirectoryReader.open(writer);
    }

    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    query = query(params.requiredType, params.requiredPercentage);
    collectorManager = new TotalHitCountCollectorManager(searcher.getSlices());

    int actualHitCount = searcher.search(query, collectorManager);
    if (actualHitCount != expectedHitCount) {
      throw new AssertionError("expected " + expectedHitCount + " hits but got " + actualHitCount);
    }
  }

  private static Query query(String requiredType, double requiredPercentage) {
    String requiredValue = Double.toString(requiredPercentage);
    String numericField = REQUIRED_NUMERIC_FIELD_PREFIX + requiredValue;
    Query required =
        switch (requiredType) {
          case "TERM" -> new TermQuery(new Term(REQUIRED_TERM_FIELD, requiredValue));
          case "POINT" -> LongPoint.newExactQuery(numericField, 1);
          case "IODV" ->
              new IndexOrDocValuesQuery(
                  LongPoint.newExactQuery(numericField, 1),
                  NumericDocValuesField.newSlowExactQuery(numericField, 1));
          default -> throw new IllegalArgumentException("Unknown required type: " + requiredType);
        };
    Query excluded =
        new IndexOrDocValuesQuery(
            LongPoint.newExactQuery(EXCLUDED_NUMERIC_FIELD, 1),
            NumericDocValuesField.newSlowExactQuery(EXCLUDED_NUMERIC_FIELD, 1));
    return new BooleanQuery.Builder()
        .add(required, Occur.FILTER)
        .add(excluded, Occur.MUST_NOT)
        .build();
  }

  private static int interval(double percentage) {
    return (int) Math.round(100 / percentage);
  }

  private static Document document(boolean required, boolean excluded, String requiredValue) {
    Document doc = new Document();
    if (required) {
      String numericField = REQUIRED_NUMERIC_FIELD_PREFIX + requiredValue;
      doc.add(new StringField(REQUIRED_TERM_FIELD, requiredValue, Field.Store.NO));
      doc.add(new LongPoint(numericField, 1));
      doc.add(new NumericDocValuesField(numericField, 1));
    }
    if (excluded) {
      doc.add(new LongPoint(EXCLUDED_NUMERIC_FIELD, 1));
      doc.add(new NumericDocValuesField(EXCLUDED_NUMERIC_FIELD, 1));
    }
    return doc;
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
