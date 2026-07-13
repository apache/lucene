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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
 * Benchmarks range queries over GCD/delta-encoded sorted numeric doc values with multiple values
 * per doc.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class SortedNumericGcdRangeIntoBitSetBenchmark {

  private static final String FIELD = "val";
  private static final String LEAD_FIELD = "lead";
  private static final String LEAD_VALUE = "yes";
  private static final long DOMAIN = 10_000_000L;
  private static final long DELTA = 1_700_000_000_000L;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;
  private Path path;
  private Query query;

  @Param({"1000000"})
  public int numDocs;

  @Param({"delta_only", "gcd_1000", "gcd_100_delta"})
  public String encoding;

  @Param({"1", "3", "5"})
  public int cardinality;

  @Param({"0.01", "0.1", "0.5"})
  public double selectivity;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("sortedNumericGcdRange");
    dir = MMapDirectory.open(path);

    Random random = new Random(0);
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        long base = valueForDoc(i, random);
        for (int c = 0; c < cardinality; c++) {
          doc.add(SortedNumericDocValuesField.indexedField(FIELD, base + c * step()));
        }
        doc.add(new StringField(LEAD_FIELD, LEAD_VALUE, Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    query = rangeQuery();
  }

  private long valueForDoc(int doc, Random random) {
    long value = random.nextLong(0, DOMAIN);
    return switch (encoding) {
      case "delta_only" -> DELTA + value;
      case "gcd_1000" -> value * 1_000L;
      case "gcd_100_delta" -> DELTA + value * 100L;
      default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
    };
  }

  private long step() {
    return switch (encoding) {
      case "delta_only" -> 1;
      case "gcd_1000" -> 1_000L;
      case "gcd_100_delta" -> 100L;
      default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
    };
  }

  private Query rangeQuery() {
    long range = Math.max(1, (long) (DOMAIN * selectivity));
    long min = (DOMAIN - range) / 2;
    long max = min + range;
    long actualMin = actualValue(min);
    long actualMax = actualValue(max);
    Query rangeQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, actualMin, actualMax);
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term(LEAD_FIELD, LEAD_VALUE)), Occur.FILTER)
        .add(rangeQuery, Occur.FILTER)
        .build();
  }

  private long actualValue(long value) {
    return switch (encoding) {
      case "delta_only" -> DELTA + value;
      case "gcd_1000" -> value * 1_000L;
      case "gcd_100_delta" -> DELTA + value * 100L;
      default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
    };
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
  public int rangeQueryDefaultProvider() throws IOException {
    return searcher.count(query);
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
  public int rangeQueryPanamaProvider() throws IOException {
    return searcher.count(query);
  }
}
