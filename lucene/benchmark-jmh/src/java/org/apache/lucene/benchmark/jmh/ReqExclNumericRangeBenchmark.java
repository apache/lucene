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
import org.apache.lucene.document.NumericDocValuesField;
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
 * Benchmarks numeric range queries in MUST_NOT position, comparing against the equivalent range
 * query in FILTER position.
 *
 * <p>A numeric range query in FILTER position uses {@code rangeIntoBitSet} to bulk-fill a BitSet
 * with matching doc IDs. NO blocks (all values outside the range) are skipped entirely by the
 * approximation. For MAYBE blocks (only some values in range), {@code rangeIntoBitSet} leverages
 * the Panama vector API to process multiple documents per SIMD instruction.
 *
 * <p>A numeric range query in MUST_NOT position is driven by {@code ReqExclBulkScorer}, which
 * calls {@code docIDRunEnd()} on the exclusion iterator. This path has two inefficiencies relative
 * to the FILTER path. First, NO blocks are not free: the exclusion approximation skips past them,
 * but the required scorer must still traverse and collect every document in those blocks as a hit.
 * Second, for MAYBE blocks (only some values excluded), {@code docIDRunEnd()} returns only the
 * current document, so each excluded doc requires a per-document {@code matches()} call with no
 * SIMD benefit. Only YES blocks (all values in the excluded range) allow efficient skipping via
 * {@code docIDRunEnd()}, which returns the block boundary so the whole block is bypassed at once.
 *
 * <p>These two costs interact with selectivity in opposite ways. At low {@code selectivity} (small
 * excluded range), NO blocks dominate: the FILTER path is nearly free (few hits, most blocks
 * skipped), while the MUST_NOT path must collect nearly all documents through the required scorer.
 * At high {@code selectivity} (large excluded range), YES blocks dominate and {@code docIDRunEnd()}
 * can skip efficiently, narrowing the gap.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class ReqExclNumericRangeBenchmark {

  private static final String FIELD = "val";
  private static final String LEAD_FIELD = "lead";
  private static final String LEAD_VALUE = "yes";
  private static final long DOMAIN = 10_000_000L;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;
  private Path path;
  /** Range as FILTER: uses {@code rangeIntoBitSet} with SIMD acceleration on MAYBE blocks. */
  private Query filterQuery;
  /** Range as MUST_NOT: uses {@code ReqExclBulkScorer}; MAYBE blocks fall back to per-doc exclusion. */
  private Query notEqualsQuery;

  @Param({"1000000"})
  public int numDocs;

  /** Fraction of documents whose value falls within the query range. */
  @Param({"0.01", "0.1", "0.5", "0.9", "0.99"})
  public double selectivity;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("reqExclNumericRange");
    dir = MMapDirectory.open(path);

    Random random = new Random(0);
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(NumericDocValuesField.indexedField(FIELD, random.nextLong(DOMAIN)));
        doc.add(new StringField(LEAD_FIELD, LEAD_VALUE, Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);

    long range = Math.max(1, (long) (DOMAIN * selectivity));
    long min = (DOMAIN - range) / 2;
    long max = min + range;

    Query rangeQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, min, max);
    Query leadQuery = new TermQuery(new Term(LEAD_FIELD, LEAD_VALUE));

    filterQuery =
        new BooleanQuery.Builder()
            .add(leadQuery, Occur.FILTER)
            .add(rangeQuery, Occur.FILTER)
            .build();

    notEqualsQuery =
        new BooleanQuery.Builder()
            .add(leadQuery, Occur.FILTER)
            .add(rangeQuery, Occur.MUST_NOT)
            .build();
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
  public int filterRangeDefaultProvider() throws IOException {
    return searcher.count(filterQuery);
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
  public int filterRangePanamaProvider() throws IOException {
    return searcher.count(filterQuery);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
  public int notEqualsRangeDefaultProvider() throws IOException {
    return searcher.count(notEqualsQuery);
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
  public int notEqualsRangePanamaProvider() throws IOException {
    return searcher.count(notEqualsQuery);
  }
}
