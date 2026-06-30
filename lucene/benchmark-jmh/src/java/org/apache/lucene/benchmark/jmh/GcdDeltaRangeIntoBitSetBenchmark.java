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

/** Benchmarks range queries over dense numeric doc values encoded as raw, delta, GCD, or both. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class GcdDeltaRangeIntoBitSetBenchmark {

  private static final String FIELD = "val";
  private static final String LEAD_FIELD = "lead";
  private static final String LEAD_VALUE = "yes";
  private static final String NONE = "none";
  private static final String DELTA_ONLY = "delta_only";
  private static final String GCD_1000 = "gcd_1000";
  private static final String GCD_100_DELTA = "gcd_100_delta";
  private static final long DOMAIN = 10_000_000L;
  private static final long DELTA = 1_700_000_000_000L;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;
  private Path path;
  private Query query;

  @Param({"1000000"})
  public int numDocs;

  @Param({NONE, DELTA_ONLY, GCD_1000, GCD_100_DELTA})
  public String encoding;

  @Param({"0.01", "0.1", "0.5"})
  public double selectivity;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    path = Files.createTempDirectory("gcdDeltaRangeIntoBitSet");
    dir = MMapDirectory.open(path);

    Random random = new Random(0);
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(NumericDocValuesField.indexedField(FIELD, valueForDoc(encoding, i, random)));
        doc.add(new StringField(LEAD_FIELD, LEAD_VALUE, Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    query = rangeQuery(encoding, selectivity);
  }

  private static long valueForDoc(String encoding, int doc, Random random) {
    if (doc == 0) {
      return minimumValue(encoding);
    } else if (doc == 1 && encoding.equals(GCD_100_DELTA)) {
      // Anchor entry.gcd to exactly 100 for the GCD_100_DELTA encoding: random multiples of 100
      // could otherwise share a larger common factor under some seeds, which would change the
      // shape of the encoded values and what the benchmark measures.
      return DELTA + 100L;
    }

    long value = random.nextLong(0, DOMAIN);
    switch (encoding) {
      case NONE:
        return value;
      case DELTA_ONLY:
        return DELTA + value;
      case GCD_1000:
        return value * 1_000L;
      case GCD_100_DELTA:
        return DELTA + value * 100L;
      default:
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }
  }

  private static long minimumValue(String encoding) {
    switch (encoding) {
      case NONE:
      case GCD_1000:
        return 0;
      case DELTA_ONLY:
      case GCD_100_DELTA:
        return DELTA;
      default:
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }
  }

  private static Query rangeQuery(String encoding, double selectivity) {
    long range = Math.max(1, (long) (DOMAIN * selectivity));
    long min = (DOMAIN - range) / 2;
    long max = min + range;
    Query rangeQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(
            FIELD, actualValue(encoding, min), actualValue(encoding, max));
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term(LEAD_FIELD, LEAD_VALUE)), Occur.FILTER)
        .add(rangeQuery, Occur.FILTER)
        .build();
  }

  private static long actualValue(String encoding, long value) {
    switch (encoding) {
      case NONE:
        return value;
      case DELTA_ONLY:
        return DELTA + value;
      case GCD_1000:
        return value * 1_000L;
      case GCD_100_DELTA:
        return DELTA + value * 100L;
      default:
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }
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
