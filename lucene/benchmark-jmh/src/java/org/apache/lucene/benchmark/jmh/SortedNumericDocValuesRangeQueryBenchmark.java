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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
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

/** Benchmarks skip-indexed sorted-numeric doc values range queries. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, warmups = 1)
public class SortedNumericDocValuesRangeQueryBenchmark {

  private static final String CLUSTERED = "clustered";
  private static final String MIXED = "mixed";
  private static final String RANDOM = "random";
  private static final String DENSE = "dense";
  private static final String SPARSE = "sparse";
  private static final String FIXED = "fixed";
  private static final String VARIABLE = "variable";
  private static final String PLAIN = "plain";
  private static final String CONJUNCTION = "conjunction";
  private static final long VALUE_SPACE = 1_000_000L;

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private Query query;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"1000000"})
    public int docCount;

    @Param({CLUSTERED, MIXED, RANDOM})
    public String dataPattern;

    @Param({"2", "4", "8"})
    public int valuesPerDoc;

    @Param({FIXED, VARIABLE})
    public String cardinality;

    @Param({DENSE, SPARSE})
    public String density;

    @Param({"0.01", "0.1", "0.5"})
    public double selectivity;

    @Param({PLAIN, CONJUNCTION})
    public String queryShape;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws Exception {
    path = Files.createTempDirectory("sortedNumericRangeBench");
    dir = MMapDirectory.open(path);

    IndexWriterConfig iwc = new IndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, iwc);
    Random random = new Random(42);
    long[] values = new long[params.valuesPerDoc];

    for (int docID = 0; docID < params.docCount; docID++) {
      Document doc = new Document();
      doc.add(new StringField("all", "yes", Store.NO));
      if (params.density.equals(DENSE) || (docID & 1) == 0) {
        int valueCount =
            params.cardinality.equals(FIXED)
                ? params.valuesPerDoc
                : 1 + random.nextInt(params.valuesPerDoc);
        fillValues(values, valueCount, params.dataPattern, docID, params.docCount, random);
        for (int i = 0; i < valueCount; i++) {
          doc.add(SortedNumericDocValuesField.indexedField("sn", values[i]));
        }
      }
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = DirectoryReader.open(writer);
    writer.close();

    long rangeWidth = Math.max(1L, (long) (VALUE_SPACE * params.selectivity));
    long rangeStart = (VALUE_SPACE - rangeWidth) / 2;
    Query rangeQuery =
        SortedNumericDocValuesField.newSlowRangeQuery("sn", rangeStart, rangeStart + rangeWidth);
    query =
        switch (params.queryShape) {
          case PLAIN -> rangeQuery;
          case CONJUNCTION ->
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("all", "yes")), Occur.FILTER)
                  .add(rangeQuery, Occur.FILTER)
                  .build();
          default ->
              throw new IllegalArgumentException("Unknown query shape: " + params.queryShape);
        };
  }

  private static void fillValues(
      long[] values, int valueCount, String dataPattern, int docID, int docCount, Random random) {
    for (int i = 0; i < valueCount; i++) {
      values[i] =
          switch (dataPattern) {
            case CLUSTERED -> clusteredValue(docID, docCount, i, valueCount);
            case MIXED ->
                i == 0
                    ? clusteredValue(docID, docCount, i, valueCount)
                    : random.nextLong(VALUE_SPACE);
            case RANDOM -> random.nextLong(VALUE_SPACE);
            default -> throw new IllegalArgumentException("Unknown pattern: " + dataPattern);
          };
    }
    Arrays.sort(values, 0, valueCount);
  }

  private static long clusteredValue(int docID, int docCount, int valueIndex, int valueCount) {
    long base = docID * VALUE_SPACE / docCount;
    long spread = Math.max(1L, VALUE_SPACE / docCount);
    return Math.min(VALUE_SPACE - 1L, base + valueIndex * spread / valueCount);
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
                  }
                });
      }
    }
  }

  @Benchmark
  public int searchSortedNumericRange() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher.count(query);
  }
}
