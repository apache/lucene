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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
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
 * Benchmarks BooleanQuery with multiple numeric range FILTER clauses.
 *
 * <p>Run with and without the MultiFieldDocValuesRangeQuery coordination changes to compare. To
 * benchmark the baseline, revert the BooleanQuery.rewrite() coordination rule and re-run.
 *
 * <p>Data patterns:
 *
 * <ul>
 *   <li>clustered: values increase with docID (tight skip blocks, many YES/NO). Best case.
 *   <li>mixed: field0 monotonic, field1 low-cardinality, rest random. Realistic.
 *   <li>sorted: field0 monotonic (index sort key), rest random. Tests pre-sorted indexes.
 *   <li>random: all fields uniform random. Worst case.
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, warmups = 1)
public class MultiFieldDocValuesRangeBenchmark {

  private static final String CLUSTERED = "clustered";
  private static final String MIXED = "mixed";
  private static final String SORTED = "sorted";
  private static final String RANDOM = "random";

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private BooleanQuery query;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"1000000", "10000000"})
    public int docCount;

    @Param({"3", "5"})
    public int fieldCount;

    @Param({CLUSTERED, MIXED, RANDOM, SORTED})
    public String dataPattern;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws Exception {
    path = Files.createTempDirectory("multiFieldBench");
    dir = MMapDirectory.open(path);

    IndexWriterConfig iwc = new IndexWriterConfig();
    if (params.dataPattern.equals(SORTED)) {
      iwc.setIndexSort(
          new org.apache.lucene.search.Sort(
              new org.apache.lucene.search.SortField(
                  "field0", org.apache.lucene.search.SortField.Type.LONG)));
    }

    IndexWriter w = new IndexWriter(dir, iwc);
    Random r = new Random(42);

    for (int i = 0; i < params.docCount; i++) {
      Document doc = new Document();
      for (int f = 0; f < params.fieldCount; f++) {
        long value = generateValue(params.dataPattern, f, i, params.docCount, r);
        doc.add(NumericDocValuesField.indexedField("field" + f, value));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();

    BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    for (int f = 0; f < params.fieldCount; f++) {
      long[] range = getQueryRange(params.dataPattern, f, params.docCount);
      bqBuilder.add(
          SortedNumericDocValuesField.newSlowRangeQuery("field" + f, range[0], range[1]),
          Occur.FILTER);
    }
    query = bqBuilder.build();
  }

  private static long generateValue(
      String pattern, int fieldIdx, int docIdx, int docCount, Random r) {
    switch (pattern) {
      case CLUSTERED:
        long scale = (fieldIdx + 1) * 100L;
        long noise = r.nextInt(50);
        return (docIdx * scale / docCount) * docCount / scale * scale + noise;
      case MIXED:
        if (fieldIdx == 0) {
          return (long) docIdx * 1000L + r.nextInt(100);
        } else if (fieldIdx == 1) {
          return r.nextInt(20);
        } else {
          return r.nextLong(0, docCount);
        }
      case SORTED:
        // field0: monotonically increasing (will be the index sort key)
        // field1+: random values (not sorted — these benefit from coordination)
        if (fieldIdx == 0) {
          return (long) docIdx * 1000L + r.nextInt(100);
        } else {
          return r.nextLong(0, docCount);
        }
      case RANDOM:
        return r.nextLong(0, docCount);
      default:
        throw new IllegalArgumentException("Unknown pattern: " + pattern);
    }
  }

  private static long[] getQueryRange(String pattern, int fieldIdx, int docCount) {
    switch (pattern) {
      case CLUSTERED:
        long scale = (fieldIdx + 1) * 100L;
        long maxVal = scale;
        long rangeSize = maxVal / 10;
        long offset = (fieldIdx * maxVal / 5);
        return new long[] {offset, offset + rangeSize};
      case MIXED:
        if (fieldIdx == 0) {
          long maxVal2 = (long) docCount * 1000L + 100;
          return new long[] {(long) (maxVal2 * 0.9), maxVal2};
        } else if (fieldIdx == 1) {
          return new long[] {15, 19};
        } else {
          long rangeSize2 = docCount / 10;
          long lower2 = (docCount - rangeSize2) / 2;
          return new long[] {lower2, lower2 + rangeSize2};
        }
      case SORTED:
        if (fieldIdx == 0) {
          long maxVal3 = (long) docCount * 1000L + 100;
          return new long[] {(long) (maxVal3 * 0.9), maxVal3};
        } else {
          long rangeSize3 = docCount / 10;
          long lower3 = (docCount - rangeSize3) / 2;
          return new long[] {lower3, lower3 + rangeSize3};
        }
      case RANDOM:
        long rangeSize4 = docCount / 5;
        long lower4 = (docCount - rangeSize4) / 2;
        return new long[] {lower4, lower4 + rangeSize4};
      default:
        throw new IllegalArgumentException("Unknown pattern: " + pattern);
    }
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
  public int searchMultiFieldRange() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher.count(query);
  }
}
