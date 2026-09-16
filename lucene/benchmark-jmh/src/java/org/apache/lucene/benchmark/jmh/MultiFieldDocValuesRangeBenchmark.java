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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
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
  // All fields random; every clause matches ~70% so the intersection stays dense (stresses the
  // bit-set intersection path).
  private static final String DENSE = "dense";
  // field0 matches ~all docs (a dense clause), every other field matches ~0.1% (very sparse), so a
  // tiny surviving set must be confirmed against a dense clause (stresses survivor pruning).
  private static final String DENSE_SPARSE = "dense_sparse";

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private BooleanQuery query;
  private Query singleRange;
  private Query sparseNoSkipRange;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"1000000", "10000000"})
    public int docCount;

    @Param({"1", "3", "5"})
    public int fieldCount;

    @Param({CLUSTERED, MIXED, RANDOM, SORTED, DENSE, DENSE_SPARSE})
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
      // A sparsely populated field WITHOUT a skip index (plain NumericDocValuesField). Its range
      // iterator is a DocValuesValueRangeIterator whose approximation reports a real (sparse) cost,
      // so a range on it routes to ConstantScoreBulkScorer's two-phase path rather than
      // DenseConjunctionBulkScorer (which is what the skip-indexed fields above always hit).
      if (i % 100 == 0) {
        doc.add(new NumericDocValuesField("sparse", i));
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
    // For fieldCount=1 on non-sorted patterns, add a MatchAllDocsQuery so
    // DenseConjunctionBulkScorer is used and intoBitSet() is called on the range iterator
    // (enabling the SIMD path). Without this, a single-clause BooleanQuery rewrites to the
    // query itself and goes through DefaultBulkScorer which doesn't call intoBitSet().
    // For the sorted pattern, field0 is the index sort key so
    // getDocIdSetIteratorOrNullForPrimarySort
    // fires and returns DocIdSetIterator.range() — adding MatchAllDocsQuery here would force it
    // through DenseConjunctionBulkScorer and bypass that fast path, causing a regression.
    if (params.fieldCount == 1 && !params.dataPattern.equals(SORTED)) {
      bqBuilder.add(new org.apache.lucene.search.MatchAllDocsQuery(), Occur.FILTER);
    }
    query = bqBuilder.build();

    long[] range0 = getQueryRange(params.dataPattern, 0, params.docCount);
    singleRange = SortedNumericDocValuesField.newSlowRangeQuery("field0", range0[0], range0[1]);

    sparseNoSkipRange =
        SortedNumericDocValuesField.newSlowRangeQuery("sparse", 0, params.docCount / 2);
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
      case DENSE:
      case DENSE_SPARSE:
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
      case DENSE:
        // Each clause matches ~70% of docs, so the intersection stays dense.
        return new long[] {0, (long) (docCount * 0.7)};
      case DENSE_SPARSE:
        if (fieldIdx == 0) {
          // Dense clause: matches all docs.
          return new long[] {0, docCount};
        }
        // Very sparse clause: matches ~0.1% of docs.
        long rangeSize5 = Math.max(1, docCount / 1000);
        long lower5 = (docCount - rangeSize5) / 2;
        return new long[] {lower5, lower5 + rangeSize5};
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

  // A bare single range, driven through the bulk scorer. We collect (index-order, no early
  // termination) rather than count() because SortedNumericDocValuesRangeQuery#count()
  // short-circuits
  // via the skip index and never builds a scorer. Selectivity follows the data pattern:
  // clustered ~ very sparse, random ~ 20%, dense ~ 70%.
  @Benchmark
  public TopDocs searchSingleRange() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher.search(
        singleRange, new TopFieldCollectorManager(Sort.INDEXORDER, 10, Integer.MAX_VALUE));
  }

  // A range on the sparse, non-skip-indexed field. Its DocValuesValueRangeIterator reports a real
  // (sparse) cost, so this is the one shape that routes to ConstantScoreBulkScorer's two-phase path
  // (ours) / DefaultBulkScorer (#16143's fallback) instead of DenseConjunctionBulkScorer. Forced
  // through the bulk scorer via a collector (count() would fall back to scoring anyway with no skip
  // index, but the collector makes the path unambiguous).
  @Benchmark
  public TopDocs searchSparseNoSkipRange() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher.search(
        sparseNoSkipRange, new TopFieldCollectorManager(Sort.INDEXORDER, 10, Integer.MAX_VALUE));
  }
}
