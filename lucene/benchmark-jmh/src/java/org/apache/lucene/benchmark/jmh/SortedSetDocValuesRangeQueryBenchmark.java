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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
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
 * Benchmarks {@link SortedSetDocValuesField#newSlowRangeQuery} when skip-indexed sorted-set doc
 * values route through a two-phase {@link DocValuesRangeIterator} (whose {@code intoBitSet}
 * bulk-evaluates skip blocks) and dense bulk scoring.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, warmups = 1)
public class SortedSetDocValuesRangeQueryBenchmark {

  private static final String FIELD = "dv";
  private static final int VALUE_MODULUS = 1024;
  private static final long RANGE_MIN = 256;
  private static final long RANGE_MAX = 511;

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private Query query;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"100000", "1000000"})
    public int docCount;

    @Param({"1", "2"})
    public int valuesPerDoc;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws Exception {
    path = Files.createTempDirectory("sortedSetDvRangeBench");
    dir = MMapDirectory.open(path);

    IndexWriterConfig iwc = new IndexWriterConfig();
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < params.docCount; i++) {
        Document doc = new Document();
        addSortedSetValue(doc, i % VALUE_MODULUS);
        if (params.valuesPerDoc == 2) {
          addSortedSetValue(doc, VALUE_MODULUS + (i % VALUE_MODULUS));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      reader = DirectoryReader.open(w);
    }

    query =
        SortedSetDocValuesField.newSlowRangeQuery(
            FIELD, encodedValue(RANGE_MIN), encodedValue(RANGE_MAX), true, true);

    verifyBenchmarkPath(params);
  }

  private void verifyBenchmarkPath(Params params) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    LeafReaderContext leaf = reader.leaves().get(0);
    if (leaf.reader().getDocValuesSkipper(FIELD) == null) {
      throw new IllegalStateException("benchmark requires a doc-values skipper");
    }
    boolean singleton =
        DocValues.unwrapSingleton(DocValues.getSortedSet(leaf.reader(), FIELD)) != null;
    if (singleton != (params.valuesPerDoc == 1)) {
      throw new IllegalStateException(
          "unexpected sorted-set singleton state for valuesPerDoc=" + params.valuesPerDoc);
    }

    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
    if (scorerSupplier instanceof ConstantScoreScorerSupplier == false) {
      throw new IllegalStateException(
          "expected ConstantScoreScorerSupplier but got " + scorerSupplier.getClass().getName());
    }
    DocIdSetIterator iterator =
        ((ConstantScoreScorerSupplier) scorerSupplier).iterator(Long.MAX_VALUE);
    TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iterator);
    if (twoPhase instanceof DocValuesRangeIterator == false) {
      throw new IllegalStateException(
          "expected a two-phase DocValuesRangeIterator but got "
              + (twoPhase == null ? iterator.getClass().getName() : twoPhase.getClass().getName()));
    }
    String bulkScorerClass = scorerSupplier.bulkScorer().getClass().getName();
    if (bulkScorerClass.endsWith(".DenseConjunctionBulkScorer") == false) {
      throw new IllegalStateException(
          "expected DenseConjunctionBulkScorer but got " + bulkScorerClass);
    }
  }

  private static void addSortedSetValue(Document doc, long value) {
    doc.add(SortedSetDocValuesField.indexedField(FIELD, encodedValue(value)));
  }

  private static BytesRef encodedValue(long value) {
    byte[] encoded = new byte[Long.BYTES];
    LongPoint.encodeDimension(value, encoded, 0);
    return new BytesRef(encoded);
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
  public int searchSortedSetRange(Params params) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher.count(query);
  }
}
