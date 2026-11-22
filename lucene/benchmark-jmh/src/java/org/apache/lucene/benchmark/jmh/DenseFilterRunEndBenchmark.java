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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Scores a dense filter backed by a {@link BitSetIterator}, the shape of a filter cached by {@code
 * LRUQueryCache} as a {@link FixedBitSet}. A dense constant-score iterator is scored by {@code
 * DenseConjunctionBulkScorer}, which calls {@link DocIdSetIterator#docIDRunEnd()} once per window.
 *
 * <p>Shapes:
 *
 * <ul>
 *   <li>{@code liveDocs}: the segment has deletions, so windows stay at 4096 docs and do not jump
 *       to the end of the run via {@code collectRange}.
 *   <li>{@code liveDocsSlice}: same, but only the first 1/16th of the doc ID space is scored, like
 *       a doc-partitioned slice or an early-terminated search.
 *   <li>{@code conjunction}: no deletions, but a second clause (here the collector's competitive
 *       iterator) does not fully match the window, which also keeps windows at 4096 docs.
 * </ul>
 *
 * <p>Filters:
 *
 * <ul>
 *   <li>{@code all}: every doc matches, e.g. a time range that covers the whole segment.
 *   <li>{@code prefix}: one run over the first 90% of the docs, e.g. a time range on a segment
 *       sorted by time.
 *   <li>{@code striped}: one non-matching doc every 1000 docs, a control with short runs.
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class DenseFilterRunEndBenchmark {

  private static final String LIVE_DOCS = "liveDocs";
  private static final String LIVE_DOCS_SLICE = "liveDocsSlice";
  private static final String CONJUNCTION = "conjunction";

  @Param({"1000000", "10000000", "50000000"})
  public int maxDoc;

  @Param({"all", "prefix", "striped"})
  public String filter;

  @Param({LIVE_DOCS, LIVE_DOCS_SLICE, CONJUNCTION})
  public String shape;

  private FixedBitSet filterBits;
  private long filterCost;
  private FixedBitSet liveDocs;
  private FixedBitSet competitiveBits;
  private int max;
  private int expectedCount;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    filterBits = new FixedBitSet(maxDoc);
    switch (filter) {
      case "all" -> filterBits.set(0, maxDoc);
      case "prefix" -> filterBits.set(0, maxDoc / 10 * 9);
      case "striped" -> {
        filterBits.set(0, maxDoc);
        for (int i = 999; i < maxDoc; i += 1000) {
          filterBits.clear(i);
        }
      }
      default -> throw new IllegalArgumentException("Unknown filter: " + filter);
    }
    filterCost = filterBits.cardinality();

    switch (shape) {
      case LIVE_DOCS, LIVE_DOCS_SLICE -> {
        // ~0.1% deleted docs
        liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        for (int i = 517; i < maxDoc; i += 1009) {
          liveDocs.clear(i);
        }
      }
      case CONJUNCTION -> {
        // One non-competitive doc every 64 docs: dense, but never a long run.
        competitiveBits = new FixedBitSet(maxDoc);
        competitiveBits.set(0, maxDoc);
        for (int i = 63; i < maxDoc; i += 64) {
          competitiveBits.clear(i);
        }
      }
      default -> throw new IllegalArgumentException("Unknown shape: " + shape);
    }
    max = LIVE_DOCS_SLICE.equals(shape) ? maxDoc / 16 : maxDoc;

    String bulkScorerName = bulkScorer().getClass().getSimpleName();
    if (bulkScorerName.equals("DenseConjunctionBulkScorer") == false) {
      throw new AssertionError("Expected DenseConjunctionBulkScorer, got " + bulkScorerName);
    }
    expectedCount = expectedCount();
    int actual = score();
    if (actual != expectedCount) {
      throw new AssertionError("count=" + actual + ", expected=" + expectedCount);
    }
  }

  private BulkScorer bulkScorer() throws IOException {
    return ConstantScoreScorerSupplier.fromIterator(
            new BitSetIterator(filterBits, filterCost), 1f, ScoreMode.COMPLETE_NO_SCORES, maxDoc)
        .bulkScorer();
  }

  private int expectedCount() {
    FixedBitSet expected = new FixedBitSet(maxDoc);
    expected.set(0, max);
    expected.and(filterBits);
    if (liveDocs != null) {
      expected.and(liveDocs);
    }
    if (competitiveBits != null) {
      expected.and(competitiveBits);
    }
    return expected.cardinality();
  }

  @Benchmark
  public int score() throws IOException {
    DocIdSetIterator competitiveIterator =
        competitiveBits == null ? null : new BitSetIterator(competitiveBits, maxDoc);
    CountingCollector collector = new CountingCollector(competitiveIterator);
    Bits acceptDocs = liveDocs;
    bulkScorer().score(collector, acceptDocs, 0, max);
    return collector.count;
  }

  private static class CountingCollector implements LeafCollector {
    private final DocIdSetIterator competitiveIterator;
    int count;

    CountingCollector(DocIdSetIterator competitiveIterator) {
      this.competitiveIterator = competitiveIterator;
    }

    @Override
    public void setScorer(Scorable scorer) {}

    @Override
    public void collect(int doc) {
      count++;
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      for (int streamCount = stream.count(); streamCount != 0; streamCount = stream.count()) {
        count += streamCount;
      }
    }

    @Override
    public void collectRange(int min, int max) {
      count += max - min;
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      return competitiveIterator;
    }
  }
}
