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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DenseLiveDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.SparseLiveDocs;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks comparing {@link SparseLiveDocs} vs {@link DenseLiveDocs} performance across different
 * deletion rates, patterns, and segment sizes.
 *
 * <p>This benchmark suite measures five key operations to evaluate the trade-offs between sparse
 * and dense LiveDocs implementations:
 *
 * <ul>
 *   <li><b>Random access (get)</b> - O(1) for both, but sparse has additional indirection overhead
 *   <li><b>Deleted docs iteration</b> - O(deletedDocs) for sparse, O(maxDoc) for dense
 *   <li><b>Live docs iteration (full)</b> - O(maxDoc) for both, tests get() performance at scale
 *   <li><b>Live docs iteration (range)</b> - O(range) for both, tests advance() and get() on subset
 *   <li><b>Mask application (applyMask)</b> - masks the candidate documents of a segment one
 *       4096-document window at a time, which is the shape the bulk scorers use
 * </ul>
 *
 * <h2>Benchmark Parameters</h2>
 *
 * <ul>
 *   <li><b>maxDoc</b> - Segment sizes: 100K, 1M, 10M documents
 *   <li><b>deletionRate</b> - Percentage of deleted documents: 0.1%, 1%, 5%, 10%, 20%, 30%
 *   <li><b>deletionPattern</b> - Distribution of deletions:
 *       <ul>
 *         <li>RANDOM: Deletions scattered uniformly across entire document space
 *         <li>CLUSTERED: Consecutive deletions at start of segment
 *         <li>UNIFORM: Deletions evenly spaced across segment
 *       </ul>
 *   <li><b>candidateRate</b> - Fraction of the segment set in the bit set passed to {@code
 *       applyMask}: 1%, 10% or 100%. Only applies to the {@code applyMask} benchmarks.
 *   <li><b>maskImpl</b> - Which {@code Bits#applyMask} implementation the {@code applyMask}
 *       benchmarks call: BULK for the override, DEFAULT for the per-bit {@code Bits} default
 *       implementation it replaces. Only applies to the {@code applyMask} benchmarks.
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Run all benchmarks:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsBenchmark"
 * </pre>
 *
 * <p>Run specific operation for sparse only:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsBenchmark.sparseIterateDeleted"
 * </pre>
 *
 * <p>Filter by specific parameters:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsBenchmark" -p deletionRate=0.01 -p deletionPattern=CLUSTERED
 * </pre>
 *
 * @see SparseLiveDocs
 * @see DenseLiveDocs
 * @see LiveDocsPathologicalBenchmark
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class LiveDocsBenchmark {

  /** Number of documents in the segment (100K, 1M, or 10M). */
  @Param({"100000", "1000000", "10000000"})
  int maxDoc;

  /** Percentage of documents to delete (0.1%, 1%, 5%, 10%, 20%, or 30%). */
  @Param({"0.001", "0.01", "0.05", "0.10", "0.20", "0.30"})
  double deletionRate;

  /**
   * Pattern for distributing deletions:
   *
   * <ul>
   *   <li>RANDOM - Deletions scattered uniformly across document space
   *   <li>CLUSTERED - Consecutive deletions at start of segment
   *   <li>UNIFORM - Deletions evenly spaced across segment
   * </ul>
   */
  @Param({"RANDOM", "CLUSTERED", "UNIFORM"})
  String deletionPattern;

  /** Sparse LiveDocs implementation under test. */
  private SparseLiveDocs sparseLiveDocs;

  /** Dense LiveDocs implementation for comparison. */
  private DenseLiveDocs denseLiveDocs;

  /** {@link #sparseLiveDocs}, exposed so that {@code applyMask} resolves to the default impl. */
  private Bits sparseDefaultBits;

  /** {@link #denseLiveDocs}, exposed so that {@code applyMask} resolves to the default impl. */
  private Bits denseDefaultBits;

  /** Pre-generated random document IDs for random access benchmarks. */
  private int[] randomDocIds;

  /** Number of random accesses to perform in each benchmark iteration. */
  private static final int RANDOM_ACCESS_SIZE = 10000;

  /** Memory used by SparseLiveDocs in bytes. */
  private long sparseBytes;

  /** Memory used by DenseLiveDocs in bytes. */
  private long denseBytes;

  /** Memory overhead percentage (negative means sparse uses less memory). */
  private double overheadPct;

  /** Number of deleted documents. */
  private int deleted;

  /** Number of documents masked per {@code applyMask} call, as in the bulk scorers. */
  private static final int WINDOW = 4096;

  /** Which {@link Bits#applyMask} implementation the {@code applyMask} benchmarks call. */
  public enum MaskImpl {
    /** The bulk override implemented by {@link SparseLiveDocs} and {@link DenseLiveDocs}. */
    BULK,
    /** The per-bit {@link Bits} default implementation that the override replaces. */
    DEFAULT
  }

  /**
   * Candidate documents that the mask is applied to, held in its own state so that {@code
   * candidateRate} and {@code maskImpl} multiply the {@code applyMask} benchmarks only.
   *
   * <p>The candidates of the whole segment are kept as words, and each measured call refills a
   * single {@link #WINDOW}-document scratch bit set from them and masks that, which is how {@code
   * AcceptDocs} and the bulk scorers use {@code applyMask}: the destination has just been written
   * and is still in cache. The refill is part of every measured call, so it costs each {@code
   * maskImpl} the same.
   */
  @State(Scope.Thread)
  public static class Candidates {

    /**
     * Fraction of the segment's documents that are set in the candidate bit set (1%, 10%, 100%).
     */
    @Param({"0.01", "0.10", "1.0"})
    double candidateRate;

    /** Implementation of {@code applyMask} to call: the bulk override or the per-bit default. */
    @Param({"BULK", "DEFAULT"})
    MaskImpl maskImpl;

    /** Candidate documents of the whole segment, as words. */
    private long[] candidates;

    /** Scratch window that the mask is applied to. */
    FixedBitSet window;

    /** Number of whole windows in the segment. */
    int numWindows;

    /**
     * Builds the candidate documents over the enclosing benchmark's segment.
     *
     * @param benchmark the enclosing benchmark state, for its segment size
     */
    @Setup(Level.Trial)
    public void setup(final LiveDocsBenchmark benchmark) {
      Random random = new Random(17);
      FixedBitSet bitSet = new FixedBitSet(benchmark.maxDoc);
      if (candidateRate >= 1.0) {
        bitSet.set(0, benchmark.maxDoc);
      } else {
        for (int doc = 0; doc < benchmark.maxDoc; doc++) {
          if (random.nextDouble() < candidateRate) {
            bitSet.set(doc);
          }
        }
      }
      candidates = bitSet.getBits();
      window = new FixedBitSet(WINDOW);
      numWindows = benchmark.maxDoc / WINDOW;
    }

    /** Refills the scratch window with the candidates of the window starting at {@code base}. */
    void fillWindow(final int base) {
      System.arraycopy(candidates, base >> 6, window.getBits(), 0, WINDOW >> 6);
    }
  }

  /**
   * JMH auxiliary counters for tracking memory metrics across benchmark runs.
   *
   * <p>These metrics are reported as secondary results in JMH output and include: deletion count,
   * memory usage for both implementations, and overhead percentage.
   */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class LiveDocsMetrics {
    /** Number of deleted documents. */
    public int deleted;

    /** Memory used by SparseLiveDocs in bytes. */
    public long sparseBytes;

    /** Memory used by DenseLiveDocs in bytes. */
    public long denseBytes;

    /** Memory overhead percentage (negative means sparse uses less memory). */
    public double overheadPct;
  }

  /**
   * Sets up the benchmark by creating both sparse and dense LiveDocs with identical deletion
   * patterns.
   *
   * <p>This method is called once per trial (combination of parameters) before any benchmark
   * iterations run.
   */
  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    int numDeleted = (int) (maxDoc * deletionRate);

    if (numDeleted == 0) {
      throw new IllegalStateException(
          "Benchmark requires at least one deletion. "
              + "Current parameters: maxDoc="
              + maxDoc
              + ", deletionRate="
              + deletionRate
              + " result in zero deletions.");
    }

    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);

    switch (deletionPattern) {
      case "RANDOM":
        Set<Integer> deletedSet = new HashSet<>();
        while (deletedSet.size() < numDeleted) {
          deletedSet.add(random.nextInt(maxDoc));
        }
        for (int docId : deletedSet) {
          sparseSet.set(docId);
          fixedSet.clear(docId);
        }
        break;

      case "CLUSTERED":
        for (int i = 0; i < numDeleted; i++) {
          sparseSet.set(i);
          fixedSet.clear(i);
        }
        break;

      case "UNIFORM":
        for (int i = 0; i < numDeleted; i++) {
          int docId = (int) ((long) i * maxDoc / numDeleted);
          sparseSet.set(docId);
          fixedSet.clear(docId);
        }
        break;
    }

    sparseLiveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    denseLiveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();

    sparseDefaultBits = defaultApplyMask(sparseLiveDocs);
    denseDefaultBits = defaultApplyMask(denseLiveDocs);

    sparseBytes = sparseLiveDocs.ramBytesUsed();
    denseBytes = denseLiveDocs.ramBytesUsed();
    overheadPct = ((double) sparseBytes - denseBytes) / denseBytes * 100.0;
    deleted = (int) (maxDoc * deletionRate);

    randomDocIds = new int[RANDOM_ACCESS_SIZE];
    for (int i = 0; i < RANDOM_ACCESS_SIZE; i++) {
      randomDocIds[i] = random.nextInt(maxDoc);
    }
  }

  /**
   * Benchmarks random access (get) performance for {@link SparseLiveDocs}.
   *
   * <p>Tests 10,000 random get() operations on pre-generated random document IDs. Sparse
   * implementation has additional indirection overhead (block lookup + word lookup) compared to
   * dense.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void sparseRandomAccess(final LiveDocsMetrics metrics, final Blackhole blackhole) {
    fillMetrics(metrics);

    for (int docId : randomDocIds) {
      blackhole.consume(sparseLiveDocs.get(docId));
    }
  }

  /**
   * Benchmarks random access (get) performance for {@link DenseLiveDocs}.
   *
   * <p>Tests 10,000 random get() operations on pre-generated random document IDs. Dense
   * implementation uses simple array access with bit masking.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void denseRandomAccess(final LiveDocsMetrics metrics, final Blackhole blackhole) {
    fillMetrics(metrics);

    for (int docId : randomDocIds) {
      blackhole.consume(denseLiveDocs.get(docId));
    }
  }

  /**
   * Benchmarks iteration over deleted documents for {@link SparseLiveDocs}.
   *
   * <p>This is the primary use case for sparse LiveDocs. Sparse implementation only iterates over
   * actually deleted documents, making it much faster at low deletion rates.
   *
   * <p><b>Expected performance:</b>
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of deleted documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateDeleted(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    DocIdSetIterator it = sparseLiveDocs.deletedDocsIterator();
    int count = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      count++;
    }
    return count;
  }

  /**
   * Benchmarks iteration over deleted documents for {@link DenseLiveDocs}.
   *
   * <p>Dense implementation must scan all maxDoc positions to find deleted documents, making it
   * slower at low deletion rates but more predictable.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of deleted documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateDeleted(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    DocIdSetIterator it = denseLiveDocs.deletedDocsIterator();
    int count = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      count++;
    }
    return count;
  }

  /**
   * Benchmarks full iteration over live (non-deleted) documents for {@link SparseLiveDocs}.
   *
   * <p>Tests iteration over all live documents in the segment. Performance depends on deletion
   * pattern:
   *
   * <ul>
   *   <li>CLUSTERED: Excellent (3-5× faster than dense)
   *   <li>UNIFORM: Good (2-4× faster than dense)
   *   <li>RANDOM: Variable (4× faster at 0.1%, but can be 2.4× SLOWER at 30%)
   * </ul>
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of live documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateLiveDocs(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    DocIdSetIterator it = sparseLiveDocs.liveDocsIterator();
    int count = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      count++;
    }
    return count;
  }

  /**
   * Benchmarks full iteration over live (non-deleted) documents for {@link DenseLiveDocs}.
   *
   * <p>Dense implementation provides consistent, predictable performance regardless of deletion
   * pattern or rate.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of live documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateLiveDocs(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    DocIdSetIterator it = denseLiveDocs.liveDocsIterator();
    int count = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      count++;
    }
    return count;
  }

  /**
   * Benchmarks range iteration over live documents for {@link SparseLiveDocs}.
   *
   * <p>Tests iteration over live documents in a specific range (from maxDoc/4 to maxDoc/2),
   * simulating range queries.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of live documents in range (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateLiveDocsRange(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    int rangeStart = maxDoc / 4;
    int rangeEnd = maxDoc / 2;
    DocIdSetIterator it = sparseLiveDocs.liveDocsIterator();
    int count = 0;
    int doc = it.advance(rangeStart);
    while (doc < rangeEnd) {
      count++;
      doc = it.nextDoc();
    }
    return count;
  }

  /**
   * Benchmarks range iteration over live documents for {@link DenseLiveDocs}.
   *
   * <p>Tests iteration over live documents in a specific range (from maxDoc/4 to maxDoc/2),
   * simulating range queries.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @return number of live documents in range (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateLiveDocsRange(final LiveDocsMetrics metrics) throws IOException {
    fillMetrics(metrics);

    int rangeStart = maxDoc / 4;
    int rangeEnd = maxDoc / 2;
    DocIdSetIterator it = denseLiveDocs.liveDocsIterator();
    int count = 0;
    int doc = it.advance(rangeStart);
    while (doc < rangeEnd) {
      count++;
      doc = it.nextDoc();
    }
    return count;
  }

  /**
   * Benchmarks {@link SparseLiveDocs#applyMask} over the segment, one window at a time.
   *
   * <p>Sparse holds the deleted documents in a {@link SparseFixedBitSet} and and-nots them into the
   * candidates word by word, so the cost of the {@code BULK} arm depends on how many non-zero words
   * the deleted documents span in the window, not on the candidate density. The {@code DEFAULT} arm
   * is the per-bit loop this replaces, whose cost is one {@code get} per candidate.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @param candidates the candidate documents to mask
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void sparseApplyMask(
      final LiveDocsMetrics metrics, final Candidates candidates, final Blackhole blackhole) {
    fillMetrics(metrics);

    applyMaskOverSegment(
        candidates,
        candidates.maskImpl == MaskImpl.BULK ? sparseLiveDocs : sparseDefaultBits,
        blackhole);
  }

  /**
   * Benchmarks {@link DenseLiveDocs#applyMask} over the segment, one window at a time.
   *
   * <p>Dense holds live documents in a {@link FixedBitSet}, so the {@code BULK} arm is a word-wise
   * AND whose cost depends on the window size alone, not on the deletion rate or the candidate
   * density. The {@code DEFAULT} arm is the per-bit loop this replaces, whose cost is one {@code
   * get} per candidate.
   *
   * @param metrics JMH auxiliary counters for memory statistics
   * @param candidates the candidate documents to mask
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void denseApplyMask(
      final LiveDocsMetrics metrics, final Candidates candidates, final Blackhole blackhole) {
    fillMetrics(metrics);

    applyMaskOverSegment(
        candidates,
        candidates.maskImpl == MaskImpl.BULK ? denseLiveDocs : denseDefaultBits,
        blackhole);
  }

  /**
   * Refills a window of candidate documents and masks it, for every whole window of the segment.
   * The reported time is for the whole sweep; divide by {@code maxDoc / 4096} for a per-window
   * cost.
   */
  private static void applyMaskOverSegment(
      final Candidates candidates, final Bits liveDocs, final Blackhole blackhole) {
    FixedBitSet window = candidates.window;
    for (int i = 0; i < candidates.numWindows; i++) {
      candidates.fillWindow(i * WINDOW);
      liveDocs.applyMask(window, i * WINDOW);
      blackhole.consume(window.getBits());
    }
  }

  /**
   * Wraps a {@link Bits} instance so that {@link Bits#applyMask} resolves to the default
   * implementation, which is the baseline the bulk overrides are measured against.
   */
  private static Bits defaultApplyMask(final Bits in) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return in.get(index);
      }

      @Override
      public int length() {
        return in.length();
      }
    };
  }

  private void fillMetrics(final LiveDocsMetrics metrics) {
    metrics.deleted = deleted;
    metrics.sparseBytes = sparseBytes;
    metrics.denseBytes = denseBytes;
    metrics.overheadPct = overheadPct;
  }
}
