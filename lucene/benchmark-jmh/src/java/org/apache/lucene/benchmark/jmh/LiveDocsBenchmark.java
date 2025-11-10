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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DenseLiveDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.SparseLiveDocs;
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
 * <p>This benchmark suite measures four key operations to evaluate the trade-offs between sparse
 * and dense LiveDocs implementations:
 *
 * <ul>
 *   <li><b>Random access (get)</b> - O(1) for both, but sparse has additional indirection overhead
 *   <li><b>Deleted docs iteration</b> - O(deletedDocs) for sparse, O(maxDoc) for dense
 *   <li><b>Live docs iteration (full)</b> - O(maxDoc) for both, tests get() performance at scale
 *   <li><b>Live docs iteration (range)</b> - O(range) for both, tests advance() and get() on subset
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
 * </ul>
 *
 * <h2>Memory Reporting</h2>
 *
 * <p>Memory usage for each configuration is printed during {@link #setup()} in the format:
 *
 * <pre>
 * [Memory] pattern=CLUSTERED, maxDoc=10,000,000, delRate=1.0%, deleted=100,000:
 *   Sparse=42,376 bytes, Dense=1,250,040 bytes, Overhead=-96.6%
 * </pre>
 *
 * Negative overhead indicates memory savings (sparse uses less memory than dense).
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

  /** Pre-generated random document IDs for random access benchmarks. */
  private int[] randomDocIds;

  /** Number of random accesses to perform in each benchmark iteration. */
  private static final int RANDOM_ACCESS_SIZE = 10000;

  /**
   * Sets up the benchmark by creating both sparse and dense LiveDocs with identical deletion
   * patterns. Prints memory usage statistics for analysis.
   *
   * <p>This method is called once per trial (combination of parameters) before any benchmark
   * iterations run.
   */
  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    int numDeleted = (int) (maxDoc * deletionRate);

    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);

    switch (deletionPattern) {
      case "RANDOM":
        java.util.Set<Integer> deleted = new java.util.HashSet<>();
        while (deleted.size() < numDeleted) {
          deleted.add(random.nextInt(maxDoc));
        }
        for (int docId : deleted) {
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

    sparseLiveDocs = new SparseLiveDocs(sparseSet, maxDoc);
    denseLiveDocs = new DenseLiveDocs(fixedSet, maxDoc);

    randomDocIds = new int[RANDOM_ACCESS_SIZE];
    for (int i = 0; i < RANDOM_ACCESS_SIZE; i++) {
      randomDocIds[i] = random.nextInt(maxDoc);
    }

    long sparseBytes = sparseLiveDocs.ramBytesUsed();
    long denseBytes = denseLiveDocs.ramBytesUsed();
    double overheadPct = ((double) sparseBytes / denseBytes - 1.0) * 100;
    System.out.printf(
        java.util.Locale.ROOT,
        "[Memory] pattern=%s, maxDoc=%,d, delRate=%.1f%%, deleted=%,d: Sparse=%,d bytes, Dense=%,d bytes, Overhead=%.1f%%%n",
        deletionPattern,
        maxDoc,
        deletionRate * 100,
        numDeleted,
        sparseBytes,
        denseBytes,
        overheadPct);
  }

  /**
   * Benchmarks random access (get) performance for {@link SparseLiveDocs}.
   *
   * <p>Tests 10,000 random get() operations on pre-generated random document IDs. Sparse
   * implementation has additional indirection overhead (block lookup + word lookup) compared to
   * dense.
   *
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void sparseRandomAccess(Blackhole blackhole) {
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
   * @param blackhole JMH blackhole to prevent dead code elimination
   */
  @Benchmark
  public void denseRandomAccess(Blackhole blackhole) {
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
   * @return number of deleted documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateDeleted() throws IOException {
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
   * @return number of deleted documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateDeleted() throws IOException {
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
   * @return number of live documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateLiveDocs() throws IOException {
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
   * @return number of live documents (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateLiveDocs() throws IOException {
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
   * @return number of live documents in range (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int sparseIterateLiveDocsRange() throws IOException {
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
   * @return number of live documents in range (for verification)
   * @throws IOException if iteration fails
   */
  @Benchmark
  public int denseIterateLiveDocsRange() throws IOException {
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
}
