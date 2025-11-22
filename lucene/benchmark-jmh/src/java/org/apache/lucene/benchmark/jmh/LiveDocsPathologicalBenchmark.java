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

/**
 * Benchmarks the worst-case (pathological) deletion pattern for {@link SparseLiveDocs}.
 *
 * <p>This benchmark tests the adversarial case that maximizes SparseFixedBitSet memory usage while
 * maintaining a low deletion rate. The pattern is designed to force maximum memory allocation in
 * the sparse structure.
 *
 * <h2>Pathological Deletion Pattern</h2>
 *
 * <p>The benchmark deletes exactly 64 documents per 4096-bit block, with one deletion per 64-bit
 * long, spread across all blocks. This pattern:
 *
 * <ul>
 *   <li>Forces allocation of ALL blocks (because each block has at least one deletion)
 *   <li>Forces allocation of ALL 64 longs within each block (one deletion per long)
 *   <li>Results in ~1.56% deletion rate (64 deleted per 4096 docs per block)
 *   <li>Maximizes memory overhead while keeping deletion rate low
 * </ul>
 *
 * <h2>Why This Pattern Is Pathological</h2>
 *
 * <p>SparseFixedBitSet uses a two-level sparse structure:
 *
 * <pre>
 * Level 1: 4096-bit blocks (only allocated if block has deletions)
 * Level 2: Within each block, 64 longs (only allocated if long has deletions)
 * </pre>
 *
 * <p>This pattern defeats both levels of sparsity:
 *
 * <ul>
 *   <li>All blocks must be allocated (defeats level 1 sparsity)
 *   <li>All longs within blocks must be allocated (defeats level 2 sparsity)
 *   <li>Results in maximum possible memory overhead for ~1.56% deletion rate
 *   <li>May actually use MORE memory than dense representation
 * </ul>
 *
 * <h2>Real-World Relevance</h2>
 *
 * <p>While highly unlikely in practice, this pattern could theoretically occur with:
 *
 * <ul>
 *   <li>Adversarial deletion patterns designed to defeat sparse structures
 *   <li>Certain systematic deletion patterns based on document ID modulo operations
 *   <li>Worst-case hash-based deletion patterns
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Run the pathological benchmark:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsPathologicalBenchmark"
 * </pre>
 *
 * <p>Test specific segment size:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsPathologicalBenchmark" -p maxDoc=10000000
 * </pre>
 *
 * @see SparseLiveDocs
 * @see DenseLiveDocs
 * @see LiveDocsBenchmark
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class LiveDocsPathologicalBenchmark {

  /**
   * Number of documents in the segment. Tests across a wide range of segment sizes to evaluate how
   * the pathological pattern scales from small (100K) to very large (100M) segments.
   */
  @Param({"100000", "500000", "1000000", "5000000", "10000000", "50000000", "100000000"})
  int maxDoc;

  /** Sparse LiveDocs implementation under test with pathological deletion pattern. */
  private SparseLiveDocs sparseLiveDocs;

  /** Dense LiveDocs implementation for comparison (unaffected by deletion patterns). */
  private DenseLiveDocs denseLiveDocs;

  /**
   * Sets up the benchmark by creating both sparse and dense LiveDocs with the pathological deletion
   * pattern.
   *
   * <p>Applies worst-case deletion pattern: deletes 64 documents per 4096-bit block (one deletion
   * per 64-bit long), spread across all blocks. This maximizes memory allocation in the sparse
   * structure while maintaining ~1.56% deletion rate.
   */
  @Setup(Level.Trial)
  public void setup() {
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc); // All docs initially live

    // Apply worst-case deletion pattern:
    // Delete 64 docs per block (one per long), spread across all blocks
    // This maximizes both number of blocks AND number of longs per block
    // SparseFixedBitSet has 4096-bit blocks with 64 longs per block
    final int blockSize = 4096;
    final int longsPerBlock = 64;
    final int bitsPerLong = 64;

    for (int blockIdx = 0; blockIdx * blockSize < maxDoc; blockIdx++) {
      // Within each block, delete one document per long (spread across all 64 longs)
      for (int longIdx = 0; longIdx < longsPerBlock; longIdx++) {
        int docId = blockIdx * blockSize + longIdx * bitsPerLong;
        if (docId < maxDoc) {
          sparseSet.set(docId);
          fixedSet.clear(docId);
        }
      }
    }

    sparseLiveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    denseLiveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();
  }

  /**
   * Benchmarks iteration over deleted documents for {@link SparseLiveDocs} with pathological
   * pattern.
   *
   * <p>With the pathological deletion pattern (~1.56% deletion rate), sparse is forced to allocate
   * all blocks and all longs within blocks, defeating both levels of sparsity. This results in:
   *
   * <ul>
   *   <li>Maximum memory overhead (may exceed dense representation)
   *   <li>Additional indirection overhead during iteration
   *   <li>No benefit from sparse structure
   * </ul>
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
   * Benchmarks iteration over deleted documents for {@link DenseLiveDocs} with pathological
   * pattern.
   *
   * <p>Dense implementation is unaffected by deletion patterns. Provides consistent baseline
   * performance for comparison.
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
}
