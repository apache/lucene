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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
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
 * Benchmark for partition strategies behind ReaderUtil#partitionByLeaf.
 *
 * <p>Two families of benchmarks live here:
 *
 * <ul>
 *   <li><b>Partition step only</b> (operate on pre-sorted doc IDs, isolating the partition from
 *       sorting overhead):
 *       <ul>
 *         <li>linearPartition: linear-scan partition
 *         <li>binarySearchPartition: binary-search partition using leaf boundaries
 *       </ul>
 *   <li><b>Full partition task</b> (start from unsorted input, so they include the sort): these
 *       compare the two candidate public-API shapes for a caller that only wants the per-leaf doc
 *       IDs and does <em>not</em> need input ordinals:
 *       <ul>
 *         <li>partitionByLeaf: clone + sort {@code int[]} + binary-search partition. No ordinal
 *             bookkeeping.
 *         <li>partitionByLeafWithOrdinals: pack {@code (docId, ordinal)} into a {@code long[]} +
 *             sort + binary-search partition, producing both per-leaf doc IDs and per-leaf
 *             ordinals. This mimics a caller that is "forced" through the ordinal-tracking API: it
 *             pays to build the ordinals array too, then uses only the per-leaf doc IDs. The delta
 *             against {@code partitionByLeaf} measures the overhead of standardizing on a single
 *             ordinal-tracking API.
 *         <li>partitionByLeafWithOrdinalsIntroSort: the same ordinal-tracking task, but sorting the
 *             doc IDs and ordinals as parallel {@code int[]}s with an {@link IntroSorter} instead
 *             of packing into a {@code long[]}. The delta against {@code
 *             partitionByLeafWithOrdinals} isolates the packed-long sort's win over the
 *             parallel-array sort.
 *       </ul>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class PartitionByLeafBenchmark {

  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Number of doc IDs we'll be partitioning. */
  @Param({"100", "1000", "10000", "100000"})
  int numDocIds;

  /** Number of leaves in the test index. */
  @Param({"5", "10", "20", "50", "200"})
  int numLeaves;

  /** Pre-sorted doc IDs to partition. */
  private int[] sortedDocIds;

  /**
   * Unsorted doc IDs (the shuffled pool prefix), used by the full-task benchmarks so they include
   * the sort. Never mutated by the benchmarks: each iteration clones/packs it before sorting.
   */
  private int[] unsortedDocIds;

  /** Leaf boundaries: leafDocBase[i] is the docBase for leaf i. */
  private int[] leafDocBase;

  /** Max doc per leaf (uniform for simplicity). */
  private int docsPerLeaf;

  @Setup(Level.Trial)
  public void setup() {
    Random r = new Random();

    docsPerLeaf = Math.max(numDocIds / numLeaves, 1) * 10;
    int totalDocs = numLeaves * docsPerLeaf;

    leafDocBase = new int[numLeaves];
    for (int i = 0; i < numLeaves; i++) {
      leafDocBase[i] = i * docsPerLeaf;
    }

    // Generate unique doc IDs via shuffle
    int[] pool = new int[totalDocs];
    for (int i = 0; i < totalDocs; i++) {
      pool[i] = i;
    }
    for (int i = totalDocs - 1; i > 0; i--) {
      int j = r.nextInt(i + 1);
      int tmp = pool[i];
      pool[i] = pool[j];
      pool[j] = tmp;
    }
    // Unsorted (shuffled) prefix drives the full-task benchmarks; the sorted copy drives the
    // partition-only benchmarks.
    unsortedDocIds = ArrayUtil.copyOfSubArray(pool, 0, numDocIds);
    sortedDocIds = unsortedDocIds.clone();
    Arrays.sort(sortedDocIds);
  }

  @Benchmark
  public void linearPartition(Blackhole bh) {
    bh.consume(partitionSortedLinear(sortedDocIds));
  }

  @Benchmark
  public void binarySearchPartition(Blackhole bh) {
    bh.consume(partitionSortedBinarySearch(sortedDocIds));
  }

  /**
   * Full task for a caller that only wants per-leaf doc IDs: clone + sort the {@code int[]}, then
   * binary-search partition.
   */
  @Benchmark
  public void partitionByLeaf(Blackhole bh) {
    int[] sorted = unsortedDocIds.clone();
    Arrays.sort(sorted);
    bh.consume(partitionSortedBinarySearch(sorted));
  }

  /**
   * Full task through the packed-long ordinal-tracking strategy for a caller that ultimately
   * ignores the ordinals: pack {@code (docId, ordinal)} into a {@code long[]}, sort, then
   * binary-search partition, producing both per-leaf doc IDs and per-leaf ordinals. This models a
   * caller that doesn't need ordinals but is forced through a single ordinal-tracking API: the
   * ordinals are built regardless of what the caller looks at. We consume the whole result (doc IDs
   * and ordinals) so that ordinal work can't be eliminated as dead code and the measurement stays
   * faithful to the work the ordinal-tracking strategy performs.
   */
  @Benchmark
  public void partitionByLeafWithOrdinals(Blackhole bh) {
    long[] packed = new long[unsortedDocIds.length];
    for (int i = 0; i < packed.length; i++) {
      packed[i] = ((long) unsortedDocIds[i] << 32) | i;
    }
    Arrays.sort(packed);
    bh.consume(partitionSortedPackedWithOrdinals(packed));
  }

  /**
   * The same ordinal-tracking full task as {@link #partitionByLeafWithOrdinals}, but sorting the
   * doc IDs and their ordinals as two parallel {@code int[]}s with an {@link IntroSorter} rather
   * than packing into a {@code long[]}. Kept alongside the packed-long variant so the two can be
   * compared directly. As with the packed variant, we consume both output arrays so the ordinal
   * work can't be eliminated as dead code.
   */
  @Benchmark
  public void partitionByLeafWithOrdinalsIntroSort(Blackhole bh) {
    final int[] sortedDocIds = unsortedDocIds.clone();
    final int[] sortedOrdinals = new int[unsortedDocIds.length];
    for (int i = 0; i < sortedOrdinals.length; i++) {
      sortedOrdinals[i] = i;
    }
    new IntroSorter() {
      int pivot;

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(sortedDocIds[i], sortedDocIds[j]);
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = tmp;
        tmp = sortedOrdinals[i];
        sortedOrdinals[i] = sortedOrdinals[j];
        sortedOrdinals[j] = tmp;
      }

      @Override
      protected void setPivot(int i) {
        pivot = sortedDocIds[i];
      }

      @Override
      protected int comparePivot(int j) {
        return Integer.compare(pivot, sortedDocIds[j]);
      }
    }.sort(0, sortedDocIds.length);
    bh.consume(partitionSortedParallelWithOrdinals(sortedDocIds, sortedOrdinals));
  }

  /** Partition sorted doc IDs across leaves using a linear scan. */
  private int[][] partitionSortedLinear(int[] sortedDocIds) {
    int[][] result = new int[numLeaves][];
    if (sortedDocIds.length == 0) {
      Arrays.fill(result, EMPTY_INT_ARRAY);
      return result;
    }
    int leafStart = 0;
    int leafIdx = 0;
    int leafEnd = leafDocBase[0] + docsPerLeaf;
    for (int i = 0; i < sortedDocIds.length; i++) {
      int docId = sortedDocIds[i];
      while (docId >= leafEnd) {
        int count = i - leafStart;
        if (count == 0) {
          result[leafIdx] = EMPTY_INT_ARRAY;
        } else {
          result[leafIdx] = new int[count];
          System.arraycopy(sortedDocIds, leafStart, result[leafIdx], 0, count);
        }
        leafStart = i;
        leafIdx++;
        leafEnd = leafDocBase[leafIdx] + docsPerLeaf;
      }
    }
    int count = sortedDocIds.length - leafStart;
    result[leafIdx] = new int[count];
    System.arraycopy(sortedDocIds, leafStart, result[leafIdx], 0, count);
    Arrays.fill(result, leafIdx + 1, numLeaves, EMPTY_INT_ARRAY);
    return result;
  }

  /**
   * Binary-search partition over a sorted array of packed {@code (docId << 32) | ordinal} entries,
   * unpacking into per-leaf doc IDs <em>and</em> per-leaf ordinals. Returns both arrays as {@code
   * {docIds, ordinals}} so neither the ordinal allocation nor its unpack loop can be eliminated as
   * dead code — that produced-but-unused work is precisely the overhead this benchmark measures.
   */
  private int[][][] partitionSortedPackedWithOrdinals(long[] sortedPacked) {
    int[][] docIdsByLeaf = new int[numLeaves][];
    int[][] ordinalsByLeaf = new int[numLeaves][];
    if (sortedPacked.length == 0) {
      Arrays.fill(docIdsByLeaf, EMPTY_INT_ARRAY);
      Arrays.fill(ordinalsByLeaf, EMPTY_INT_ARRAY);
      return new int[][][] {docIdsByLeaf, ordinalsByLeaf};
    }
    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < sortedPacked.length; leafIdx++) {
      int leafEnd = leafDocBase[leafIdx] + docsPerLeaf;
      long leafEndPacked = ((long) leafEnd) << 32;
      if (sortedPacked[from] >= leafEndPacked) {
        docIdsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        ordinalsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(sortedPacked, from, sortedPacked.length, leafEndPacked);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      int[] leafDocs = new int[count];
      int[] leafOrds = new int[count];
      for (int i = 0; i < count; i++) {
        long packed = sortedPacked[from + i];
        leafDocs[i] = (int) (packed >>> 32);
        leafOrds[i] = (int) packed;
      }
      docIdsByLeaf[leafIdx] = leafDocs;
      ordinalsByLeaf[leafIdx] = leafOrds;
      from = to;
    }
    Arrays.fill(docIdsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    Arrays.fill(ordinalsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return new int[][][] {docIdsByLeaf, ordinalsByLeaf};
  }

  /**
   * Binary-search partition over parallel sorted doc IDs and their ordinals, slicing each per-leaf
   * run out with {@link System#arraycopy}. Returns both arrays as {@code {docIds, ordinals}} so
   * neither the ordinal allocation nor its copy can be eliminated as dead code — that
   * produced-but-unused work is precisely the overhead this benchmark measures.
   */
  private int[][][] partitionSortedParallelWithOrdinals(int[] sortedDocIds, int[] sortedOrdinals) {
    int[][] docIdsByLeaf = new int[numLeaves][];
    int[][] ordinalsByLeaf = new int[numLeaves][];
    if (sortedDocIds.length == 0) {
      Arrays.fill(docIdsByLeaf, EMPTY_INT_ARRAY);
      Arrays.fill(ordinalsByLeaf, EMPTY_INT_ARRAY);
      return new int[][][] {docIdsByLeaf, ordinalsByLeaf};
    }
    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < sortedDocIds.length; leafIdx++) {
      int leafEnd = leafDocBase[leafIdx] + docsPerLeaf;
      if (sortedDocIds[from] >= leafEnd) {
        docIdsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        ordinalsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(sortedDocIds, from, sortedDocIds.length, leafEnd);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      int[] leafDocs = new int[count];
      int[] leafOrds = new int[count];
      System.arraycopy(sortedDocIds, from, leafDocs, 0, count);
      System.arraycopy(sortedOrdinals, from, leafOrds, 0, count);
      docIdsByLeaf[leafIdx] = leafDocs;
      ordinalsByLeaf[leafIdx] = leafOrds;
      from = to;
    }
    Arrays.fill(docIdsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    Arrays.fill(ordinalsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return new int[][][] {docIdsByLeaf, ordinalsByLeaf};
  }

  /**
   * Partition sorted doc IDs across leaves using binary search on leaf boundaries. For each leaf,
   * binary search for its end boundary in the sorted doc IDs to find the slice belonging to that
   * leaf. Each successive search is bounded by the previous result. Includes an O(1) peek to skip
   * empty leaves and early termination when all docs are placed.
   */
  private int[][] partitionSortedBinarySearch(int[] sortedDocIds) {
    int[][] result = new int[numLeaves][];
    if (sortedDocIds.length == 0) {
      Arrays.fill(result, EMPTY_INT_ARRAY);
      return result;
    }
    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < sortedDocIds.length; leafIdx++) {
      int leafEnd = leafDocBase[leafIdx] + docsPerLeaf;
      if (sortedDocIds[from] >= leafEnd) {
        result[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(sortedDocIds, from, sortedDocIds.length, leafEnd);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      result[leafIdx] = new int[count];
      System.arraycopy(sortedDocIds, from, result[leafIdx], 0, count);
      from = to;
    }
    Arrays.fill(result, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return result;
  }
}
