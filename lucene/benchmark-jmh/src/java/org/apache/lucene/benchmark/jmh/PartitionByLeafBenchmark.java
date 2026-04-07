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
 * Benchmark strategies for ReaderUtil#partitionByLeaf:
 *
 * <ul>
 *   <li>arraysSortOnly: Arrays#sort on int[] then linear-scan partition
 *   <li>introSortWithOrdinals: IntroSorter sorting docIDs + ordinals as parallel arrays then
 *       linear-scan partition
 *   <li>arraysSortBinarySearchPartition: Arrays#sort then binary-search partition
 *   <li>arraysSortHybridPartition: Arrays#sort then picks linear or binary-search based on
 *       numLeaves vs numDocIds
 *   <li>arraysSortBranchlessBinarySearchPartition: Arrays#sort then branchless binary-search
 *       partition (cmov pattern)
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

  /** Number of doc IDs we'll be sorting/partitioning */
  @Param({"100", "1000", "10000", "100000"})
  int numDocIds;

  /** Number of leaves in the test index. */
  @Param({"5", "10", "20", "50", "200"})
  int numLeaves;

  /** Doc IDs to partition (shuffled before each test). */
  private int[] docIds;

  /** Leaf boundaries: leafDocBase[i] is the docBase for leaf i. */
  private int[] leafDocBase;

  /** Max doc per leaf (uniform for simplicity). */
  private int docsPerLeaf;

  /** Main copy of doc IDs to copy from each invocation. */
  private int[] mainDocIds;

  /** Pre-sorted doc IDs for partition-only benchmarks. */
  private int[] mainDocIdsSorted;

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
    mainDocIds = ArrayUtil.copyOfSubArray(pool, 0, numDocIds);

    // Pre-sorted copy for partition-only benchmarks
    mainDocIdsSorted = ArrayUtil.copyArray(mainDocIds);
    Arrays.sort(mainDocIdsSorted);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // Fresh unsorted copy each invocation since sort is in-place
    docIds = ArrayUtil.copyArray(mainDocIds);
  }

  @Benchmark
  public void arraysSortOnly(Blackhole bh) {
    int[] sorted = docIds;
    Arrays.sort(sorted);
    int[][] result = partitionSorted(sorted);
    bh.consume(result);
  }

  @Benchmark
  public void introSortWithOrdinals(Blackhole bh) {
    int[] sorted = docIds;
    int[] ordinals = new int[sorted.length];
    for (int i = 0; i < ordinals.length; i++) {
      ordinals[i] = i;
    }

    new IntroSorter() {
      int pivot;

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(sorted[i], sorted[j]);
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = sorted[i];
        sorted[i] = sorted[j];
        sorted[j] = tmp;
        tmp = ordinals[i];
        ordinals[i] = ordinals[j];
        ordinals[j] = tmp;
      }

      @Override
      protected void setPivot(int i) {
        pivot = sorted[i];
      }

      @Override
      protected int comparePivot(int j) {
        return Integer.compare(pivot, sorted[j]);
      }
    }.sort(0, sorted.length);

    int[][] result = partitionSorted(sorted);
    bh.consume(result);
    bh.consume(ordinals);
  }

  @Benchmark
  public void arraysSortBinarySearchPartition(Blackhole bh) {
    int[] sorted = docIds;
    Arrays.sort(sorted);
    int[][] result = partitionSortedBinarySearch(sorted);
    bh.consume(result);
  }

  @Benchmark
  public void arraysSortHybridPartition(Blackhole bh) {
    int[] sorted = docIds;
    Arrays.sort(sorted);
    int[][] result;
    if (numLeaves <= sorted.length) {
      result = partitionSortedBinarySearch(sorted);
    } else {
      result = partitionSorted(sorted);
    }
    bh.consume(result);
  }

  @Benchmark
  public void arraysSortBranchlessBinarySearchPartition(Blackhole bh) {
    int[] sorted = docIds;
    Arrays.sort(sorted);
    int[][] result = partitionSortedBranchlessBinarySearch(sorted);
    bh.consume(result);
  }

  @Benchmark
  public void partitionOnlyBinarySearch(Blackhole bh) {
    int[][] result = partitionSortedBinarySearch(mainDocIdsSorted);
    bh.consume(result);
  }

  @Benchmark
  public void partitionOnlyBranchlessBinarySearch(Blackhole bh) {
    int[][] result = partitionSortedBranchlessBinarySearch(mainDocIdsSorted);
    bh.consume(result);
  }

  /** Partition sorted doc IDs across leaves. Mirrors the logic in ReaderUtil#partitionByLeaf. */
  private int[][] partitionSorted(int[] sortedDocIds) {
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
   * Partition sorted doc IDs across leaves using binary search on leaf boundaries. For each leaf,
   * binary search for its end boundary in the sorted doc IDs to find the slice belonging to that
   * leaf. Each successive search is bounded by the previous result.
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

  /**
   * Partition sorted doc IDs across leaves using branchless binary search on leaf boundaries. Same
   * structure as the regular binary search partition, but uses a branchless search that compiles to
   * cmov instructions, avoiding branch misprediction penalties.
   */
  private int[][] partitionSortedBranchlessBinarySearch(int[] sortedDocIds) {
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
      int to = branchlessBinarySearch(sortedDocIds, from, sortedDocIds.length, leafEnd);
      int count = to - from;
      result[leafIdx] = new int[count];
      System.arraycopy(sortedDocIds, from, result[leafIdx], 0, count);
      from = to;
    }
    Arrays.fill(result, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return result;
  }

  /**
   * Branchless binary search returning the index of the first element >= target. Each iteration
   * uses a conditional move (cmov) pattern instead of if/else branching.
   */
  private static int branchlessBinarySearch(int[] a, int from, int to, int target) {
    int lo = from;
    int len = to - from;
    while (len > 1) {
      int half = len >>> 1;
      // This pattern compiles to cmov — no branch, no misprediction
      lo = a[lo + half] < target ? lo + half : lo;
      len = len - half;
    }
    return a[lo] < target ? lo + 1 : lo;
  }
}
