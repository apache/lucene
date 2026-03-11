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
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.TimSorter;
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
 * Benchmark comparing different sort implementations for sorting {@link ScoreDoc}[] by ascending
 * doc ID. Simulates realistic ScoreDoc arrays with random doc IDs drawn from a 5M-doc index and
 * random scores.
 *
 * <h2>Running</h2>
 *
 * Use {@code run-benchmark.sh} which automatically recompiles if sources changed, then runs JMH:
 *
 * <pre>{@code
 * ./lucene/benchmark-jmh/run-benchmark.sh ScoreDocSortBenchmark \
 *   -rf json -rff results.json
 * }</pre>
 *
 * <p>Or build and run manually:
 *
 * <pre>{@code
 * ./gradlew :lucene:benchmark-jmh:assemble
 * java --module-path lucene/benchmark-jmh/build/benchmarks \
 *   --module org.apache.lucene.benchmark.jmh \
 *   ScoreDocSortBenchmark \
 *   -rf json -rff results.json
 * }</pre>
 *
 * <h2>Visualizing results</h2>
 *
 * The companion {@code jmh-table.py} script (in the same directory as this source file) converts
 * JMH JSON output into an interactive HTML report:
 *
 * <pre>{@code
 * python3 lucene/benchmark-jmh/jmh-table.py \
 *   lucene/benchmark-jmh/src/java/org/apache/lucene/benchmark/jmh/ScoreDocSortBenchmark.java \
 *   < results.json > results.html
 * }</pre>
 *
 * <p>The HTML report provides:
 *
 * <ul>
 *   <li>A heatmap table with algorithms as rows and array sizes as columns. Green cells are the
 *       fastest, red cells are the slowest within each column.
 *   <li>Inline sparkline histograms in each cell showing the distribution of raw iteration samples,
 *       making outliers immediately visible.
 *   <li>Click any column header to sort the table by that column (click again to reverse).
 *   <li>Click any data cell to show a full histogram below the table with detailed statistics
 *       (mean, median, stddev, p5/p95, range) and the benchmark method source code to the right.
 *   <li>Clicking a cell updates the URL hash (e.g. {@code #introSorterAnonymous|1000}) so you can
 *       share a direct link to a specific result.
 *   <li>A configuration banner at the top showing JMH settings (mode, forks, threads, warmup,
 *       measurement iterations, JVM args).
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 10,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class ScoreDocSortBenchmark {

  private static final Comparator<ScoreDoc> BY_DOC_ASC = (a, b) -> Integer.compare(a.doc, b.doc);

  private static final int MAX_DOC = 5_000_000;

  @Param({"10", "50", "100", "500", "1000", "10000"})
  int size;

  // add "nearly_sorted", "reversed" to test other distributions
  @Param({"random"})
  String distribution;

  /** Template array; copied before each invocation so every sort sees the same random order. */
  private ScoreDoc[] template;

  /** Working copy that each benchmark method sorts in place. */
  private ScoreDoc[] work;

  @Setup(Level.Trial)
  public void setupTrial() {
    SplittableRandom rng = new SplittableRandom(0xCAFEBABE);
    template = new ScoreDoc[size];
    for (int i = 0; i < size; i++) {
      int doc = rng.nextInt(MAX_DOC);
      float score = (float) rng.nextDouble(0.0, 10.0);
      template[i] = new ScoreDoc(doc, score);
    }

    if (distribution.equals("nearly_sorted")) {
      Arrays.sort(template, BY_DOC_ASC);
      // swap ~5% of adjacent pairs to introduce mild disorder
      int numSwaps = (int) (size * 0.05);
      for (int i = 0; i < numSwaps; i++) {
        int idx = rng.nextInt(size - 1);
        ScoreDoc tmp = template[idx];
        template[idx] = template[idx + 1];
        template[idx + 1] = tmp;
      }
    } else if (distribution.equals("reversed")) {
      Arrays.sort(template, BY_DOC_ASC);
      for (int i = 0; i < size / 2; i++) {
        ScoreDoc tmp = template[i];
        template[i] = template[size - 1 - i];
        template[size - 1 - i] = tmp;
      }
    }

    // verification - runs once up front per trial (per parameter set)
    ScoreDoc[] reference = Arrays.copyOf(template, size);
    Arrays.sort(reference, BY_DOC_ASC);

    verify("jdkSortLambda", reference, runJdkSortLambda(Arrays.copyOf(template, size)));
    verify("jdkSortComparator", reference, runJdkSortComparator(Arrays.copyOf(template, size)));
    verify("arrayUtilIntroSort", reference, runArrayUtilIntroSort(Arrays.copyOf(template, size)));
    verify("arrayUtilTimSort", reference, runArrayUtilTimSort(Arrays.copyOf(template, size)));
    verify(
        "introSorterAnonymous", reference, runIntroSorterAnonymous(Arrays.copyOf(template, size)));
    verify("timSorterAnonymous", reference, runTimSorterAnonymous(Arrays.copyOf(template, size)));
    verify(
        "inPlaceMergeSorterAnonymous",
        reference,
        runInPlaceMergeSorterAnonymous(Arrays.copyOf(template, size)));
    verify("jdkParallelSort", reference, runJdkParallelSort(Arrays.copyOf(template, size)));
    verify(
        "jdkSortPrimitiveExtractLong",
        reference,
        runJdkSortPrimitiveExtractLong(Arrays.copyOf(template, size)));
    verify(
        "jdkSortPrimitiveExtractAdaptive",
        reference,
        runJdkSortPrimitiveExtractAdaptive(Arrays.copyOf(template, size)));
    verify("lsbRadixSortExtract", reference, runLsbRadixSortExtract(Arrays.copyOf(template, size)));
    verify("radixSort2Pass", reference, runRadixSort2Pass(Arrays.copyOf(template, size)));
  }

  private void verify(String name, ScoreDoc[] reference, ScoreDoc[] result) {
    if (result.length != reference.length) {
      throw new IllegalStateException(
          name
              + " failed: length mismatch. expected "
              + reference.length
              + " but got "
              + result.length);
    }
    for (int i = 0; i < result.length; i++) {
      if (i > 0 && result[i].doc < result[i - 1].doc) {
        throw new IllegalStateException(
            name
                + " failed: not sorted at index "
                + i
                + ". "
                + result[i - 1].doc
                + " > "
                + result[i].doc);
      }
      // check if doc matches reference (handles duplicates correctly since both are doc-sorted)
      if (result[i].doc != reference[i].doc) {
        throw new IllegalStateException(
            name
                + " failed: doc mismatch at index "
                + i
                + ". expected "
                + reference[i].doc
                + " but got "
                + result[i].doc);
      }
    }
    // integrity check: ensure we didn't lose or duplicate objects
    IdentityHashMap<ScoreDoc, Integer> counts = new IdentityHashMap<>();
    for (ScoreDoc sd : template) {
      counts.merge(sd, 1, Integer::sum);
    }
    for (ScoreDoc sd : result) {
      Integer c = counts.get(sd);
      if (c == null) {
        throw new IllegalStateException(
            name + " failed: result contains unknown ScoreDoc instance");
      }
      if (c == 1) {
        counts.remove(sd);
      } else {
        counts.put(sd, c - 1);
      }
    }
    if (counts.isEmpty() == false) {
      throw new IllegalStateException(name + " failed: result missing ScoreDoc instances");
    }
  }

  /**
   * setupInvocation performs a shallow copy of the template.
   *
   * <p>Note: using Level.Invocation introduces overhead that JMH cannot easily subtract. For very
   * small sizes (e.g. size=10), this overhead might be comparable to the benchmarked sort itself.
   * We accept this because each invocation must start with the same unsorted array to ensure
   * reproducibility across different sorting algorithms.
   */
  @Setup(Level.Invocation)
  public void setupInvocation() {
    work = new ScoreDoc[size];
    System.arraycopy(template, 0, work, 0, size);
  }

  // ---- 1. JDK Arrays.sort with lambda ----

  private ScoreDoc[] runJdkSortLambda(ScoreDoc[] work) {
    Arrays.sort(work, (a, b) -> Integer.compare(a.doc, b.doc));
    return work;
  }

  @Benchmark
  public void jdkSortLambda(Blackhole bh) {
    // intentionally inline — tests whether JIT handles inline lambda differently than static
    // comparator
    bh.consume(runJdkSortLambda(work));
  }

  // ---- 2. JDK Arrays.sort with static comparator ----

  private ScoreDoc[] runJdkSortComparator(ScoreDoc[] work) {
    Arrays.sort(work, BY_DOC_ASC);
    return work;
  }

  @Benchmark
  public void jdkSortComparator(Blackhole bh) {
    bh.consume(runJdkSortComparator(work));
  }

  // ---- 3. ArrayUtil.introSort (wraps ArrayIntroSorter) ----

  private ScoreDoc[] runArrayUtilIntroSort(ScoreDoc[] work) {
    ArrayUtil.introSort(work, BY_DOC_ASC);
    return work;
  }

  @Benchmark
  public void arrayUtilIntroSort(Blackhole bh) {
    bh.consume(runArrayUtilIntroSort(work));
  }

  // ---- 4. ArrayUtil.timSort (wraps ArrayTimSorter) ----

  private ScoreDoc[] runArrayUtilTimSort(ScoreDoc[] work) {
    ArrayUtil.timSort(work, BY_DOC_ASC);
    return work;
  }

  @Benchmark
  public void arrayUtilTimSort(Blackhole bh) {
    bh.consume(runArrayUtilTimSort(work));
  }

  // ---- 5. Anonymous IntroSorter ----

  private ScoreDoc[] runIntroSorterAnonymous(ScoreDoc[] work) {
    final ScoreDoc[] arr = work;
    new IntroSorter() {
      ScoreDoc pivot;

      @Override
      protected void swap(int i, int j) {
        ScoreDoc tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
      }

      @Override
      protected void setPivot(int i) {
        pivot = arr[i];
      }

      @Override
      protected int comparePivot(int j) {
        return Integer.compare(pivot.doc, arr[j].doc);
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(arr[i].doc, arr[j].doc);
      }
    }.sort(0, arr.length);
    return arr;
  }

  @Benchmark
  public void introSorterAnonymous(Blackhole bh) {
    bh.consume(runIntroSorterAnonymous(work));
  }

  // ---- 6. Anonymous TimSorter ----

  private ScoreDoc[] runTimSorterAnonymous(ScoreDoc[] work) {
    final ScoreDoc[] arr = work;
    final int len = arr.length;
    new TimSorter(len / 2) {
      ScoreDoc[] tmp = new ScoreDoc[len / 2];

      @Override
      protected void swap(int i, int j) {
        ScoreDoc t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(arr[i].doc, arr[j].doc);
      }

      @Override
      protected void copy(int src, int dest) {
        arr[dest] = arr[src];
      }

      @Override
      protected void save(int start, int l) {
        System.arraycopy(arr, start, tmp, 0, l);
      }

      @Override
      protected void restore(int src, int dest) {
        arr[dest] = tmp[src];
      }

      @Override
      protected int compareSaved(int i, int j) {
        return Integer.compare(tmp[i].doc, arr[j].doc);
      }
    }.sort(0, len);
    return arr;
  }

  @Benchmark
  public void timSorterAnonymous(Blackhole bh) {
    bh.consume(runTimSorterAnonymous(work));
  }

  // ---- 7. Anonymous InPlaceMergeSorter ----

  private ScoreDoc[] runInPlaceMergeSorterAnonymous(ScoreDoc[] work) {
    final ScoreDoc[] arr = work;
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        ScoreDoc tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(arr[i].doc, arr[j].doc);
      }
    }.sort(0, arr.length);
    return arr;
  }

  @Benchmark
  public void inPlaceMergeSorterAnonymous(Blackhole bh) {
    bh.consume(runInPlaceMergeSorterAnonymous(work));
  }

  // ---- 8. JDK Arrays.parallelSort with static comparator ----

  private ScoreDoc[] runJdkParallelSort(ScoreDoc[] work) {
    Arrays.parallelSort(work, BY_DOC_ASC);
    return work;
  }

  @Benchmark
  public void jdkParallelSort(Blackhole bh) {
    bh.consume(runJdkParallelSort(work));
  }

  // ---- 9. Extract doc IDs, sort with JDK Arrays.sort (primitive long[]), reorder ----

  private ScoreDoc[] runJdkSortPrimitiveExtractLong(ScoreDoc[] work) {
    int len = work.length;
    // pack (doc, originalIndex) into a long: doc in upper 32, index in lower 32
    long[] packed = new long[len];
    for (int i = 0; i < len; i++) {
      packed[i] = ((long) work[i].doc << 32) | (i & 0xFFFFFFFFL);
    }
    Arrays.sort(packed);
    ScoreDoc[] sorted = new ScoreDoc[len];
    for (int i = 0; i < len; i++) {
      sorted[i] = work[(int) packed[i]];
    }
    return sorted;
  }

  @Benchmark
  public void jdkSortPrimitiveExtractLong(Blackhole bh) {
    bh.consume(runJdkSortPrimitiveExtractLong(work));
  }

  // ---- 10. Extract doc IDs, sort with int[] when bits fit, else long[] ----

  // bits needed to represent values in [0, max)
  private static int bitsNeeded(int max) {
    return 32 - Integer.numberOfLeadingZeros(max - 1);
  }

  private ScoreDoc[] runJdkSortPrimitiveExtractAdaptive(ScoreDoc[] work) {
    int len = work.length;
    int docBits = bitsNeeded(MAX_DOC);
    int indexBits = bitsNeeded(len);
    if (docBits + indexBits <= 31) {
      // pack into int[]: doc in upper bits, index in lower bits
      // <= 31 (not 32) because Arrays.sort uses signed comparison,
      // so bit 31 must stay clear to avoid sign-bit corruption
      int[] packed = new int[len];
      for (int i = 0; i < len; i++) {
        packed[i] = (work[i].doc << indexBits) | i;
      }
      Arrays.sort(packed);
      int indexMask = (1 << indexBits) - 1;
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[packed[i] & indexMask];
      }
      return sorted;
    } else {
      // fall back to long[]
      long[] packed = new long[len];
      for (int i = 0; i < len; i++) {
        packed[i] = ((long) work[i].doc << 32) | (i & 0xFFFFFFFFL);
      }
      Arrays.sort(packed);
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[(int) packed[i]];
      }
      return sorted;
    }
  }

  @Benchmark
  public void jdkSortPrimitiveExtractAdaptive(Blackhole bh) {
    /**
     * Documentation of int vs long paths given MAX_DOC = 5,000,000:
     *
     * <ul>
     *   <li>sizes 10, 50, 100 take the int[] path (23 + 7 = 30 <= 31 bits)
     *   <li>sizes 500, 1,000, 10,000 take the long[] path (23 + 9 = 32 > 31 bits)
     * </ul>
     */
    bh.consume(runJdkSortPrimitiveExtractAdaptive(work));
  }

  // ---- 11. Extract doc IDs, sort with LSBRadixSorter when bits fit, else JDK long[] ----

  private ScoreDoc[] runLsbRadixSortExtract(ScoreDoc[] work) {
    int len = work.length;
    int docBits = bitsNeeded(MAX_DOC);
    int indexBits = bitsNeeded(len);
    if (docBits + indexBits <= 32) {
      int[] packed = new int[len];
      for (int i = 0; i < len; i++) {
        packed[i] = (work[i].doc << indexBits) | i;
      }
      new LSBRadixSorter().sort(docBits + indexBits, packed, len);
      int indexMask = (1 << indexBits) - 1;
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[packed[i] & indexMask];
      }
      return sorted;
    } else {
      // fallback to long[] + Arrays.sort
      long[] packed = new long[len];
      for (int i = 0; i < len; i++) {
        packed[i] = ((long) work[i].doc << 32) | (i & 0xFFFFFFFFL);
      }
      Arrays.sort(packed);
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[(int) packed[i]];
      }
      return sorted;
    }
  }

  @Benchmark
  public void lsbRadixSortExtract(Blackhole bh) {
    bh.consume(runLsbRadixSortExtract(work));
  }

  // ---- 12. Extract doc IDs, manual 4-pass radix sort (8-bit) ----

  private ScoreDoc[] runRadixSort2Pass(ScoreDoc[] work) {
    int len = work.length;
    int docBits = bitsNeeded(MAX_DOC);
    int indexBits = bitsNeeded(len);
    if (docBits + indexBits <= 32) {
      int[] packed = new int[len];
      for (int i = 0; i < len; i++) {
        packed[i] = (work[i].doc << indexBits) | i;
      }

      int totalBits = docBits + indexBits;
      int[] bucket = new int[256];
      int[] workArray = new int[len];

      // up to 4 passes over 8-bit radix, skip unnecessary high passes
      int passes = (totalBits + 7) >>> 3; // ceil(totalBits / 8)
      for (int pass = 0; pass < passes; pass++) {
        int shift = pass * 8;
        int[] src = (pass % 2 == 0) ? packed : workArray;
        int[] dst = (pass % 2 == 0) ? workArray : packed;

        // histogram
        for (int i = 0; i < len; i++) {
          bucket[(src[i] >>> shift) & 0xFF]++;
        }
        // prefix sum
        for (int i = 1; i < 256; i++) {
          bucket[i] += bucket[i - 1];
        }
        // scatter
        for (int i = len - 1; i >= 0; i--) {
          dst[--bucket[(src[i] >>> shift) & 0xFF]] = src[i];
        }

        Arrays.fill(bucket, 0);
      }

      // if odd number of passes, result is in workArray
      int[] sorted_packed = (passes % 2 == 0) ? packed : workArray;

      int indexMask = (1 << indexBits) - 1;
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[sorted_packed[i] & indexMask];
      }
      return sorted;
    } else {
      // long fallback
      long[] packed = new long[len];
      for (int i = 0; i < len; i++) {
        packed[i] = ((long) work[i].doc << 32) | (i & 0xFFFFFFFFL);
      }
      Arrays.sort(packed);
      ScoreDoc[] sorted = new ScoreDoc[len];
      for (int i = 0; i < len; i++) {
        sorted[i] = work[(int) packed[i]];
      }
      return sorted;
    }
  }

  @Benchmark
  public void radixSort2Pass(Blackhole bh) {
    bh.consume(runRadixSort2Pass(work));
  }
}
