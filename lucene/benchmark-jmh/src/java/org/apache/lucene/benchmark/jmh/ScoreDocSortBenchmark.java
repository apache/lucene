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
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.IntroSorter;
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
@Measurement(iterations = 20, time = 1)
@Fork(
    value = 10,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class ScoreDocSortBenchmark {

  private static final Comparator<ScoreDoc> BY_DOC_ASC = (a, b) -> Integer.compare(a.doc, b.doc);

  @Param({"10", "50", "100", "500", "1000", "10000"})
  int size;

  /** Template array; copied before each invocation so every sort sees the same random order. */
  private ScoreDoc[] template;

  /** Working copy that each benchmark method sorts in place. */
  private ScoreDoc[] work;

  @Setup(Level.Trial)
  public void setupTrial() {
    SplittableRandom rng = new SplittableRandom(0xCAFEBABE);
    int maxDoc = 5_000_000; // realistic large index size
    template = new ScoreDoc[size];
    for (int i = 0; i < size; i++) {
      int doc = rng.nextInt(maxDoc);
      float score = (float) rng.nextDouble(0.0, 10.0);
      template[i] = new ScoreDoc(doc, score);
    }
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    work = Arrays.copyOf(template, size); // shallow copy – same ScoreDoc objects, different array
  }

  // ---- 1. JDK Arrays.sort with lambda ----

  @Benchmark
  public void jdkSortLambda(Blackhole bh) {
    Arrays.sort(work, (a, b) -> Integer.compare(a.doc, b.doc));
    bh.consume(work);
  }

  // ---- 2. JDK Arrays.sort with static comparator ----

  @Benchmark
  public void jdkSortComparator(Blackhole bh) {
    Arrays.sort(work, BY_DOC_ASC);
    bh.consume(work);
  }

  // ---- 3. ArrayUtil.introSort (wraps ArrayIntroSorter) ----

  @Benchmark
  public void arrayUtilIntroSort(Blackhole bh) {
    ArrayUtil.introSort(work, BY_DOC_ASC);
    bh.consume(work);
  }

  // ---- 4. ArrayUtil.timSort (wraps ArrayTimSorter) ----

  @Benchmark
  public void arrayUtilTimSort(Blackhole bh) {
    ArrayUtil.timSort(work, BY_DOC_ASC);
    bh.consume(work);
  }

  // ---- 5. Anonymous IntroSorter ----

  @Benchmark
  public void introSorterAnonymous(Blackhole bh) {
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
    bh.consume(work);
  }

  // ---- 6. Anonymous TimSorter ----

  @Benchmark
  public void timSorterAnonymous(Blackhole bh) {
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
    bh.consume(work);
  }

  // ---- 7. Anonymous InPlaceMergeSorter ----

  @Benchmark
  public void inPlaceMergeSorterAnonymous(Blackhole bh) {
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
    bh.consume(work);
  }

  // ---- 8. Extract doc IDs, sort with JDK Arrays.sort (primitive long[]), reorder ----

  @Benchmark
  public void jdkSortPrimitiveExtractLong(Blackhole bh) {
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
    bh.consume(sorted);
  }

  // ---- 9. Extract doc IDs, sort with int[] when bits fit, else long[] ----

  /** bits needed to represent values in [0, max) */
  private static int bitsNeeded(int max) {
    return 32 - Integer.numberOfLeadingZeros(max - 1);
  }

  @Benchmark
  public void jdkSortPrimitiveExtractAdaptive(Blackhole bh) {
    int len = work.length;
    int maxDoc = 5_000_000; // must match setupTrial
    int docBits = bitsNeeded(maxDoc);
    int indexBits = bitsNeeded(len);
    if (docBits + indexBits <= 32) {
      // pack into int[]: doc in upper bits, index in lower bits
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
      bh.consume(sorted);
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
      bh.consume(sorted);
    }
  }
}
