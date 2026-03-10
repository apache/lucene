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
 * Benchmark comparing different sort implementations for sorting ScoreDoc[] by ascending doc ID.
 * Simulates realistic ScoreDoc arrays with random doc IDs drawn from a large index and random
 * scores. Use jmh-table.py to visualize JSON results as an interactive HTML report.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 20, time = 1)
@Fork(
    value = 8,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class ScoreDocSortBenchmark {

  private static final Comparator<ScoreDoc> BY_DOC_ASC = (a, b) -> Integer.compare(a.doc, b.doc);

  @Param({"10", "100", "1000", "10000"})
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
    work = new ScoreDoc[size];
    for (int i = 0; i < size; i++) {
      work[i] = template[i]; // shallow copy – same ScoreDoc objects, different array
    }
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
  public void jdkSortPrimitiveExtract(Blackhole bh) {
    int len = work.length;
    // Build parallel array of (doc, originalIndex) packed into a long for a single-array sort
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
