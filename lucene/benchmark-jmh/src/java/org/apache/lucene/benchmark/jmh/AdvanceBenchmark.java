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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx1g",
      "-Xms1g",
      "-XX:+AlwaysPreTouch",
      "--add-modules",
      "jdk.incubator.vector"
    })
public class AdvanceBenchmark {

  private final int[] values = new int[129];
  private final int[] startIndexes = new int[1_000];
  private final int[] targets = new int[startIndexes.length];

  @Setup(Level.Trial)
  public void setup() throws Exception {
    for (int i = 0; i < 128; ++i) {
      values[i] = i;
    }
    values[128] = DocIdSetIterator.NO_MORE_DOCS;
    Random r = new Random(0);
    for (int i = 0; i < startIndexes.length; ++i) {
      startIndexes[i] = r.nextInt(64);
      targets[i] = startIndexes[i] + 1 + r.nextInt(1 << r.nextInt(7));
    }
  }

  @Benchmark
  public void binarySearch() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch(int[] values, int target, int startIndex) {
    // Standard binary search
    int i = Arrays.binarySearch(values, startIndex, values.length, target);
    if (i < 0) {
      i = -1 - i;
    }
    return i;
  }

  @Benchmark
  public void inlinedBranchlessBinarySearch() {
    for (int i = 0; i < targets.length; ++i) {
      inlinedBranchlessBinarySearch(values, targets[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int inlinedBranchlessBinarySearch(int[] values, int target) {
    // This compiles to cmov instructions.
    int start = 0;

    if (values[63] < target) {
      start += 64;
    }
    if (values[start + 31] < target) {
      start += 32;
    }
    if (values[start + 15] < target) {
      start += 16;
    }
    if (values[start + 7] < target) {
      start += 8;
    }
    if (values[start + 3] < target) {
      start += 4;
    }
    if (values[start + 1] < target) {
      start += 2;
    }
    if (values[start] < target) {
      start += 1;
    }

    return start;
  }

  @Benchmark
  public void linearSearch() {
    for (int i = 0; i < startIndexes.length; ++i) {
      linearSearch(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int linearSearch(int[] values, long target, int startIndex) {
    // Naive linear search.
    for (int i = startIndex; i < values.length; ++i) {
      if (values[i] >= target) {
        return i;
      }
    }
    return values.length;
  }

  @Benchmark
  public void vectorUtilSearch() {
    for (int i = 0; i < startIndexes.length; ++i) {
      VectorUtil.findNextGEQ(values, targets[i], startIndexes[i], 128);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int vectorUtilSearch(int[] values, int target, int startIndex) {
    return VectorUtil.findNextGEQ(values, target, startIndex, 128);
  }

  private static void assertEquals(int expected, int actual) {
    if (expected != actual) {
      throw new AssertionError("Expected: " + expected + ", got " + actual);
    }
  }

  public static void main(String[] args) {
    // For testing purposes
    int[] values = new int[129];
    for (int i = 0; i < 128; ++i) {
      values[i] = i;
    }
    values[128] = DocIdSetIterator.NO_MORE_DOCS;
    for (int start = 0; start < 128; ++start) {
      for (int targetIndex = start; targetIndex < 128; ++targetIndex) {
        int actualIndex = binarySearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = inlinedBranchlessBinarySearch(values, values[targetIndex]);
        assertEquals(targetIndex, actualIndex);
        actualIndex = linearSearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = vectorUtilSearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
      }
    }
  }
}
