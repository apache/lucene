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
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class AdvanceBenchmark {

  private final long[] values = new long[129];
  private final int[] startIndexes = new int[1_000];
  private final long[] targets = new long[startIndexes.length];

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
  private static int binarySearch(long[] values, long target, int startIndex) {
    // Standard binary search
    int i = Arrays.binarySearch(values, startIndex, values.length, target);
    if (i < 0) {
      i = -1 - i;
    }
    return i;
  }

  @Benchmark
  public void binarySearch2() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch2(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch2(long[] values, long target, int startIndex) {
    // Try to help the compiler by providing predictable start/end offsets.
    int i = Arrays.binarySearch(values, 0, 128, target);
    if (i < 0) {
      i = -1 - i;
    }
    return i;
  }

  @Benchmark
  public void binarySearch3() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch3(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch3(long[] values, long target, int startIndex) {
    // Organize code the same way as suggested in https://quickwit.io/blog/search-a-sorted-block,
    // which proved to help with LLVM.
    int start = 0;
    int length = 128;

    while (length > 1) {
      length /= 2;
      if (values[start + length - 1] < target) {
        start += length;
      }
    }
    return start;
  }

  @Benchmark
  public void binarySearch4() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch4(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch4(long[] values, long target, int startIndex) {
    // Explicitly inline the binary-search logic to see if it helps the compiler.
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
  public void binarySearch5() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch5(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch5(long[] values, long target, int startIndex) {
    // Other way to write a binary search
    int start = 0;

    for (int shift = 6; shift >= 0; --shift) {
      int halfRange = 1 << shift;
      if (values[start + halfRange - 1] < target) {
        start += halfRange;
      }
    }

    return start;
  }

  @Benchmark
  public void binarySearch6() {
    for (int i = 0; i < startIndexes.length; ++i) {
      binarySearch6(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int binarySearch6(long[] values, long target, int startIndex) {
    // Other way to write a binary search
    int start = 0;

    for (int halfRange = 64; halfRange > 0; halfRange >>= 1) {
      if (values[start + halfRange - 1] < target) {
        start += halfRange;
      }
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
  private static int linearSearch(long[] values, long target, int startIndex) {
    // Naive linear search.
    for (int i = startIndex; i < values.length; ++i) {
      if (values[i] >= target) {
        return i;
      }
    }
    return values.length;
  }

  @Benchmark
  public void bruteForceSearch() {
    for (int i = 0; i < startIndexes.length; ++i) {
      bruteForceSearch(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int bruteForceSearch(long[] values, long target, int startIndex) {
    // Linear search with predictable start/end offsets to see if it helps the compiler.
    for (int i = 0; i < 128; ++i) {
      if (values[i] >= target) {
        return i;
      }
    }
    return values.length;
  }

  @Benchmark
  public void linearSearch2() {
    for (int i = 0; i < startIndexes.length; ++i) {
      linearSearch2(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int linearSearch2(long[] values, long target, int startIndex) {
    // Two-level linear search, first checking every 8-th value, then values within an 8-value range
    int rangeEnd = values.length;

    for (int i = startIndex + 7; i < values.length; i += 8) {
      if (values[i] >= target) {
        rangeEnd = i;
        break;
      }
    }

    for (int i = rangeEnd - 7; i < rangeEnd; ++i) {
      if (values[i] >= target) {
        return i;
      }
    }

    return rangeEnd;
  }

  @Benchmark
  public void linearSearch3() {
    for (int i = 0; i < startIndexes.length; ++i) {
      linearSearch3(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int linearSearch3(long[] values, long target, int startIndex) {
    // Iteration over linearSearch that tries to reduce branches
    while (startIndex + 4 <= values.length) {
      int count = values[startIndex] < target ? 1 : 0;
      if (values[startIndex + 1] < target) {
        count++;
      }
      if (values[startIndex + 2] < target) {
        count++;
      }
      if (values[startIndex + 3] < target) {
        count++;
      }
      if (count != 4) {
        return startIndex + count;
      }
      startIndex += 4;
    }

    for (int i = startIndex; i < values.length; ++i) {
      if (values[i] >= target) {
        return i;
      }
    }

    return values.length;
  }

  @Benchmark
  public void hybridSearch() {
    for (int i = 0; i < startIndexes.length; ++i) {
      hybridSearch(values, targets[i], startIndexes[i]);
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static int hybridSearch(long[] values, long target, int startIndex) {
    // Two-level linear search, first checking every 8-th value, then values within an 8-value range
    int rangeEnd = values.length;

    for (int i = startIndex + 7; i < values.length; i += 8) {
      if (values[i] >= target) {
        rangeEnd = i;
        break;
      }
    }

    return binarySearchHelper8(values, target, rangeEnd - 7);
  }

  // branchless binary search over 8 values
  private static int binarySearchHelper8(long[] values, long target, int start) {
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

  private static void assertEquals(int expected, int actual) {
    if (expected != actual) {
      throw new AssertionError("Expected: " + expected + ", got " + actual);
    }
  }

  public static void main(String[] args) {
    // For testing purposes
    long[] values = new long[129];
    for (int i = 0; i < 128; ++i) {
      values[i] = i;
    }
    values[128] = DocIdSetIterator.NO_MORE_DOCS;
    for (int start = 0; start < 128; ++start) {
      for (int targetIndex = start; targetIndex < 128; ++targetIndex) {
        int actualIndex = binarySearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = binarySearch2(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = binarySearch3(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = binarySearch4(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = binarySearch5(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = binarySearch6(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = bruteForceSearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = hybridSearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = linearSearch(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = linearSearch2(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
        actualIndex = linearSearch3(values, values[targetIndex], start);
        assertEquals(targetIndex, actualIndex);
      }
    }
  }
}
