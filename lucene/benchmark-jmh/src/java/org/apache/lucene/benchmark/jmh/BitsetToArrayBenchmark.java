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
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.lucene.util.ArrayUtil;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class BitsetToArrayBenchmark {

  private final SplittableRandom R = new SplittableRandom(4314123142L);

  @Param({"5", "10", "20", "30", "40", "50", "60"})
  int bitCount;

  private long word;
  private int[] resultArray;
  private int base;
  private int offset;

  @Setup(Level.Trial)
  public void setup() {
    base = R.nextInt(1000);
    resultArray = new int[bitCount + 64 + Long.SIZE];
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    word = 0L;
    while (Long.bitCount(word) < bitCount) {
      word |= 1L << R.nextInt(64);
    }
    offset = R.nextInt(64);
  }

  @Benchmark
  public int whileLoop() {
    return _whileLoop(word, resultArray, offset, base);
  }

  @Benchmark
  public int forLoop() {
    return _forLoop(word, resultArray, offset, base);
  }

  @Benchmark
  public int forLoopManualUnrolling() {
    return _forLoopManualUnrolling(word, resultArray, offset, base);
  }

  @Benchmark
  public int dense() {
    return _dense(word, resultArray, offset, base);
  }

  @Benchmark
  public int denseBranchLess() {
    return _denseBranchLess(word, resultArray, offset, base);
  }

  @Benchmark
  public int denseBranchLessUnrolling() {
    return _denseBranchLessUnrolling(word, resultArray, offset, base);
  }

  @Benchmark
  public int denseBranchLessParallel() {
    return _denseBranchLessParallel(word, resultArray, offset, base);
  }

  @Benchmark
  public int denseBranchLessCmov() {
    return _denseBranchLessCmov(word, resultArray, offset, base);
  }

  @Benchmark
  public int denseInvert() {
    return _denseInvert(word, resultArray, offset, base);
  }

  @Benchmark
  public int hybrid() {
    return _hybrid(word, resultArray, offset, base);
  }

  private static int _whileLoop(long word, int[] resultArray, int offset, int base) {
    while (word != 0) {
      int bit = Long.numberOfTrailingZeros(word);
      resultArray[offset++] = base + bit;
      word ^= 1L << bit;
    }
    return offset;
  }

  private static int _forLoop(long word, int[] resultArray, int offset, int base) {
    int to = offset + Long.bitCount(word);
    for (int i = offset; i < to; i++) {
      int bit = Long.numberOfTrailingZeros(word);
      resultArray[i] = base + bit;
      word ^= 1L << bit;
    }
    return to;
  }

  private static int _forLoopManualUnrolling(long word, int[] resultArray, int offset, int base) {
    int to = offset + Long.bitCount(word);
    int i = offset;

    for (; i < to - 3; i += 4) {
      int ntz = Long.numberOfTrailingZeros(word);
      resultArray[i] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      resultArray[i + 1] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      resultArray[i + 2] = base + ntz;
      word ^= 1L << ntz;
      ntz = Long.numberOfTrailingZeros(word);
      resultArray[i + 3] = base + ntz;
      word ^= 1L << ntz;
    }

    for (; i < to; i++) {
      int ntz = Long.numberOfTrailingZeros(word);
      resultArray[i] = base + ntz;
      word ^= 1L << ntz;
    }

    return to;
  }

  private static int _dense(long word, int[] resultArray, int offset, int base) {
    for (int i = 0; i < Long.SIZE; i++) {
      if ((word & (1L << i)) != 0) {
        resultArray[offset++] = base + i;
      }
    }
    return offset;
  }

  private static int _denseBranchLess(long word, int[] resultArray, int offset, int base) {
    int lWord = (int) word;
    int hWord = (int) (word >>> 32);
    for (int i = 0; i < Integer.SIZE; i++) {
      resultArray[offset] = base + i;
      offset += lWord & 1;
      lWord >>>= 1;
    }
    for (int i = Integer.SIZE; i < Long.SIZE; i++) {
      resultArray[offset] = base + i;
      offset += hWord & 1;
      hWord >>>= 1;
    }
    return offset;
  }

  private static int _denseBranchLessCmov(long word, int[] resultArray, int offset, int base) {
    for (int j = 0; j < Long.SIZE; ++j) {
      resultArray[offset] = base + j;
      long bit = word & (1L << j);
      if (bit
          != 0L) { // can be compiled into cmovne bitCount = 20/30/40/50 (branch is unpredictable).
        offset = offset + 1;
      }
    }

    return offset;
  }

  private static int _denseBranchLessUnrolling(long word, int[] resultArray, int offset, int base) {
    int lWord = (int) word;
    int hWord = (int) (word >>> 32);

    resultArray[offset] = base + 0;
    offset += (lWord >>> 0) & 1;
    resultArray[offset] = base + 1;
    offset += (lWord >>> 1) & 1;
    resultArray[offset] = base + 2;
    offset += (lWord >>> 2) & 1;
    resultArray[offset] = base + 3;
    offset += (lWord >>> 3) & 1;
    resultArray[offset] = base + 4;
    offset += (lWord >>> 4) & 1;
    resultArray[offset] = base + 5;
    offset += (lWord >>> 5) & 1;
    resultArray[offset] = base + 6;
    offset += (lWord >>> 6) & 1;
    resultArray[offset] = base + 7;
    offset += (lWord >>> 7) & 1;
    resultArray[offset] = base + 8;
    offset += (lWord >>> 8) & 1;
    resultArray[offset] = base + 9;
    offset += (lWord >>> 9) & 1;
    resultArray[offset] = base + 10;
    offset += (lWord >>> 10) & 1;
    resultArray[offset] = base + 11;
    offset += (lWord >>> 11) & 1;
    resultArray[offset] = base + 12;
    offset += (lWord >>> 12) & 1;
    resultArray[offset] = base + 13;
    offset += (lWord >>> 13) & 1;
    resultArray[offset] = base + 14;
    offset += (lWord >>> 14) & 1;
    resultArray[offset] = base + 15;
    offset += (lWord >>> 15) & 1;
    resultArray[offset] = base + 16;
    offset += (lWord >>> 16) & 1;
    resultArray[offset] = base + 17;
    offset += (lWord >>> 17) & 1;
    resultArray[offset] = base + 18;
    offset += (lWord >>> 18) & 1;
    resultArray[offset] = base + 19;
    offset += (lWord >>> 19) & 1;
    resultArray[offset] = base + 20;
    offset += (lWord >>> 20) & 1;
    resultArray[offset] = base + 21;
    offset += (lWord >>> 21) & 1;
    resultArray[offset] = base + 22;
    offset += (lWord >>> 22) & 1;
    resultArray[offset] = base + 23;
    offset += (lWord >>> 23) & 1;
    resultArray[offset] = base + 24;
    offset += (lWord >>> 24) & 1;
    resultArray[offset] = base + 25;
    offset += (lWord >>> 25) & 1;
    resultArray[offset] = base + 26;
    offset += (lWord >>> 26) & 1;
    resultArray[offset] = base + 27;
    offset += (lWord >>> 27) & 1;
    resultArray[offset] = base + 28;
    offset += (lWord >>> 28) & 1;
    resultArray[offset] = base + 29;
    offset += (lWord >>> 29) & 1;
    resultArray[offset] = base + 30;
    offset += (lWord >>> 30) & 1;
    resultArray[offset] = base + 31;
    offset += (lWord >>> 31) & 1;

    resultArray[offset] = base + 32;
    offset += (hWord >>> 0) & 1;
    resultArray[offset] = base + 33;
    offset += (hWord >>> 1) & 1;
    resultArray[offset] = base + 34;
    offset += (hWord >>> 2) & 1;
    resultArray[offset] = base + 35;
    offset += (hWord >>> 3) & 1;
    resultArray[offset] = base + 36;
    offset += (hWord >>> 4) & 1;
    resultArray[offset] = base + 37;
    offset += (hWord >>> 5) & 1;
    resultArray[offset] = base + 38;
    offset += (hWord >>> 6) & 1;
    resultArray[offset] = base + 39;
    offset += (hWord >>> 7) & 1;
    resultArray[offset] = base + 40;
    offset += (hWord >>> 8) & 1;
    resultArray[offset] = base + 41;
    offset += (hWord >>> 9) & 1;
    resultArray[offset] = base + 42;
    offset += (hWord >>> 10) & 1;
    resultArray[offset] = base + 43;
    offset += (hWord >>> 11) & 1;
    resultArray[offset] = base + 44;
    offset += (hWord >>> 12) & 1;
    resultArray[offset] = base + 45;
    offset += (hWord >>> 13) & 1;
    resultArray[offset] = base + 46;
    offset += (hWord >>> 14) & 1;
    resultArray[offset] = base + 47;
    offset += (hWord >>> 15) & 1;
    resultArray[offset] = base + 48;
    offset += (hWord >>> 16) & 1;
    resultArray[offset] = base + 49;
    offset += (hWord >>> 17) & 1;
    resultArray[offset] = base + 50;
    offset += (hWord >>> 18) & 1;
    resultArray[offset] = base + 51;
    offset += (hWord >>> 19) & 1;
    resultArray[offset] = base + 52;
    offset += (hWord >>> 20) & 1;
    resultArray[offset] = base + 53;
    offset += (hWord >>> 21) & 1;
    resultArray[offset] = base + 54;
    offset += (hWord >>> 22) & 1;
    resultArray[offset] = base + 55;
    offset += (hWord >>> 23) & 1;
    resultArray[offset] = base + 56;
    offset += (hWord >>> 24) & 1;
    resultArray[offset] = base + 57;
    offset += (hWord >>> 25) & 1;
    resultArray[offset] = base + 58;
    offset += (hWord >>> 26) & 1;
    resultArray[offset] = base + 59;
    offset += (hWord >>> 27) & 1;
    resultArray[offset] = base + 60;
    offset += (hWord >>> 28) & 1;
    resultArray[offset] = base + 61;
    offset += (hWord >>> 29) & 1;
    resultArray[offset] = base + 62;
    offset += (hWord >>> 30) & 1;
    resultArray[offset] = base + 63;
    offset += (hWord >>> 31) & 1;

    return offset;
  }

  private static int _denseBranchLessParallel(long word, int[] resultArray, int offset, int base) {
    final int lWord = (int) word;
    final int hWord = (int) (word >>> 32);

    final int offset32 = offset + Integer.bitCount(lWord);
    int hOffset = offset32;

    for (int i = 0; i < 32; i++) {
      resultArray[offset] = base + i;
      resultArray[hOffset] = base + i + 32;
      offset += (lWord >>> i) & 1;
      hOffset += (hWord >>> i) & 1;
    }

    resultArray[offset32] = base + 32 + Integer.numberOfTrailingZeros(hWord);

    return hOffset;
  }

  private static int _denseInvert(long word, int[] resultArray, int offset, int base) {
    int bit = 0;
    while (word != 0L) {
      int zeros = Long.numberOfTrailingZeros(word);
      word >>>= zeros;
      bit += zeros;

      int ones = Long.numberOfTrailingZeros(~word);
      word >>>= ones;
      for (int i = 0; i < ones; i++) {
        resultArray[offset++] = i + bit;
      }
      bit += ones;
    }

    return offset;
  }

  private static int _hybrid(long word, int[] resultArray, int offset, int base) {
    int bitCount = Long.bitCount(word);
    if (bitCount >= 32) {
      return _denseBranchLessParallel(word, resultArray, offset, base);
    }

    int to = offset + Long.bitCount(word);

    for (int i = offset; i < to; i++) {
      int ntz = Long.numberOfTrailingZeros(word);
      resultArray[i] = base + ntz;
      word ^= 1L << ntz;
    }

    return to;
  }

  public static void main(String[] args) {
    Random r = new Random(System.currentTimeMillis());
    long word = r.nextLong();
    int[] expected = new int[64 + Long.SIZE];
    int expectedSize = _whileLoop(word, expected, 0, 0);

    int[] actual = new int[64 + Long.SIZE];
    for (IntSupplier supplier :
        new IntSupplier[] {
          () -> _whileLoop(word, actual, 0, 0),
          () -> _forLoop(word, actual, 0, 0),
          () -> _forLoopManualUnrolling(word, actual, 0, 0),
          () -> _dense(word, actual, 0, 0),
          () -> _denseBranchLess(word, actual, 0, 0),
          () -> _denseBranchLessUnrolling(word, actual, 0, 0),
          () -> _denseBranchLessParallel(word, actual, 0, 0),
          () -> _denseBranchLessCmov(word, actual, 0, 0),
          () -> _denseInvert(word, actual, 0, 0),
          () -> _hybrid(word, actual, 0, 0)
        }) {
      int actualSize = supplier.getAsInt();
      if (actualSize != expectedSize) {
        throw new AssertionError("Expected size: " + expectedSize + ", but got: " + actualSize);
      }
      if (Arrays.equals(expected, 0, expectedSize, actual, 0, actualSize) == false) {
        throw new AssertionError(
            "Arrays do not match for supplier: "
                + supplier
                + ", expected: "
                + Arrays.toString(ArrayUtil.copyOfSubArray(expected, 0, expectedSize))
                + ", but got: "
                + Arrays.toString(ArrayUtil.copyOfSubArray(actual, 0, actualSize)));
      }
    }
  }
}
