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
import java.util.stream.IntStream;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
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

  @Param({"256", "384", "512", "768", "1024"})
  int bitLength;

  private int base;
  private FixedBitSet bitSet;
  private final int[] resultArray = new int[128 + 16];

  @Setup(Level.Trial)
  public void setup() {
    bitSet = new FixedBitSet(bitLength);
    if (bitLength % Long.SIZE != 0) {
      throw new IllegalArgumentException(
          "bitLength must be a multiple of 64, but was: " + bitLength);
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    while (bitSet.cardinality() < 128) {
      bitSet.set(R.nextInt(bitLength));
    }
    base = R.nextInt(1000);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // light-weight shuffle of bits
    long[] bits = bitSet.getBits();
    long first = bits[0], last = bits[bits.length - 1];
    for (int i = 0; i < bits.length - 1; i++) {
      bits[i] = Long.reverseBytes((bits[i] << 31) | (bits[i + 1] >>> 33));
    }
    bits[bits.length - 1] = Long.reverseBytes((last << 31) | (first >>> 33));
  }

  @Benchmark
  public int whileLoop() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _whileLoop(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int forLoop() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _forLoop(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int forLoopManualUnrolling() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _forLoopManualUnrolling(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int dense() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _dense(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int denseBranchLess() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLess(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int denseBranchLessUnrolling() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessUnrolling(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int denseBranchLessParallel() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessParallel(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int denseBranchLessCmov() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessCmov(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int denseInvert() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseInvert(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int hybrid() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _hybrid(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  public int hybridUnrolling() {
    int offset = 0, i = 0;
    long[] bits = bitSet.getBits();
    for (int to = bits.length - 3; i < to; i += 4) {
      int bitCount1 = Long.bitCount(bits[i]);
      int bitCount2 = Long.bitCount(bits[i + 1]);
      int bitCount3 = Long.bitCount(bits[i + 2]);
      int bitCount4 = Long.bitCount(bits[i + 3]);
      int offset1 = bitCount1 + offset;
      int offset2 = bitCount2 + offset1;
      int offset3 = bitCount3 + offset2;
      _hybridBitCount(bits[i], resultArray, offset, bitCount1, base);
      _hybridBitCount(bits[i + 1], resultArray, offset1, bitCount2, base);
      _hybridBitCount(bits[i + 2], resultArray, offset2, bitCount3, base);
      _hybridBitCount(bits[i + 3], resultArray, offset3, bitCount4, base);
      offset = offset3 + bitCount4;
    }
    for (; i < bits.length; i++) {
      int bitCount = Long.bitCount(bits[i]);
      _hybridBitCount(bits[i], resultArray, offset, bitCount, base);
      offset += bitCount;
    }
    return offset;
  }

  // NOCOMMIT remove vector module requirement if merge
  @Benchmark
  @Fork(jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "-XX:UseAVX=3"})
  public int denseBranchLessVectorized() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessVectorized(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  @Fork(jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "-XX:UseAVX=2"})
  public int denseBranchLessVectorizedAVX2() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessVectorized(bit, resultArray, offset, base);
    }
    return offset;
  }

  @Benchmark
  @Fork(jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "-XX:UseAVX=3"})
  public int denseBranchLessVectorizedFromLong() {
    int offset = 0;
    for (long bit : bitSet.getBits()) {
      offset = _denseBranchLessVectorizedFromLong(bit, resultArray, offset, base);
    }
    return offset;
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

  private static final byte[] IDENTITY_BYTES = new byte[64];

  static {
    for (int i = 0; i < IDENTITY_BYTES.length; i++) {
      IDENTITY_BYTES[i] = (byte) i;
    }
  }

  // NOCOMMIT remove vectorized methods and requirement on vector module before merge.
  @SuppressWarnings("fallthrough")
  private static int _denseBranchLessVectorizedFromLong(
      long word, int[] resultArray, int offset, int base) {
    if (word == 0L) {
      return offset;
    }

    int bitCount = Long.bitCount(word);

    VectorMask<Byte> mask = VectorMask.fromLong(ByteVector.SPECIES_512, word);
    ByteVector indices =
        ByteVector.fromArray(ByteVector.SPECIES_512, IDENTITY_BYTES, 0).compress(mask);

    switch ((bitCount - 1) >> 4) {
      case 3:
        indices
            .convert(VectorOperators.B2I, 3)
            .reinterpretAsInts()
            .add(base)
            .intoArray(resultArray, offset + 48);
      case 2:
        indices
            .convert(VectorOperators.B2I, 2)
            .reinterpretAsInts()
            .add(base)
            .intoArray(resultArray, offset + 32);
      case 1:
        indices
            .convert(VectorOperators.B2I, 1)
            .reinterpretAsInts()
            .add(base)
            .intoArray(resultArray, offset + 16);
      case 0:
        indices
            .convert(VectorOperators.B2I, 0)
            .reinterpretAsInts()
            .add(base)
            .intoArray(resultArray, offset);
        break;
      default:
        throw new IllegalStateException(bitCount + "");
    }

    return offset + bitCount;
  }

  private static final int[] IDENTITY = IntStream.range(0, Long.SIZE).toArray();
  private static final int[] IDENTITY_MASK = IntStream.range(0, 16).map(i -> 1 << i).toArray();
  private static final int MASK = (1 << IntVector.SPECIES_PREFERRED.length()) - 1;

  private static int _denseBranchLessVectorized(
      long word, int[] resultArray, int offset, int base) {
    offset = _denseBranchLessVectorizedInt((int) word, resultArray, offset, base);
    return _denseBranchLessVectorizedInt((int) (word >>> 32), resultArray, offset, base + 32);
  }

  private static int _denseBranchLessVectorizedInt(
      int word, int[] resultArray, int offset, int base) {
    IntVector bitMask = IntVector.fromArray(IntVector.SPECIES_PREFERRED, IDENTITY_MASK, 0);

    for (int i = 0; i < Integer.SIZE; i += IntVector.SPECIES_PREFERRED.length()) {
      VectorMask<Integer> mask =
          IntVector.broadcast(IntVector.SPECIES_PREFERRED, word)
              .and(bitMask)
              .compare(VectorOperators.NE, 0);

      IntVector.fromArray(IntVector.SPECIES_PREFERRED, IDENTITY, i)
          .add(base)
          .compress(mask)
          .reinterpretAsInts()
          .intoArray(resultArray, offset);

      offset += Integer.bitCount(word & MASK); // faster than mask.trueCount()
      word >>>= IntVector.SPECIES_PREFERRED.length();
    }

    return offset;
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

  private static void _hybridBitCount(
      long word, int[] resultArray, int offset, int bitCount, int base) {
    if (bitCount >= 32) {
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
    } else {
      for (int i = offset, to = offset + bitCount; i < to; i++) {
        int ntz = Long.numberOfTrailingZeros(word);
        resultArray[i] = base + ntz;
        word ^= 1L << ntz;
      }
    }
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
          () -> _hybrid(word, actual, 0, 0),
          () -> {
            int bitCount = Long.bitCount(word);
            _hybridBitCount(word, actual, 0, bitCount, 0);
            return bitCount;
          }
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
