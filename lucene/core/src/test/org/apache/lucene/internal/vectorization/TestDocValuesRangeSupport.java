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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectPackedValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Tests {@link DocValuesRangeSupport#rangeIntoBitSet} by comparing the default (scalar) and the
 * Panama implementations against a brute-force evaluation. The Panama implementation evaluates
 * values packed by {@link DirectWriter} directly from the memory segment when the input is
 * memory-mapped, and falls back to {@link LongValues#get} otherwise.
 */
public class TestDocValuesRangeSupport extends BaseVectorizationTestCase {

  private static final int[] ALL_BITS_PER_VALUE = {
    1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64
  };

  /** Bits per value for which the Panama implementation reads the memory segment directly. */
  private static final int[] MEMORY_SEGMENT_BITS_PER_VALUE = {8, 16, 32, 64};

  /** Range lengths around every possible vector length (2 to 64 lanes). */
  private static final int[] LENGTHS_AROUND_VECTOR_SIZES = {
    0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 131
  };

  private final DocValuesRangeSupport scalarSupport = LUCENE_PROVIDER.getDocValuesRangeSupport();
  private final DocValuesRangeSupport vectorSupport =
      PANAMA_OR_NATIVE_PROVIDER.getDocValuesRangeSupport();

  public void testMemoryMapped() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : ALL_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "values_" + bitsPerValue, bitsPerValue)) {
          assertTrue(
              "Expected a single memory segment on MMapDirectory",
              file.values.input() instanceof MemorySegmentAccessInput msai
                  && msai.segmentSliceOrNull(0, file.values.input().length()) != null);
          assertRandomRangesMatchBruteForce(file);
        }
      }
    }
  }

  public void testNotMemoryMapped() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      for (int bitsPerValue : ALL_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "values_" + bitsPerValue, bitsPerValue)) {
          assertFalse(file.values.input() instanceof MemorySegmentAccessInput);
          assertRandomRangesMatchBruteForce(file);
        }
      }
    }
  }

  public void testMultipleMemoryMapChunks() throws IOException {
    // Tiny chunks so that values span several memory segments and can't be read as one.
    try (Directory dir = new MMapDirectory(createTempDir(), 1 << 10)) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        int numValues = TestUtil.nextInt(random(), 2_000, 5_000);
        try (PackedFile file =
            PackedFile.write(dir, "chunked_" + bitsPerValue, bitsPerValue, numValues)) {
          RandomAccessInput input = file.values.input();
          assertTrue(input instanceof MemorySegmentAccessInput);
          assertNull(
              "Values should span several chunks",
              ((MemorySegmentAccessInput) input).segmentSliceOrNull(0, input.length()));
          assertRandomRangesMatchBruteForce(file);
        }
      }
    }
  }

  public void testLengthsAroundVectorSizes() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        try (PackedFile file =
            PackedFile.write(dir, "lengths_" + bitsPerValue, bitsPerValue, 300)) {
          for (int length : LENGTHS_AROUND_VECTOR_SIZES) {
            for (int from : new int[] {0, 1, random().nextInt(300 - length + 1)}) {
              if (from + length > file.numValues()) {
                continue;
              }
              long[] bounds = randomBounds(file.stored, bitsPerValue);
              assertRangeMatchesBruteForce(file, from, from + length, bounds[0], bounds[1], from);
            }
          }
        }
      }
    }
  }

  public void testEmptyRange() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "empty_" + bitsPerValue, bitsPerValue)) {
          int from = random().nextInt(file.numValues() + 1);
          FixedBitSet expected = randomBitSet(file.numValues() + 64);
          for (DocValuesRangeSupport support :
              new DocValuesRangeSupport[] {scalarSupport, vectorSupport}) {
            FixedBitSet actual = expected.clone();
            support.rangeIntoBitSet(file.values(), from, from, 0, Long.MAX_VALUE, actual, 0);
            assertEquals(expected, actual);
          }
        }
      }
    }
  }

  public void testAllValuesMatch() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "all_" + bitsPerValue, bitsPerValue)) {
          int from = random().nextInt(file.numValues() + 1);
          int to = TestUtil.nextInt(random(), from, file.numValues());
          long min = bitsPerValue == 64 ? Long.MIN_VALUE : 0;
          long max = bitsPerValue == 64 ? Long.MAX_VALUE : maxStoredValue(bitsPerValue);
          FixedBitSet actual = new FixedBitSet(file.numValues());
          vectorSupport.rangeIntoBitSet(file.values(), from, to, min, max, actual, 0);
          FixedBitSet expected = new FixedBitSet(file.numValues());
          expected.set(from, to);
          assertEquals(expected, actual);
        }
      }
    }
  }

  public void testWrappedValues() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "wrapped_" + bitsPerValue, bitsPerValue)) {
          LongValues directValues = file.values();
          // A wrapper, e.g. a decoding LongValues, doesn't expose its storage.
          LongValues wrapped =
              new LongValues() {
                @Override
                public long get(long index) {
                  return directValues.get(index);
                }
              };
          assertFalse(wrapped instanceof DirectPackedValues);
          assertRandomRangesMatchBruteForce(file, wrapped);
        }
      }
    }
  }

  public void testMergeInstance() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        try (PackedFile file = PackedFile.write(dir, "merge_" + bitsPerValue, bitsPerValue)) {
          LongValues mergeValues =
              DirectReader.getMergeInstance(
                  file.slice, bitsPerValue, file.valuesOffset, file.numValues());
          assertFalse(mergeValues instanceof DirectPackedValues);
          assertRandomRangesMatchBruteForce(file, mergeValues);
        }
      }
    }
  }

  public void testBeyondReadableBytes() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      for (int bitsPerValue : MEMORY_SEGMENT_BITS_PER_VALUE) {
        int numValues = TestUtil.nextInt(random(), 10, 100);
        long[] stored = randomValues(bitsPerValue, numValues);
        String name = "short_" + bitsPerValue;
        writeValues(dir, name, bitsPerValue, stored, 0);
        try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
          // A slice that holds exactly numValues values, without DirectWriter's padding.
          RandomAccessInput slice =
              in.randomAccessSlice(0, (long) numValues * (bitsPerValue / Byte.SIZE));
          LongValues values = DirectReader.getInstance(slice, bitsPerValue);
          int to = numValues + TestUtil.nextInt(random(), 1, 10);
          FixedBitSet bitSet = new FixedBitSet(to);
          Throwable scalarFailure =
              expectThrows(
                  Throwable.class,
                  () -> scalarSupport.rangeIntoBitSet(values, 0, to, 1, 2, bitSet, 0));
          Throwable vectorFailure =
              expectThrows(
                  Throwable.class,
                  () -> vectorSupport.rangeIntoBitSet(values, 0, to, 1, 2, bitSet, 0));
          assertEquals(scalarFailure.getClass(), vectorFailure.getClass());
        }
      }
    }
  }

  public void testAlreadyClosed() throws IOException {
    try (Directory dir = new MMapDirectory(createTempDir())) {
      int numValues = TestUtil.nextInt(random(), 100, 1000);
      long[] stored = randomValues(8, numValues);
      writeValues(dir, "closed", 8, stored, 0);
      IndexInput in = dir.openInput("closed", IOContext.DEFAULT);
      LongValues values = DirectReader.getInstance(in.randomAccessSlice(0, in.length()), 8);
      FixedBitSet bitSet = new FixedBitSet(numValues);
      vectorSupport.rangeIntoBitSet(values, 0, numValues, 10, 200, bitSet, 0);
      in.close();
      expectThrows(
          AlreadyClosedException.class,
          () -> vectorSupport.rangeIntoBitSet(values, 0, numValues, 10, 200, bitSet, 0));
    }
  }

  /** Packed values written to a file, read back with {@link DirectReader}. */
  private static final class PackedFile implements AutoCloseable {
    final long[] stored;
    final IndexInput in;
    final RandomAccessInput slice;
    final long valuesOffset;
    final DirectPackedValues values;

    private PackedFile(
        long[] stored, IndexInput in, RandomAccessInput slice, long valuesOffset, int bpv) {
      this.stored = stored;
      this.in = in;
      this.slice = slice;
      this.valuesOffset = valuesOffset;
      this.values = (DirectPackedValues) DirectReader.getInstance(slice, bpv, valuesOffset);
      assertEquals(bpv, values.bitsPerValue());
      assertSame(slice, values.input());
      assertEquals(valuesOffset, values.offset());
    }

    static PackedFile write(Directory dir, String name, int bitsPerValue) throws IOException {
      int numValues =
          random().nextBoolean()
              ? TestUtil.nextInt(random(), 1, 200)
              : TestUtil.nextInt(random(), 1000, 20_000);
      return write(dir, name, bitsPerValue, numValues);
    }

    static PackedFile write(Directory dir, String name, int bitsPerValue, int numValues)
        throws IOException {
      long[] stored = randomValues(bitsPerValue, numValues);
      int prefix = random().nextInt(17);
      writeValues(dir, name, bitsPerValue, stored, prefix);
      IndexInput in = dir.openInput(name, IOContext.DEFAULT);
      if (random().nextBoolean()) {
        // Like Lucene90DocValuesProducer: a slice that starts at the first value.
        return new PackedFile(
            stored, in, in.randomAccessSlice(prefix, in.length() - prefix), 0, bitsPerValue);
      } else {
        // A slice over the whole file, with values starting at an offset.
        return new PackedFile(
            stored, in, in.randomAccessSlice(0, in.length()), prefix, bitsPerValue);
      }
    }

    LongValues values() {
      return (LongValues) values;
    }

    int numValues() {
      return stored.length;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  private void assertRandomRangesMatchBruteForce(PackedFile file) {
    assertRandomRangesMatchBruteForce(file, file.values());
  }

  private void assertRandomRangesMatchBruteForce(PackedFile file, LongValues values) {
    final int iters = atLeast(50);
    for (int iter = 0; iter < iters; ++iter) {
      int from = random().nextInt(file.numValues() + 1);
      int to = TestUtil.nextInt(random(), from, file.numValues());
      // Bits are set at i - offset; offset may be below `from`, and may be negative.
      int offset = from - random().nextInt(65);
      long[] bounds = randomBounds(file.stored, file.values.bitsPerValue());
      assertRangeMatchesBruteForce(file, values, from, to, bounds[0], bounds[1], offset);
    }
  }

  private void assertRangeMatchesBruteForce(
      PackedFile file, int from, int to, long min, long max, int offset) {
    assertRangeMatchesBruteForce(file, file.values(), from, to, min, max, offset);
  }

  private void assertRangeMatchesBruteForce(
      PackedFile file, LongValues values, int from, int to, long min, long max, int offset) {
    int numBits = Math.max(1, to - offset + random().nextInt(65));
    // Pre-existing bits must be preserved (OR semantics).
    FixedBitSet initial = random().nextBoolean() ? randomBitSet(numBits) : new FixedBitSet(numBits);
    FixedBitSet expected = initial.clone();
    for (int i = from; i < to; ++i) {
      if (file.stored[i] >= min && file.stored[i] <= max) {
        expected.set(i - offset);
      }
    }
    String message =
        "bpv="
            + file.values.bitsPerValue()
            + " from="
            + from
            + " to="
            + to
            + " offset="
            + offset
            + " min="
            + min
            + " max="
            + max;
    FixedBitSet scalar = initial.clone();
    scalarSupport.rangeIntoBitSet(values, from, to, min, max, scalar, offset);
    assertEquals(message, expected, scalar);
    FixedBitSet vector = initial.clone();
    vectorSupport.rangeIntoBitSet(values, from, to, min, max, vector, offset);
    assertEquals(message, expected, vector);
  }

  private static FixedBitSet randomBitSet(int numBits) {
    FixedBitSet bitSet = new FixedBitSet(numBits);
    for (int i = 0; i < numBits; ++i) {
      if (random().nextInt(8) == 0) {
        bitSet.set(i);
      }
    }
    return bitSet;
  }

  /** Writes {@code prefix} random bytes followed by packed values. */
  private static void writeValues(
      Directory dir, String name, int bitsPerValue, long[] stored, int prefix) throws IOException {
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      for (int i = 0; i < prefix; ++i) {
        out.writeByte((byte) random().nextInt());
      }
      DirectWriter writer = DirectWriter.getInstance(out, stored.length, bitsPerValue);
      for (long v : stored) {
        writer.add(v);
      }
      writer.finish();
    }
  }

  private static long maxStoredValue(int bitsPerValue) {
    return bitsPerValue == 64 ? Long.MAX_VALUE : (1L << bitsPerValue) - 1;
  }

  private static long[] randomValues(int bitsPerValue, int numValues) {
    long[] stored = new long[numValues];
    if (bitsPerValue == 64 && random().nextBoolean()) {
      // 64-bit values are read as signed longs: exercise negative values too.
      for (int i = 0; i < numValues; ++i) {
        stored[i] = random().nextLong();
      }
      return stored;
    }
    long maxValue = maxStoredValue(bitsPerValue);
    boolean narrow = random().nextBoolean();
    long base = narrow ? (long) (random().nextDouble() * maxValue) : 0;
    long spread = narrow ? Math.min(maxValue - base, 1 + random().nextInt(100)) : maxValue;
    for (int i = 0; i < numValues; ++i) {
      stored[i] = base + (long) (random().nextDouble() * (spread + 1.0));
      stored[i] = Math.min(Math.max(stored[i], 0), maxValue);
    }
    // Make sure the extremes of the stored domain are present sometimes.
    if (numValues > 2 && random().nextBoolean()) {
      stored[random().nextInt(numValues)] = 0;
      stored[random().nextInt(numValues)] = maxValue;
    }
    return stored;
  }

  private static long[] randomBounds(long[] stored, int bitsPerValue) {
    long maxValue = maxStoredValue(bitsPerValue);
    long a = pickBound(stored, maxValue);
    long b = pickBound(stored, maxValue);
    if (random().nextInt(10) != 0 && a > b) {
      // Mostly use non-empty ranges, but sometimes inverted ones.
      long tmp = a;
      a = b;
      b = tmp;
    }
    return new long[] {a, b};
  }

  private static long pickBound(long[] stored, long maxValue) {
    switch (random().nextInt(8)) {
      case 0:
        return Long.MIN_VALUE;
      case 1:
        return Long.MAX_VALUE;
      case 2:
        return -1 - random().nextInt(10);
      case 3:
        return maxValue == Long.MAX_VALUE ? maxValue : maxValue + 1 + random().nextInt(10);
      case 4:
        return 0;
      case 5:
        return maxValue;
      default:
        if (stored.length == 0) {
          return 0;
        }
        long v = stored[random().nextInt(stored.length)];
        long delta = random().nextInt(5) - 2;
        return v + delta;
    }
  }
}
