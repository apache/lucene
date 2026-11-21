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
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.DirectPackedValues;

/**
 * Utility methods to evaluate doc-values ranges on values packed by {@link
 * org.apache.lucene.util.packed.DirectWriter}, when they are memory-mapped. Byte-aligned values (8,
 * 16, 32 or 64 bits per value) are loaded directly from the {@link MemorySegment} into vector
 * registers, compared against the range, and the resulting lane masks are ORed into the bit set.
 * Unlike the {@link org.apache.lucene.util.LongValues}-based path, values aren't decoded one by one
 * through a scratch array, and for 8, 16 and 32 bits per value they aren't widened to longs either,
 * so a single comparison covers as many values as fit in a vector register.
 */
final class MemorySegmentDocValuesRangeUtil {

  // Cap the shape at 512 bits so that byte vectors have at most 64 lanes, which is the maximum
  // number of lanes that VectorMask#toLong supports.
  private static final VectorShape SHAPE =
      VectorShape.forBitSize(Math.min(PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE, 512));
  private static final VectorSpecies<Byte> BYTE_SPECIES = VectorSpecies.of(byte.class, SHAPE);
  private static final VectorSpecies<Short> SHORT_SPECIES = VectorSpecies.of(short.class, SHAPE);
  private static final VectorSpecies<Integer> INT_SPECIES = VectorSpecies.of(int.class, SHAPE);
  private static final VectorSpecies<Long> LONG_SPECIES = VectorSpecies.of(long.class, SHAPE);

  private static final ValueLayout.OfShort LE_SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfInt LE_INT =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfLong LE_LONG =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  private MemorySegmentDocValuesRangeUtil() {}

  static boolean supportsBitsPerValue(int bitsPerValue) {
    return bitsPerValue == 8 || bitsPerValue == 16 || bitsPerValue == 32 || bitsPerValue == 64;
  }

  /**
   * Sets bit {@code i - offset} for every index {@code i} in {@code [fromIndex, toIndex)} whose
   * stored value is in {@code [minValue, maxValue]}, reading values directly from the memory
   * segment of {@code packed}. Returns {@code false}, without touching {@code bitSet}, if {@code
   * packed} can't be read this way, in which case the caller must fall back to another path.
   */
  static boolean rangeIntoBitSet(
      DirectPackedValues packed,
      int fromIndex,
      int toIndex,
      long minValue,
      long maxValue,
      FixedBitSet bitSet,
      int offset) {
    final int bitsPerValue = packed.bitsPerValue();
    if (supportsBitsPerValue(bitsPerValue) == false) {
      return false;
    }
    final MemorySegment segment = segmentOrNull(packed.input());
    if (segment == null) {
      return false;
    }
    final int bytesPerValue = bitsPerValue / Byte.SIZE;
    final long base = packed.offset();
    if (fromIndex >= toIndex) {
      return true;
    }
    if (base + (long) toIndex * bytesPerValue > segment.byteSize()) {
      return false;
    }
    try {
      if (bitsPerValue == 64) {
        // 64-bit values are read as signed longs, exactly like the LongValues-based path does, so
        // there is no domain to narrow the bounds to.
        if (minValue <= maxValue) {
          rangeLongs(segment, base, fromIndex, toIndex, minValue, maxValue, bitSet, offset);
        }
        return true;
      }
      // Stored values are unsigned and fit in bitsPerValue bits: narrow the bounds to that
      // domain. This is what allows comparing against narrow lanes instead of longs.
      final long maxStoredValue = (1L << bitsPerValue) - 1;
      final long lo = Math.max(minValue, 0L);
      final long hi = Math.min(maxValue, maxStoredValue);
      if (lo > hi) {
        return true;
      }
      if (lo == 0 && hi == maxStoredValue) {
        bitSet.set(fromIndex - offset, toIndex - offset);
        return true;
      }
      switch (bitsPerValue) {
        case 8:
          rangeBytes(segment, base, fromIndex, toIndex, (int) lo, (int) hi, bitSet, offset);
          break;
        case 16:
          rangeShorts(segment, base, fromIndex, toIndex, (int) lo, (int) hi, bitSet, offset);
          break;
        case 32:
          rangeInts(segment, base, fromIndex, toIndex, lo, hi, bitSet, offset);
          break;
        default:
          throw new AssertionError("Unsupported bitsPerValue: " + bitsPerValue);
      }
      return true;
    } catch (IllegalStateException e) {
      // The memory segment was unmapped because the input got closed.
      throw new AlreadyClosedException("Already closed", e);
    }
  }

  /**
   * Returns the memory segment backing {@code input} if it's memory-mapped and fits in a single
   * segment, or {@code null} otherwise.
   */
  private static MemorySegment segmentOrNull(RandomAccessInput input) {
    if (input instanceof MemorySegmentAccessInput msai) {
      try {
        return msai.segmentSliceOrNull(0, input.length());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return null;
  }

  private static void rangeBytes(
      MemorySegment segment,
      long base,
      int from,
      int to,
      int lo,
      int hi,
      FixedBitSet bitSet,
      int offset) {
    final int lanes = BYTE_SPECIES.length();
    final ByteVector vlo = ByteVector.broadcast(BYTE_SPECIES, (byte) lo);
    final ByteVector vhi = ByteVector.broadcast(BYTE_SPECIES, (byte) hi);
    final int loopBound = from + BYTE_SPECIES.loopBound(to - from);
    int i = from;
    for (; i < loopBound; i += lanes) {
      ByteVector v =
          ByteVector.fromMemorySegment(BYTE_SPECIES, segment, base + i, ByteOrder.LITTLE_ENDIAN);
      long bits =
          v.compare(VectorOperators.UGE, vlo).and(v.compare(VectorOperators.ULE, vhi)).toLong();
      if (bits != 0) {
        bitSet.orMask(i - offset, bits, lanes);
      }
    }
    for (; i < to; ++i) {
      int v = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, base + i));
      if (v >= lo && v <= hi) {
        bitSet.set(i - offset);
      }
    }
  }

  private static void rangeShorts(
      MemorySegment segment,
      long base,
      int from,
      int to,
      int lo,
      int hi,
      FixedBitSet bitSet,
      int offset) {
    final int lanes = SHORT_SPECIES.length();
    final ShortVector vlo = ShortVector.broadcast(SHORT_SPECIES, (short) lo);
    final ShortVector vhi = ShortVector.broadcast(SHORT_SPECIES, (short) hi);
    final int loopBound = from + SHORT_SPECIES.loopBound(to - from);
    int i = from;
    for (; i < loopBound; i += lanes) {
      ShortVector v =
          ShortVector.fromMemorySegment(
              SHORT_SPECIES, segment, base + (long) i * Short.BYTES, ByteOrder.LITTLE_ENDIAN);
      long bits =
          v.compare(VectorOperators.UGE, vlo).and(v.compare(VectorOperators.ULE, vhi)).toLong();
      if (bits != 0) {
        bitSet.orMask(i - offset, bits, lanes);
      }
    }
    for (; i < to; ++i) {
      int v = Short.toUnsignedInt(segment.get(LE_SHORT, base + (long) i * Short.BYTES));
      if (v >= lo && v <= hi) {
        bitSet.set(i - offset);
      }
    }
  }

  private static void rangeInts(
      MemorySegment segment,
      long base,
      int from,
      int to,
      long lo,
      long hi,
      FixedBitSet bitSet,
      int offset) {
    final int lanes = INT_SPECIES.length();
    final IntVector vlo = IntVector.broadcast(INT_SPECIES, (int) lo);
    final IntVector vhi = IntVector.broadcast(INT_SPECIES, (int) hi);
    final int loopBound = from + INT_SPECIES.loopBound(to - from);
    int i = from;
    for (; i < loopBound; i += lanes) {
      IntVector v =
          IntVector.fromMemorySegment(
              INT_SPECIES, segment, base + (long) i * Integer.BYTES, ByteOrder.LITTLE_ENDIAN);
      long bits =
          v.compare(VectorOperators.UGE, vlo).and(v.compare(VectorOperators.ULE, vhi)).toLong();
      if (bits != 0) {
        bitSet.orMask(i - offset, bits, lanes);
      }
    }
    for (; i < to; ++i) {
      long v = Integer.toUnsignedLong(segment.get(LE_INT, base + (long) i * Integer.BYTES));
      if (v >= lo && v <= hi) {
        bitSet.set(i - offset);
      }
    }
  }

  private static void rangeLongs(
      MemorySegment segment,
      long base,
      int from,
      int to,
      long lo,
      long hi,
      FixedBitSet bitSet,
      int offset) {
    final int lanes = LONG_SPECIES.length();
    final LongVector vlo = LongVector.broadcast(LONG_SPECIES, lo);
    final LongVector vhi = LongVector.broadcast(LONG_SPECIES, hi);
    final int loopBound = from + LONG_SPECIES.loopBound(to - from);
    int i = from;
    for (; i < loopBound; i += lanes) {
      LongVector v =
          LongVector.fromMemorySegment(
              LONG_SPECIES, segment, base + (long) i * Long.BYTES, ByteOrder.LITTLE_ENDIAN);
      long bits =
          v.compare(VectorOperators.GE, vlo).and(v.compare(VectorOperators.LE, vhi)).toLong();
      if (bits != 0) {
        bitSet.orMask(i - offset, bits, lanes);
      }
    }
    for (; i < to; ++i) {
      long v = segment.get(LE_LONG, base + (long) i * Long.BYTES);
      if (v >= lo && v <= hi) {
        bitSet.set(i - offset);
      }
    }
  }
}
