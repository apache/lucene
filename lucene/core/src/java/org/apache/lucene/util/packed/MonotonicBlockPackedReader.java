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
package org.apache.lucene.util.packed;

import static org.apache.lucene.util.packed.AbstractBlockPackedWriter.MAX_BLOCK_SIZE;
import static org.apache.lucene.util.packed.AbstractBlockPackedWriter.MIN_BLOCK_SIZE;
import static org.apache.lucene.util.packed.PackedInts.checkBlockSize;
import static org.apache.lucene.util.packed.PackedInts.numBlocks;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Provides random access to a stream written with {@link MonotonicBlockPackedWriter}.
 *
 * @lucene.internal
 */
public class MonotonicBlockPackedReader extends LongValues implements Accountable {

  static long expected(long origin, float average, int index) {
    return origin + (long) (average * (long) index);
  }

  private static final int BLOCK_SIZE = Byte.SIZE; // #bits in a block
  private static final int BLOCK_BITS = 3; // The #bits representing BLOCK_SIZE
  private static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  final int blockShift, blockMask;
  final long valueCount;
  final long[] minValues;
  final float[] averages;
  final LongValues[] subReaders;
  final long sumBPV;
  final long totalByteCount;

  /** Sole constructor. */
  public static MonotonicBlockPackedReader of(
      IndexInput in, int packedIntsVersion, int blockSize, long valueCount) throws IOException {
    return new MonotonicBlockPackedReader(in, packedIntsVersion, blockSize, valueCount);
  }

  private MonotonicBlockPackedReader(
      IndexInput in, int packedIntsVersion, int blockSize, long valueCount) throws IOException {
    this.valueCount = valueCount;
    blockShift = checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    blockMask = blockSize - 1;
    final int numBlocks = numBlocks(valueCount, blockSize);
    minValues = new long[numBlocks];
    averages = new float[numBlocks];
    subReaders = new LongValues[numBlocks];
    long sumBPV = 0, totalByteCount = 0;
    for (int i = 0; i < numBlocks; ++i) {
      minValues[i] = in.readZLong();
      averages[i] = Float.intBitsToFloat(in.readInt());
      final int bitsPerValue = in.readVInt();
      sumBPV += bitsPerValue;
      if (bitsPerValue > 64) {
        throw new IOException("Corrupted");
      }
      if (bitsPerValue == 0) {
        subReaders[i] = LongValues.ZEROES;
      } else {
        final int size = (int) Math.min(blockSize, valueCount - (long) i * blockSize);
        final int byteCount =
            Math.toIntExact(
                PackedInts.Format.PACKED.byteCount(packedIntsVersion, size, bitsPerValue));
        totalByteCount += byteCount;
        final byte[] blocks = new byte[byteCount];
        in.readBytes(blocks, 0, byteCount);
        final long maskRight = ((1L << bitsPerValue) - 1);
        final int bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
        subReaders[i] =
            new LongValues() {
              @Override
              public long get(long index) {
                // The abstract index in a bit stream
                final long majorBitPos = index * bitsPerValue;
                // The offset of the first block in the backing byte-array
                int blockOffset = (int) (majorBitPos >>> BLOCK_BITS);
                // The number of value-bits after the first byte
                long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;
                if (endBits <= 0) {
                  // Single block
                  return ((blocks[blockOffset] & 0xFFL) >>> -endBits) & maskRight;
                }
                // Multiple blocks
                long value = ((blocks[blockOffset++] & 0xFFL) << endBits) & maskRight;
                while (endBits > BLOCK_SIZE) {
                  endBits -= BLOCK_SIZE;
                  value |= (blocks[blockOffset++] & 0xFFL) << endBits;
                }
                return value | ((blocks[blockOffset] & 0xFFL) >>> (BLOCK_SIZE - endBits));
              }
            };
      }
    }
    this.sumBPV = sumBPV;
    this.totalByteCount = totalByteCount;
  }

  @Override
  public long get(long index) {
    assert index >= 0 && index < valueCount;
    final int block = (int) (index >>> blockShift);
    final int idx = (int) (index & blockMask);
    return expected(minValues[block], averages[block], idx) + subReaders[block].get(idx);
  }

  /** Returns the number of values */
  public long size() {
    return valueCount;
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;
    sizeInBytes += RamUsageEstimator.sizeOf(minValues);
    sizeInBytes += RamUsageEstimator.sizeOf(averages);
    sizeInBytes += totalByteCount;
    return sizeInBytes;
  }

  @Override
  public String toString() {
    long avgBPV = subReaders.length == 0 ? 0 : sumBPV / subReaders.length;
    return getClass().getSimpleName()
        + "(blocksize="
        + (1 << blockShift)
        + ",size="
        + valueCount
        + ",avgBPV="
        + avgBPV
        + ")";
  }
}
