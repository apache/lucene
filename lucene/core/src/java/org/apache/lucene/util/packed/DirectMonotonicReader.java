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

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectMonotonicWriter}.
 *
 * @see DirectMonotonicWriter
 */
public abstract sealed class DirectMonotonicReader extends LongValues
    permits DirectMonotonicReader.MultiBlockDirectMonotonicReader,
        DirectMonotonicReader.SingleBlockDirectMonotonicReader {

  private static final Meta SINGLE_ZERO_BLOCK = new SingleBlockMeta(0L, 0.0f, (byte) 0, 0L);

  /**
   * In-memory metadata that needs to be kept around for {@link DirectMonotonicReader} to read data
   * from disk.
   */
  public abstract static sealed class Meta permits SingleBlockMeta, MultiBlockMeta {

    protected abstract DirectMonotonicReader getInstance(RandomAccessInput data, boolean merging);
  }

  /** Get lower/upper bounds for the value at a given index without hitting the direct reader. */
  protected abstract long[] getBounds(long mid);

  /**
   * Return the index of a key if it exists, or its insertion point otherwise like {@link
   * Arrays#binarySearch(long[], int, int, long)}.
   *
   * @see Arrays#binarySearch(long[], int, int, long)
   */
  public final long binarySearch(long fromIndex, long toIndex, long key) {
    if (fromIndex < 0 || fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex=" + fromIndex + ",toIndex=" + toIndex);
    }
    long lo = fromIndex;
    long hi = toIndex - 1;

    while (lo <= hi) {
      final long mid = (lo + hi) >>> 1;
      // Try to run as many iterations of the binary search as possible without
      // hitting the direct readers, since they might hit a page fault.
      final long[] bounds = getBounds(mid);
      if (bounds[1] < key) {
        lo = mid + 1;
      } else if (bounds[0] > key) {
        hi = mid - 1;
      } else {
        final long midVal = get(mid);
        if (midVal < key) {
          lo = mid + 1;
        } else if (midVal > key) {
          hi = mid - 1;
        } else {
          return mid;
        }
      }
    }
    return -1 - lo;
  }

  /**
   * Load metadata from the given {@link IndexInput}.
   *
   * @see DirectMonotonicReader#getInstance(Meta, RandomAccessInput)
   */
  public static Meta loadMeta(IndexInput metaIn, long numValues, int blockShift)
      throws IOException {
    final int numBlocks = numBlocks(numValues, blockShift);
    if (numBlocks == 1) {
      return loadSingleBlockMeta(metaIn);
    } else {
      return loadMultiBlockMeta(metaIn, numBlocks, blockShift);
    }
  }

  private static int numBlocks(long numValues, int blockShift) {
    long numBlocks = numValues >>> blockShift;
    if ((numBlocks << blockShift) < numValues) {
      numBlocks += 1;
    }
    return (int) numBlocks;
  }

  private static Meta loadSingleBlockMeta(IndexInput metaIn) throws IOException {
    final long min = metaIn.readLong();
    final float avgInt = Float.intBitsToFloat(metaIn.readInt());
    final long offsets = metaIn.readLong();
    final byte bpvs = metaIn.readByte();
    final boolean allValuesZero = min == 0L && avgInt == 0 && bpvs == 0;
    // save heap in case all values are zero
    return allValuesZero ? SINGLE_ZERO_BLOCK : new SingleBlockMeta(min, avgInt, bpvs, offsets);
  }

  private static Meta loadMultiBlockMeta(IndexInput metaIn, int numBlocks, int blockShift)
      throws IOException {
    final MultiBlockMeta meta = new MultiBlockMeta(numBlocks, blockShift);
    boolean allValuesZero = true;
    for (int i = 0; i < numBlocks; ++i) {
      final long min = metaIn.readLong();
      meta.mins[i] = min;
      final int avgInt = metaIn.readInt();
      meta.avgs[i] = Float.intBitsToFloat(avgInt);
      meta.offsets[i] = metaIn.readLong();
      final byte bpvs = metaIn.readByte();
      meta.bpvs[i] = bpvs;
      allValuesZero = allValuesZero && min == 0L && avgInt == 0 && bpvs == 0;
    }
    // save heap in case all values are zero
    return allValuesZero ? SINGLE_ZERO_BLOCK : meta;
  }

  /** Retrieves a non-merging instance from the specified slice. */
  public static DirectMonotonicReader getInstance(Meta meta, RandomAccessInput data)
      throws IOException {
    return getInstance(meta, data, false);
  }

  /** Retrieves an instance from the specified slice. */
  public static DirectMonotonicReader getInstance(
      Meta meta, RandomAccessInput data, boolean merging) {
    return meta.getInstance(data, merging);
  }

  private static final class SingleBlockMeta extends Meta {
    private final long min;
    private final float avg;
    private final byte bpv;
    private final long offset;

    private SingleBlockMeta(long min, float avg, byte bpv, long offset) {
      this.min = min;
      this.avg = avg;
      this.bpv = bpv;
      this.offset = offset;
    }

    @Override
    protected DirectMonotonicReader getInstance(RandomAccessInput data, boolean merging) {
      LongValues reader;
      if (bpv == 0) {
        reader = LongValues.ZEROES;
      } else {
        reader = DirectReader.getInstance(data, bpv, offset);
      }
      return new SingleBlockDirectMonotonicReader(reader, min, avg, bpv);
    }
  }

  /** A DirectMonotonicReader that reads from a single block. */
  protected static final class SingleBlockDirectMonotonicReader extends DirectMonotonicReader {
    private final LongValues reader;
    private final long min;
    private final float avg;
    private final byte bpv;

    private SingleBlockDirectMonotonicReader(LongValues reader, long min, float avg, byte bpv) {
      this.reader = reader;
      this.min = min;
      this.avg = avg;
      this.bpv = bpv;
    }

    @Override
    public long get(long index) {
      final long delta = reader.get(index);
      return min + (long) (avg * index) + delta;
    }

    @Override
    protected long[] getBounds(long index) {
      final long lowerBound = min + (long) (avg * index);
      final long upperBound = lowerBound + (1L << bpv) - 1;
      if (bpv == 64 || upperBound < lowerBound) { // overflow
        return new long[] {Long.MIN_VALUE, Long.MAX_VALUE};
      } else {
        return new long[] {lowerBound, upperBound};
      }
    }
  }

  private static final class MultiBlockMeta extends Meta {
    private final int blockShift;
    private final long[] mins;
    private final float[] avgs;
    private final byte[] bpvs;
    private final long[] offsets;

    private MultiBlockMeta(int numBlocks, int blockShift) {
      this.blockShift = blockShift;
      this.mins = new long[numBlocks];
      this.avgs = new float[numBlocks];
      this.bpvs = new byte[numBlocks];
      this.offsets = new long[numBlocks];
    }

    @Override
    protected DirectMonotonicReader getInstance(RandomAccessInput data, boolean merging) {
      final int numBlocks = mins.length;
      final LongValues[] readers = new LongValues[numBlocks];
      for (int i = 0; i < numBlocks; ++i) {
        if (bpvs[i] == 0) {
          readers[i] = LongValues.ZEROES;
        } else if (merging
            && i < numBlocks - 1 // we only know the number of values for the last block
            && blockShift >= DirectReader.MERGE_BUFFER_SHIFT) {
          readers[i] = DirectReader.getMergeInstance(data, bpvs[i], offsets[i], 1L << blockShift);
        } else {
          readers[i] = DirectReader.getInstance(data, bpvs[i], offsets[i]);
        }
      }
      return new MultiBlockDirectMonotonicReader(blockShift, readers, mins, avgs, bpvs);
    }
  }

  /** A DirectMonotonicReader that reads from multiple blocks. */
  protected static final class MultiBlockDirectMonotonicReader extends DirectMonotonicReader {
    private final int blockShift;
    private final long blockMask;
    private final LongValues[] readers;
    private final long[] mins;
    private final float[] avgs;
    private final byte[] bpvs;

    private MultiBlockDirectMonotonicReader(
        int blockShift, LongValues[] readers, long[] mins, float[] avgs, byte[] bpvs) {
      this.blockMask = (1L << blockShift) - 1;
      this.blockShift = blockShift;
      this.readers = readers;
      this.mins = mins;
      this.avgs = avgs;
      this.bpvs = bpvs;
      if (readers.length != mins.length
          || readers.length != avgs.length
          || readers.length != bpvs.length) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public long get(long index) {
      final int block = (int) (index >>> blockShift);
      final long blockIndex = index & blockMask;
      final long delta = readers[block].get(blockIndex);
      return mins[block] + (long) (avgs[block] * blockIndex) + delta;
    }

    @Override
    protected long[] getBounds(long index) {
      final int block = Math.toIntExact(index >>> blockShift);
      final long blockIndex = index & blockMask;
      final long lowerBound = mins[block] + (long) (avgs[block] * blockIndex);
      final long upperBound = lowerBound + (1L << bpvs[block]) - 1;
      if (bpvs[block] == 64 || upperBound < lowerBound) { // overflow
        return new long[] {Long.MIN_VALUE, Long.MAX_VALUE};
      } else {
        return new long[] {lowerBound, upperBound};
      }
    }
  }
}
