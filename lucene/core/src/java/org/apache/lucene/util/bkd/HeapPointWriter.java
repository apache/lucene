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
package org.apache.lucene.util.bkd;

import java.util.Arrays;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class to write new points into in-heap arrays.
 *
 * @lucene.internal
 */
public final class HeapPointWriter implements PointWriter {
  private final byte[] block;
  final int size;
  private final BKDConfig config;
  private final byte[] scratch;
  private final ArrayUtil.ByteArrayComparator dimComparator;
  // length is composed by the data dimensions plus the docID
  private final int dataDimsAndDocLength;
  private int nextWrite;
  private boolean closed;
  private final HeapPointValue pointValue;

  public HeapPointWriter(BKDConfig config, int size) {
    this.config = config;
    this.block = new byte[config.bytesPerDoc * size];
    this.size = size;
    this.dimComparator = ArrayUtil.getUnsignedComparator(config.bytesPerDim);
    this.dataDimsAndDocLength = config.bytesPerDoc - config.packedIndexBytesLength;
    this.scratch = new byte[config.bytesPerDoc];
    if (size > 0) {
      pointValue = new HeapPointValue(config, block);
    } else {
      // no values
      pointValue = null;
    }
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public PointValue getPackedValueSlice(int index) {
    assert index < nextWrite : "nextWrite=" + (nextWrite) + " vs index=" + index;
    pointValue.setOffset(index * config.bytesPerDoc);
    return pointValue;
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false : "point writer is already closed";
    assert packedValue.length == config.packedBytesLength
        : "[packedValue] must have length ["
            + config.packedBytesLength
            + "] but was ["
            + packedValue.length
            + "]";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    final int position = nextWrite * config.bytesPerDoc;
    System.arraycopy(packedValue, 0, block, position, config.packedBytesLength);
    BitUtil.VH_BE_INT.set(block, position + config.packedBytesLength, docID);
    nextWrite++;
  }

  @Override
  public void append(PointValue pointValue) {
    assert closed == false : "point writer is already closed";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    final BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
    assert packedValueDocID.length == config.bytesPerDoc
        : "[packedValue] must have length ["
            + (config.bytesPerDoc)
            + "] but was ["
            + packedValueDocID.length
            + "]";
    final int position = nextWrite * config.bytesPerDoc;
    System.arraycopy(
        packedValueDocID.bytes, packedValueDocID.offset, block, position, config.bytesPerDoc);
    nextWrite++;
  }

  /** Swaps the point at point {@code i} with the point at position {@code j} */
  void swap(int i, int j) {
    final int indexI = i * config.bytesPerDoc;
    final int indexJ = j * config.bytesPerDoc;
    // scratch1 = values[i]
    System.arraycopy(block, indexI, scratch, 0, config.bytesPerDoc);
    // values[i] = values[j]
    System.arraycopy(block, indexJ, block, indexI, config.bytesPerDoc);
    // values[j] = scratch1
    System.arraycopy(scratch, 0, block, indexJ, config.bytesPerDoc);
  }

  /** Return the byte at position {@code k} of the point at position {@code i} */
  int byteAt(int i, int k) {
    return block[i * config.bytesPerDoc + k] & 0xff;
  }

  /**
   * Copy the dimension {@code dim} of the point at position {@code i} in the provided {@code bytes}
   * at the given offset
   */
  void copyDim(int i, int dim, byte[] bytes, int offset) {
    System.arraycopy(block, i * config.bytesPerDoc + dim, bytes, offset, config.bytesPerDim);
  }

  /**
   * Copy the data dimensions and doc value of the point at position {@code i} in the provided
   * {@code bytes} at the given offset
   */
  void copyDataDimsAndDoc(int i, byte[] bytes, int offset) {
    System.arraycopy(
        block,
        i * config.bytesPerDoc + config.packedIndexBytesLength,
        bytes,
        offset,
        dataDimsAndDocLength);
  }

  /**
   * Compares the dimension {@code dim} value of the point at position {@code i} with the point at
   * position {@code j}
   */
  int compareDim(int i, int j, int dim) {
    final int iOffset = i * config.bytesPerDoc + dim;
    final int jOffset = j * config.bytesPerDoc + dim;
    return compareDim(block, iOffset, block, jOffset);
  }

  /**
   * Compares the dimension {@code dim} value of the point at position {@code j} with the provided
   * value
   */
  int compareDim(int j, byte[] dimValue, int offset, int dim) {
    final int jOffset = j * config.bytesPerDoc + dim;
    return compareDim(dimValue, offset, block, jOffset);
  }

  private int compareDim(byte[] blockI, int offsetI, byte[] blockJ, int offsetJ) {
    return dimComparator.compare(blockI, offsetI, blockJ, offsetJ);
  }

  /**
   * Compares the data dimensions and doc values of the point at position {@code i} with the point
   * at position {@code j}
   */
  int compareDataDimsAndDoc(int i, int j) {
    final int iOffset = i * config.bytesPerDoc + config.packedIndexBytesLength;
    final int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
    return compareDataDimsAndDoc(block, iOffset, block, jOffset);
  }

  /**
   * Compares the data dimensions and doc values of the point at position {@code j} with the
   * provided value
   */
  int compareDataDimsAndDoc(int j, byte[] dataDimsAndDocs, int offset) {
    final int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
    return compareDataDimsAndDoc(dataDimsAndDocs, offset, block, jOffset);
  }

  private int compareDataDimsAndDoc(byte[] blockI, int offsetI, byte[] blockJ, int offsetJ) {
    return Arrays.compareUnsigned(
        blockI,
        offsetI,
        offsetI + dataDimsAndDocLength,
        blockJ,
        offsetJ,
        offsetJ + dataDimsAndDocLength);
  }

  /** Computes the cardinality of the points between {@code from} tp {@code to} */
  public int computeCardinality(int from, int to, int[] commonPrefixLengths) {
    int leafCardinality = 1;
    for (int i = from + 1; i < to; i++) {
      final int pointOffset = (i - 1) * config.bytesPerDoc;
      final int nextPointOffset = pointOffset + config.bytesPerDoc;
      for (int dim = 0; dim < config.numDims; dim++) {
        final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * config.bytesPerDim + config.bytesPerDim;
        if (Arrays.mismatch(
                block,
                nextPointOffset + start,
                nextPointOffset + end,
                block,
                pointOffset + start,
                pointOffset + end)
            != -1) {
          leafCardinality++;
          break;
        }
      }
    }
    return leafCardinality;
  }

  @Override
  public long count() {
    return nextWrite;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert closed : "point writer is still open and trying to get a reader";
    assert start + length <= size
        : "start=" + start + " length=" + length + " docIDs.length=" + size;
    assert start + length <= nextWrite
        : "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    return new HeapPointReader(
        this::getPackedValueSlice, (int) start, Math.toIntExact(start + length));
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public void destroy() {}

  @Override
  public String toString() {
    return "HeapPointWriter(count=" + nextWrite + " size=" + size + ")";
  }

  /** Reusable implementation for a point value on-heap */
  private static class HeapPointValue implements PointValue {

    private final BytesRef packedValue;
    private final BytesRef packedValueDocID;
    private final int packedValueLength;

    HeapPointValue(BKDConfig config, byte[] value) {
      this.packedValueLength = config.packedBytesLength;
      this.packedValue = new BytesRef(value, 0, packedValueLength);
      this.packedValueDocID = new BytesRef(value, 0, config.bytesPerDoc);
    }

    /** Sets a new value by changing the offset. */
    void setOffset(int offset) {
      packedValue.offset = offset;
      packedValueDocID.offset = offset;
    }

    @Override
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      int position = packedValueDocID.offset + packedValueLength;
      return (int) BitUtil.VH_BE_INT.get(packedValueDocID.bytes, position);
    }

    @Override
    public BytesRef packedValueDocIDBytes() {
      return packedValueDocID;
    }
  }
}
