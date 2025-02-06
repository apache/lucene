package org.apache.lucene.benchmark.jmh;


import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.internal.vectorization.BKDDecodingUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;


public class BKDVectorDecodingUtil extends BKDDecodingUtil {

  private static final VectorSpecies<Integer> INT_SPECIES =
      PanamaVectorConstants.PRERERRED_INT_SPECIES;

  private final MemorySegment memorySegment;

  BKDDocIdDecodingUtil(IndexInput in) {
    super(in);
    if (in instanceof MemorySegmentAccessInput msai) {
      this.segment = msai.segmentSliceOrNull(0, input.length());
    } else {
      this.segment = null;
    }
  }

  @Override
  public void decodeDelta16(int[] docIds, int count) throws IOException {
    if (memorySegment == null) {
      decode16Default(docIds, count);
    } else {
      decode16Segment(docIds, count);
    }
  }

  private void decode16Default(int[] docIds, int count) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >>> 1;
    in.readInts(docIds, 0, halfLen);
    int upperBound = INT_SPECIES.loopBound(halfLen);
    int i = 0;
    for (; i < upperBound; i += INT_SPECIES.length()) {
      IntVector vector = IntVector.fromArray(INT_SPECIES, docIds, i);
      vector
          .lanewise(VectorOperators.LSHR, 16)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i);
      vector
          .lanewise(VectorOperators.AND, 0xFFFF)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i + halfLen);
    }
    for (; i < halfLen; ++i) {
      int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  private void decode16Segment(int[] docIds, int count) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >>> 1;
    int upperBound = INT_SPECIES.loopBound(halfLen - 1);
    long offset = in.getFilePointer();
    final long endOffset = offset + (long) halfLen * Integer.BYTES;
    final long step = (long) INT_SPECIES.length() * Integer.BYTES;
    for (int i = 0; i < upperBound; i += INT_SPECIES.length(), offset += step) {
      IntVector vector =
          IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
      vector
          .lanewise(VectorOperators.LSHR, 16)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i);
      vector
          .lanewise(VectorOperators.AND, 0xFFFF)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i + halfLen);
    }

    int i = halfLen - INT_SPECIES.length();
    IntVector vector =
        IntVector.fromMemorySegment(
            INT_SPECIES, memorySegment, endOffset - step, ByteOrder.LITTLE_ENDIAN);
    vector
        .lanewise(VectorOperators.LSHR, 16)
        .lanewise(VectorOperators.ADD, min)
        .intoArray(docIds, i);
    vector
        .lanewise(VectorOperators.AND, 0xFFFF)
        .lanewise(VectorOperators.ADD, min)
        .intoArray(docIds, i + halfLen);

    in.seek(endOffset);
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  @Override
  public void decode24(int[] docIds, int[] scratch, int count) throws IOException {
    if (memorySegment == null) {
      decode24Default(docIds, scratch, count);
    } else {
      decode24Segment(docIds, count);
    }
  }

  private void decode24Default(int[] outputInts, int[] scratch, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int halfLen = quarterLen << 1;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(scratch, 0, quarterLen3);
    int upperBound = INT_SPECIES.loopBound(quarterLen3);
    int i = 0;
    for (; i < upperBound; i += INT_SPECIES.length()) {
      IntVector.fromArray(INT_SPECIES, scratch, i)
          .lanewise(VectorOperators.LSHR, 8)
          .intoArray(outputInts, i);
    }
    for (; i < quarterLen3; ++i) {
      outputInts[i] = scratch[i] >>> 8;
    }

    i = 0;
    int upperBound2 = INT_SPECIES.loopBound(quarterLen);
    for (; i < upperBound2; i += INT_SPECIES.length()) {
      IntVector.fromArray(INT_SPECIES, scratch, i)
          .lanewise(VectorOperators.AND, 0xFF)
          .lanewise(VectorOperators.LSHL, 16)
          .lanewise(
              VectorOperators.OR,
              IntVector.fromArray(INT_SPECIES, scratch, i + quarterLen)
                  .lanewise(VectorOperators.AND, 0xFF)
                  .lanewise(VectorOperators.LSHL, 8))
          .lanewise(
              VectorOperators.OR,
              IntVector.fromArray(INT_SPECIES, scratch, i + halfLen)
                  .lanewise(VectorOperators.AND, 0xFF))
          .intoArray(outputInts, quarterLen3 + i);
    }
    for (; i < quarterLen; i++) {
      outputInts[i + quarterLen3] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarterLen] & 0xFF) << 8)
              | (scratch[i + quarterLen * 2] & 0xFF);
    }

    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(outputInts, quarterLen << 2, remainder);
    }
  }

  private void decode24Segment(int[] outputInts, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int halfLen = quarterLen << 1;
    final int quarterLen3 = quarterLen * 3;

    final int upperBound = INT_SPECIES.loopBound(quarterLen3 - 1);
    long offset = in.getFilePointer();
    final long endOffset = offset + (long) quarterLen3 * Integer.BYTES;
    final long step = (long) INT_SPECIES.length() * Integer.BYTES;
    for (int i = 0; i < upperBound; i += INT_SPECIES.length(), offset += step) {
      IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN)
          .lanewise(VectorOperators.LSHR, 8)
          .intoArray(outputInts, i);
    }
    int i = quarterLen3 - INT_SPECIES.length();
    IntVector.fromMemorySegment(
            INT_SPECIES, memorySegment, endOffset - step, ByteOrder.LITTLE_ENDIAN)
        .lanewise(VectorOperators.LSHR, 8)
        .intoArray(outputInts, i);

    final int upperBound2 = INT_SPECIES.loopBound(quarterLen - 1);
    final int gap1 = quarterLen * Integer.BYTES;
    final int gap2 = halfLen * Integer.BYTES;
    for (i = 0; i < upperBound2; i += INT_SPECIES.length()) {
      IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN)
          .lanewise(VectorOperators.AND, 0xFF)
          .lanewise(VectorOperators.LSHL, 16)
          .lanewise(
              VectorOperators.OR,
              IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset + gap1, ByteOrder.LITTLE_ENDIAN)
                  .lanewise(VectorOperators.AND, 0xFF)
                  .lanewise(VectorOperators.LSHL, 8))
          .lanewise(
              VectorOperators.OR,
              IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset + gap2, ByteOrder.LITTLE_ENDIAN)
                  .lanewise(VectorOperators.AND, 0xFF))
          .intoArray(outputInts, quarterLen3 + i);
    }

    i = quarterLen - INT_SPECIES.length();
    offset = endOffset - step;
    IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN)
        .lanewise(VectorOperators.AND, 0xFF)
        .lanewise(VectorOperators.LSHL, 16)
        .lanewise(
            VectorOperators.OR,
            IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset + gap1, ByteOrder.LITTLE_ENDIAN)
                .lanewise(VectorOperators.AND, 0xFF)
                .lanewise(VectorOperators.LSHL, 8))
        .lanewise(
            VectorOperators.OR,
            IntVector.fromMemorySegment(INT_SPECIES, memorySegment, offset + gap2, ByteOrder.LITTLE_ENDIAN)
                .lanewise(VectorOperators.AND, 0xFF))
        .intoArray(outputInts, quarterLen3 + i);

    in.seek(endOffset);
    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(outputInts, quarterLen << 2, remainder);
    }
  }
}
