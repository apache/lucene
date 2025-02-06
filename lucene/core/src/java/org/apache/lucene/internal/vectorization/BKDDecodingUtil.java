package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

public class BKDDecodingUtil {

  protected final IndexInput in;

  /** Sole constructor, called by sub-classes. */
  protected BKDDecodingUtil(IndexInput in) {
    this.in = in;
  }

  public void decodeDelta16(int[] docIds, int count) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >>> 1;
    in.readInts(docIds, 0, halfLen);
    for (int i = 0; i < halfLen; ++i) {
      int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  public void decode24(int[] docIds, int[] scratch, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(scratch, 0, quarterLen3);
    for (int i = 0; i < quarterLen3; ++i) {
      docIds[i] = scratch[i] >>> 8;
    }
    for (int i = 0; i < quarterLen; i++) {
      docIds[i + quarterLen3] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarterLen] & 0xFF) << 8)
              | (scratch[i + quarterLen * 2] & 0xFF);
    }
    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(docIds, quarterLen << 2, remainder);
    }
  }
}
