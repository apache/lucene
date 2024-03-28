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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.FixedBitSet;

/** Utility class to encode reinflate deltas and encode as bitset */
final class DenseUtil {

  /**
   * Encode 128 integer deltas from {@code longs} into {@code out} as a bitset.
   *
   * @param numBits indicates the delta between the first and last encoded docid, plus one, which is
   *     the number of bits required
   */
  static void encodeDeltas(long[] longs, int numBits, DataOutput out) throws IOException {
    FixedBitSet bits = new FixedBitSet(numBits);
    int lastSetBit = 0;
    for (long l : longs) {
      lastSetBit += l;
      bits.set(lastSetBit);
    }
    assert numBits == lastSetBit + 1;
    int numLongs = bits.getBits().length;
    int numBytes = (numBits + 7) >> 3; // 2^3 = 8 = bits/byte
    // the encoding format uses this bit
    assert numBytes < 0x80;
    out.writeByte((byte) (0x80 | numBytes));
    for (int i = 0; i < numLongs - 1; i++) {
      out.writeLong(bits.getBits()[i]);
    }
    long last = bits.getBits()[numLongs - 1];
    for (int i = 0; i < numBytes - (numLongs - 1) * Long.BYTES; i++) {
      // little-endian
      out.writeByte((byte) (last & 0xff));
      last >>>= Byte.SIZE;
    }
    /*
    if (last != 0) {
      System.out.println(Arrays.toString(longs));
      System.out.printf("lastSetBit=%d last=%x lastorig=%x numLongs=%d numBytes=%d numBits=%d\n", lastSetBit, last, bits.getBits()[numLongs - 1], numLongs, numBytes, numBits);
    }
    */
    // System.out.printf("lastSetBit=%d last=%x lastorig=%x numLongs=%d numBytes=%d numBits=%d\n",
    // lastSetBit, last, bits.getBits()[numLongs - 1], numLongs, numBytes, numBits);
    assert last == 0;
  }

  public static void readBlock(int numBytes, DataInput in, PostingBits bits) throws IOException {
    int numFullLongs = numBytes >> 3;
    int extraBytes = numBytes - numFullLongs * Long.BYTES;
    bits.resize(numBytes);
    long[] longs = bits.getBits();
    in.readLongs(longs, 0, numFullLongs);
    if (extraBytes > 0) {
      long last = 0;
      for (int shift = 0; shift < extraBytes * Byte.SIZE; shift += Byte.SIZE) {
        last |= ((long) in.readByte() & 0xff) << shift;
      }
      bits.getBits()[numFullLongs] = last;
    }
    bits.updateNumBits();
  }
}
