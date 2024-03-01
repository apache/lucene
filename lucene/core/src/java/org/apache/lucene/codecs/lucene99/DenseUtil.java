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
import java.util.Arrays;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.FixedBitSet;

/** Utility class to encode 128 bits ha ha */
final class DenseUtil {

  /**
   * Encode 128 integer deltas from {@code longs} into {@code out} as a bitset.
   *
   * @param numBits indicates the delta between the first and last encoded docid, which is the
   *     number of bits required
   */
  static void encodeDeltas(long[] longs, int numBits, DataOutput out) throws IOException {
    FixedBitSet bits = new FixedBitSet(numBits);
    int lastSetBit = 0;
    for (long l : longs) {
      lastSetBit += l;
      bits.set(lastSetBit);
    }
    int numLongs = bits.getBits().length;
    // the encoding format uses this bit
    assert numLongs < 0x80;
    // nocommit: we are wastefully writing full longs when we could skip the trailing zero bytes
    // instead we should write only the bytes we need and write that number rather than numLongs
    // when we read we will have to reconstruct the trailiing partial long
    out.writeByte((byte) (0x80 | numLongs));
    assert numBits == lastSetBit + 1;
    // out.writeByte((byte) (nBytes & 0xff));
    for (long l : bits.getBits()) {
      out.writeLong(l);
    }
  }

  public static void readBlock(int bitsPerValue, DataInput in, FixedBitSet bits)
      throws IOException {
    int numLongs = bitsPerValue & 0x7f;
    long[] longs = bits.getBits();
    in.readLongs(longs, 0, numLongs);
    // empty out the "ghost bits" so FixedBitSet doesn't get upset
    Arrays.fill(longs, numLongs, longs.length, 0);
  }
}
