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

package org.apache.lucene.sandbox.codecs.lucene90.randomaccess.bitpacking;

import java.util.ArrayList;
import org.apache.lucene.util.BytesRef;

/**
 * A wasteful bit packer that use whole byte to keep a bit. Useful for tests. It uses little-endian
 * bit order.
 */
public class BitPerBytePacker implements BitPacker, BitUnpacker {
  private final ArrayList<Byte> buffer = new ArrayList<>();

  private int totalNumBits = 0;

  @Override
  public void add(long value, int numBits) {
    assert numBits < 64;
    totalNumBits += numBits;
    while (numBits-- > 0) {
      byte b = (byte) (value & 1L);
      value = value >>> 1;
      buffer.add(b);
    }
  }

  public byte[] getBytes() {
    byte[] bytes = new byte[totalNumBits];
    int index = 0;
    for (var b : buffer) {
      bytes[index++] = b;
    }

    return bytes;
  }

  public byte[] getCompactBytes() {
    int len = (totalNumBits - 1) / 8 + 1; // round up
    byte[] bytes = new byte[len];

    int remainingBits = totalNumBits;
    int pos = 0;
    while (remainingBits >= 8) {
      byte b = 0;
      int base = pos * 8;
      for (int i = 0; i < 8; i++) {
        b |= (byte) ((buffer.get(base + i) & 1) << i);
      }
      bytes[pos++] = b;
      remainingBits -= 8;
    }

    if (remainingBits > 0) {
      byte b = 0;
      int base = pos * 8;
      for (int i = 0; i < remainingBits; i++) {
        b |= (byte) ((buffer.get(base + i) & 1) << i);
      }
      bytes[pos] = b;
    }

    return bytes;
  }

  @Override
  public long unpack(BytesRef bytesRef, int startBitIndex, int bitWidth) {
    long res = 0;
    for (int i = 0; i < bitWidth; i++) {
      res |= ((long) (bytesRef.bytes[bytesRef.offset + startBitIndex + i] & 1)) << i;
    }
    return res;
  }
}
