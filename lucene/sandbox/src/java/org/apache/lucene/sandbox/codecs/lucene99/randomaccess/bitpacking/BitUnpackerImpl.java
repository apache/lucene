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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking;

import org.apache.lucene.util.BytesRef;

/** Implementation of {@link BitUnpacker} that works with compactly packed bits */
public class BitUnpackerImpl implements BitUnpacker {
  public static BitUnpackerImpl INSTANCE = new BitUnpackerImpl();

  private BitUnpackerImpl() {}

  @Override
  public long unpack(BytesRef bytesRef, int startBitIndex, int bitWidth) {
    assert (startBitIndex + bitWidth) <= bytesRef.length * 8;
    assert bitWidth < 64;

    int firstByteIndex = startBitIndex / 8;
    int numBitsToExcludeInFirstByte = startBitIndex % 8;
    int lastByteIndex = (startBitIndex + bitWidth) / 8;
    int numBitsToKeepInLastByte = (startBitIndex + bitWidth) % 8;

    /*
     *  idea: there are two cases
     *  (1) when the requests bits are within the same byte; e.g. startBitIndex = 1, bitWidth = 5
     *  (2) when the requests bits span across many bytes; e.g. startBitIndex = 1, bitWidth = 15
     *  For (1) it is trivial,
     *  for (2) we can
     *  (2.1) read first partial bytes
     *  (2.2) read full bytes for those whose index is in (first, last), exclusive.
     *  (2.3) read the last partial bytes ( can be empty )
     */

    // case (1)
    if (firstByteIndex == lastByteIndex) {
      long res = Byte.toUnsignedLong(bytesRef.bytes[bytesRef.offset + firstByteIndex]);
      res &= (1L << numBitsToKeepInLastByte) - 1;
      res >>>= numBitsToExcludeInFirstByte;
      return res;
    }

    // case (2)
    long res = 0;
    int totalNumBitsRead = 0;
    // (2.1) read first partial bytes
    res |=
        Byte.toUnsignedLong(bytesRef.bytes[bytesRef.offset + firstByteIndex])
            >>> numBitsToExcludeInFirstByte;
    totalNumBitsRead += 8 - numBitsToExcludeInFirstByte;
    // (2.2) read full bytes for whose index is in (first, last), exclusive.
    for (int byteIndex = firstByteIndex + 1; byteIndex < lastByteIndex; byteIndex++) {
      res |= Byte.toUnsignedLong(bytesRef.bytes[bytesRef.offset + byteIndex]) << totalNumBitsRead;
      totalNumBitsRead += 8;
    }
    // (2.3) read the last partial bytes ( can be empty )
    if (numBitsToKeepInLastByte > 0) {
      long partial =
          Byte.toUnsignedLong(bytesRef.bytes[bytesRef.offset + lastByteIndex])
              & ((1L << numBitsToKeepInLastByte) - 1);
      res |= partial << totalNumBitsRead;
    }

    return res;
  }
}
