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

package org.apache.lucene.sandbox.codecs.lucene90.randomaccess;

import java.util.ArrayList;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.sandbox.codecs.lucene90.randomaccess.bitpacking.BitPacker;
import org.apache.lucene.sandbox.codecs.lucene90.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestTermStateCodecImpl extends LuceneTestCase {

  public void testEncodeDecode() {
    TermStateCodecImpl codec =
        new TermStateCodecImpl(
            new TermStateCodecComponent[] {
              TermStateCodecComponent.DocFreq.INSTANCE, TermStateCodecComponent.DocStartFP.INSTANCE,
            });

    ArrayList<IntBlockTermState> termStates = new ArrayList<>();
    long maxDocFreqSeen = -1;
    long docStartFPBase = random().nextLong(Long.MAX_VALUE >> 1);
    long maxDocStartFPDeltaSeen = -1;
    for (int i = 0; i < random().nextInt(2, 256); i++) {
      var termState = new IntBlockTermState();
      termState.docFreq = random().nextInt(1, Integer.MAX_VALUE);
      if (i == 0) {
        termState.docStartFP = docStartFPBase;
      } else {
        termState.docStartFP = termStates.get(i - 1).docStartFP + random().nextLong(1024);
        maxDocStartFPDeltaSeen =
            Math.max(maxDocStartFPDeltaSeen, termState.docStartFP - docStartFPBase);
      }
      maxDocFreqSeen = Math.max(maxDocFreqSeen, termState.docFreq);
      termStates.add(termState);
    }

    IntBlockTermState[] termStatesArray = termStates.toArray(IntBlockTermState[]::new);

    BitPerBytePacker bitPerBytePacker = new BitPerBytePacker();
    byte[] metadata = codec.encodeBlock(termStatesArray, bitPerBytePacker);

    // For the metadata, we expect
    // 0: DocFreq.bitWidth,
    // 1: DocStartFP.bitWidth,
    // [2-10]: DocStartFP.referenceValue;
    assertEquals(10, metadata.length);
    assertEquals(64 - Long.numberOfLeadingZeros(maxDocFreqSeen), metadata[0]);
    assertEquals(64 - Long.numberOfLeadingZeros(maxDocStartFPDeltaSeen), metadata[1]);
    ByteArrayDataInput byteArrayDataInput = new ByteArrayDataInput(metadata, 2, 8);
    assertEquals(docStartFPBase, byteArrayDataInput.readLong());

    // Assert that each term state is the same after the encode-decode roundtrip.
    BytesRef metadataBytes = new BytesRef(metadata);
    BytesRef dataBytes = new BytesRef(bitPerBytePacker.getBytes());
    for (int i = 0; i < termStatesArray.length; i++) {
      IntBlockTermState decoded =
          codec.decodeWithinBlock(metadataBytes, dataBytes, bitPerBytePacker, i);
      assertEquals(termStatesArray[i].docFreq, decoded.docFreq);
      assertEquals(termStatesArray[i].docStartFP, decoded.docStartFP);
    }
  }
}

/**
 * A wasteful bit packer that use whole byte to keep a bit. Useful for tests. It uses little-endian
 * bit order.
 */
class BitPerBytePacker implements BitPacker, BitUnpacker {
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

  @Override
  public long unpack(BytesRef bytesRef, int startBitIndex, int bitWidth) {
    long res = 0;
    for (int i = 0; i < bitWidth; i++) {
      res |= ((long) (bytesRef.bytes[bytesRef.offset + startBitIndex + i] & 1)) << i;
    }
    return res;
  }
}
