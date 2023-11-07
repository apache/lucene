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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.util.ArrayList;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitPerBytePacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpackerImpl;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.FixedSizeByteArrayBitPacker;
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
      termState.docFreq = random().nextInt(1, 1 << random().nextInt(1, 31));
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
    int expectedDocFreqBitWidth = 64 - Long.numberOfLeadingZeros(maxDocFreqSeen);
    int expectedDocStartFPBitWidth = 64 - Long.numberOfLeadingZeros(maxDocStartFPDeltaSeen);
    assertEquals(10, metadata.length);
    assertEquals(expectedDocFreqBitWidth, metadata[0]);
    assertEquals(expectedDocStartFPBitWidth, metadata[1]);
    ByteArrayDataInput byteArrayDataInput = new ByteArrayDataInput(metadata, 2, 8);
    assertEquals(docStartFPBase, byteArrayDataInput.readLong());

    // Assert with real bit-packer we get the same bytes
    FixedSizeByteArrayBitPacker fixedSizeByteArrayBitPacker =
        new FixedSizeByteArrayBitPacker(bitPerBytePacker.getCompactBytes().length);
    codec.encodeBlock(termStatesArray, fixedSizeByteArrayBitPacker);
    assertArrayEquals(bitPerBytePacker.getCompactBytes(), fixedSizeByteArrayBitPacker.getBytes());

    // Assert that each term state is the same after the encode-decode roundtrip.
    BytesRef metadataBytes = new BytesRef(metadata);
    BytesRef dataBytes = new BytesRef(bitPerBytePacker.getBytes());
    assertBlockRoundTrip(termStatesArray, codec, metadataBytes, dataBytes, bitPerBytePacker);

    // With real compact bits instead of bit-per-byte
    dataBytes = new BytesRef(bitPerBytePacker.getCompactBytes());
    assertBlockRoundTrip(
        termStatesArray, codec, metadataBytes, dataBytes, BitUnpackerImpl.INSTANCE);

    // Also test decoding that doesn't begin at the start of the block.
    int pos = random().nextInt(termStatesArray.length);
    int startBitIndex = pos > 0 ? random().nextInt(pos) : 0;
    int recordSize = expectedDocFreqBitWidth + expectedDocStartFPBitWidth;
    // With bit-per-byte bytes
    dataBytes =
        new BytesRef(bitPerBytePacker.getBytes(), pos * recordSize - startBitIndex, recordSize);
    assertDecodeAt(
        codec, metadataBytes, dataBytes, bitPerBytePacker, startBitIndex, termStatesArray[pos]);

    // With compact bytes
    int startByteIndex = pos * recordSize / 8;
    int endByteIndex = (pos + 1) * recordSize / 8;
    int length = endByteIndex - startByteIndex + ((pos + 1) * recordSize % 8 == 0 ? 0 : 1);
    dataBytes = new BytesRef(bitPerBytePacker.getCompactBytes(), startByteIndex, length);
    assertDecodeAt(
        codec,
        metadataBytes,
        dataBytes,
        BitUnpackerImpl.INSTANCE,
        (pos * recordSize) % 8,
        termStatesArray[pos]);
  }

  private static void assertDecodeAt(
      TermStateCodecImpl codec,
      BytesRef metadataBytes,
      BytesRef dataBytes,
      BitUnpacker bitUnpacker,
      int startBitIndex,
      IntBlockTermState termState) {
    IntBlockTermState decoded =
        codec.decodeAt(metadataBytes, dataBytes, bitUnpacker, startBitIndex);
    assertEquals(termState.docFreq, decoded.docFreq);
    assertEquals(termState.docStartFP, decoded.docStartFP);
  }

  private static void assertBlockRoundTrip(
      IntBlockTermState[] termStatesArray,
      TermStateCodecImpl codec,
      BytesRef metadataBytes,
      BytesRef dataBytes,
      BitUnpacker bitUnpacker) {
    for (int i = 0; i < termStatesArray.length; i++) {
      IntBlockTermState decoded = codec.decodeWithinBlock(metadataBytes, dataBytes, bitUnpacker, i);
      assertEquals(termStatesArray[i].docFreq, decoded.docFreq);
      assertEquals(termStatesArray[i].docStartFP, decoded.docStartFP);
    }
  }
}
