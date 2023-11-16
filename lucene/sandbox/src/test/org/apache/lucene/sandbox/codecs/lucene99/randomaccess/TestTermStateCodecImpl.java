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

import java.io.IOException;
import java.util.ArrayList;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitPerBytePacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpackerImpl;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.FixedSizeByteArrayBitPacker;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestTermStateCodecImpl extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    TermStateTestFixture result = getTermStateTestFixture(256);

    BitPerBytePacker bitPerBytePacker = new BitPerBytePacker();
    byte[] metadata = result.codec().encodeBlock(result.termStatesArray(), bitPerBytePacker);

    // For the metadata, we expect
    // 0: DocFreq.bitWidth,
    // 1: DocStartFP.bitWidth,
    // [2-10]: DocStartFP.referenceValue;
    int expectedDocFreqBitWidth = 64 - Long.numberOfLeadingZeros(result.maxDocFreqSeen());
    int expectedDocStartFPBitWidth =
        64 - Long.numberOfLeadingZeros(result.maxDocStartFPDeltaSeen());
    assertEquals(10, metadata.length);
    assertEquals(expectedDocFreqBitWidth, metadata[0]);
    assertEquals(expectedDocStartFPBitWidth, metadata[1]);
    ByteArrayDataInput byteArrayDataInput = new ByteArrayDataInput(metadata, 2, 8);
    assertEquals(result.docStartFPBase(), byteArrayDataInput.readLong());

    // Assert with real bit-packer we get the same bytes
    FixedSizeByteArrayBitPacker fixedSizeByteArrayBitPacker =
        new FixedSizeByteArrayBitPacker(bitPerBytePacker.getCompactBytes().length);
    result.codec().encodeBlock(result.termStatesArray(), fixedSizeByteArrayBitPacker);
    assertArrayEquals(bitPerBytePacker.getCompactBytes(), fixedSizeByteArrayBitPacker.getBytes());

    // Assert that each term state is the same after the encode-decode roundtrip.
    BytesRef metadataBytes = new BytesRef(metadata);
    BytesRef dataBytes = new BytesRef(bitPerBytePacker.getBytes());
    assertBlockRoundTrip(
        result.termStatesArray(), result.codec(), metadataBytes, dataBytes, bitPerBytePacker);

    // With real compact bits instead of bit-per-byte
    dataBytes = new BytesRef(bitPerBytePacker.getCompactBytes());
    assertBlockRoundTrip(
        result.termStatesArray(),
        result.codec(),
        metadataBytes,
        dataBytes,
        BitUnpackerImpl.INSTANCE);

    // Also test decoding that doesn't begin at the start of the block.
    int pos = random().nextInt(result.termStatesArray().length);
    int startBitIndex = pos > 0 ? random().nextInt(pos) : 0;
    int recordSize = expectedDocFreqBitWidth + expectedDocStartFPBitWidth;
    // With bit-per-byte bytes
    dataBytes =
        new BytesRef(bitPerBytePacker.getBytes(), pos * recordSize - startBitIndex, recordSize);
    assertDecodeAt(
        result.codec(),
        metadataBytes,
        dataBytes,
        bitPerBytePacker,
        startBitIndex,
        result.termStatesArray()[pos]);

    // With compact bytes
    int startByteIndex = pos * recordSize / 8;
    int endByteIndex = (pos + 1) * recordSize / 8;
    int length = endByteIndex - startByteIndex + ((pos + 1) * recordSize % 8 == 0 ? 0 : 1);
    dataBytes = new BytesRef(bitPerBytePacker.getCompactBytes(), startByteIndex, length);
    assertDecodeAt(
        result.codec(),
        metadataBytes,
        dataBytes,
        BitUnpackerImpl.INSTANCE,
        (pos * recordSize) % 8,
        result.termStatesArray()[pos]);
  }

  public static TermStateTestFixture getTermStateTestFixture(int size) {
    TermStateCodecImpl codec =
        new TermStateCodecImpl(
            new TermStateCodecComponent[] {
              TermStateCodecComponent.DocFreq.INSTANCE, TermStateCodecComponent.DocStartFP.INSTANCE,
            });

    ArrayList<IntBlockTermState> termStates = new ArrayList<>();
    long maxDocFreqSeen = -1;
    long docStartFPBase = random().nextLong(Long.MAX_VALUE >> 1);
    long maxDocStartFPDeltaSeen = -1;
    for (int i = 0; i < size; i++) {
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
    return new TermStateTestFixture(
        codec, maxDocFreqSeen, docStartFPBase, maxDocStartFPDeltaSeen, termStatesArray);
  }

  public record TermStateTestFixture(
      TermStateCodecImpl codec,
      long maxDocFreqSeen,
      long docStartFPBase,
      long maxDocStartFPDeltaSeen,
      IntBlockTermState[] termStatesArray) {}

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

  public void testGetCodec() {
    for (IndexOptions indexOptions : IndexOptions.values()) {
      if (indexOptions == IndexOptions.NONE) {
        continue;
      }
      for (int i = 0; i < 8; i++) {
        if ((i & 0b011) == 0b011) {
          continue;
        }
        if ((i & 0b100) == 0b100
            && indexOptions.ordinal() < IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
          continue;
        }
        TermType termType = TermType.fromId(i);
        var expected = getExpectedCodec(termType, indexOptions);
        var got = TermStateCodecImpl.getCodec(termType, indexOptions);
        assertEquals(expected, got);
      }
    }
  }

  // Enumerate the expected Codec we get for (TermType, IndexOptions) pairs.
  static TermStateCodecImpl getExpectedCodec(TermType termType, IndexOptions indexOptions) {
    ArrayList<TermStateCodecComponent> components = new ArrayList<>();
    // Wish I can code this better in java...
    switch (termType.getId()) {
        // Not singleton doc; No skip data; No last position block offset
      case 0b000 -> {
        assert !termType.hasLastPositionBlockOffset()
            && !termType.hasSkipData()
            && !termType.hasSingletonDoc();
        components.add(TermStateCodecComponent.DocStartFP.INSTANCE);
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal()) {
          components.add(TermStateCodecComponent.DocFreq.INSTANCE);
        }
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
          components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
          components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        }
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }
        // Singleton doc; No skip data; No last position block offset
      case 0b001 -> {
        assert !termType.hasLastPositionBlockOffset()
            && !termType.hasSkipData()
            && termType.hasSingletonDoc();
        components.add(TermStateCodecComponent.SingletonDocId.INSTANCE);
        // If field needs frequency, we need totalTermsFreq.
        // Since there is only 1 doc, totalTermsFreq == docFreq.
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal()) {
          components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
        }
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
          components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        }
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }

        // Not Singleton doc; Has skip data; No last position block offset
      case 0b010 -> {
        assert !termType.hasLastPositionBlockOffset()
            && termType.hasSkipData()
            && !termType.hasSingletonDoc();
        components.add(TermStateCodecComponent.DocStartFP.INSTANCE);
        components.add(TermStateCodecComponent.SkipOffset.INSTANCE);
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal()) {
          components.add(TermStateCodecComponent.DocFreq.INSTANCE);
        }
        if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
          components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
          components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        }
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }
        // Singleton doc but has skip data; Invalid state.
      case 0b011, 0b111 -> {
        assert termType.hasSkipData() && termType.hasSingletonDoc();
        throw new IllegalStateException(
            "Unreachable. A term has skip data but also only has one doc!? Must be a bug");
      }
        // Not singleton doc; No skip data; Has last position block offset;
      case 0b100 -> {
        assert termType.hasLastPositionBlockOffset()
            && !termType.hasSkipData()
            && !termType.hasSingletonDoc();
        assert indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal();
        components.add(TermStateCodecComponent.DocStartFP.INSTANCE);
        components.add(TermStateCodecComponent.DocFreq.INSTANCE);
        components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
        components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        components.add(TermStateCodecComponent.LastPositionBlockOffset.INSTANCE);
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }
        // Singleton doc; No skip data; Has last position block offset;
      case 0b101 -> {
        assert termType.hasLastPositionBlockOffset()
            && !termType.hasSkipData()
            && termType.hasSingletonDoc();
        assert indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal();
        components.add(TermStateCodecComponent.SingletonDocId.INSTANCE);
        components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
        components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        components.add(TermStateCodecComponent.LastPositionBlockOffset.INSTANCE);
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }
        // Not singleton doc; Has skip data; Has last position block offset;
      case 0b110 -> {
        assert termType.hasLastPositionBlockOffset()
            && termType.hasSkipData()
            && !termType.hasSingletonDoc();
        assert indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal();
        components.add(TermStateCodecComponent.DocStartFP.INSTANCE);
        components.add(TermStateCodecComponent.SkipOffset.INSTANCE);
        components.add(TermStateCodecComponent.DocFreq.INSTANCE);
        components.add(TermStateCodecComponent.TotalTermFreq.INSTANCE);
        components.add(TermStateCodecComponent.PositionStartFP.INSTANCE);
        components.add(TermStateCodecComponent.LastPositionBlockOffset.INSTANCE);
        if (indexOptions.ordinal()
            >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
          components.add(TermStateCodecComponent.PayloadStartFP.INSTANCE);
        }
      }
      default -> throw new IllegalStateException("Unreachable");
    }

    return new TermStateCodecImpl(components.toArray(TermStateCodecComponent[]::new));
  }
}
