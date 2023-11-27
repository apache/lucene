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
import java.util.Arrays;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.DocFreq;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.DocStartFP;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.LastPositionBlockOffset;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.PayloadStartFP;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.PositionStartFP;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.SingletonDocId;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.SkipOffset;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermStateCodecComponent.TotalTermFreq;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitPacker;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

final class TermStateCodecImpl implements TermStateCodec {
  private final TermStateCodecComponent[] components;
  private final int metadataBytesLength;

  public TermStateCodecImpl(TermStateCodecComponent[] components) {
    assert components.length > 0;

    this.components = components;
    int metadataBytesLength = 0;
    for (var component : components) {
      metadataBytesLength += getMetadataLength(component);
    }
    this.metadataBytesLength = metadataBytesLength;
  }

  @Override
  public int getMaximumRecordSizeInBytes() {
    // worst case: no compression at all, so each component taks 8 byte.
    // two extra bytes when the record takes partial byte at the start and end.
    return components.length * 8 + 2;
  }

  @Override
  public int getMetadataBytesLength() {
    return metadataBytesLength;
  }

  @Override
  public int getNumBitsPerRecord(BytesRef metadataBytes) {
    int upto = metadataBytes.offset;
    int totalBitsPerTermState = 0;

    for (var component : components) {
      byte bitWidth = metadataBytes.bytes[upto++];
      if (component.isMonotonicallyIncreasing()) {
        upto += 8;
      }
      totalBitsPerTermState += bitWidth;
    }

    return totalBitsPerTermState;
  }

  private static int getMetadataLength(TermStateCodecComponent component) {
    // 1 byte for bitWidth; optionally 8 byte more for the reference value
    return 1 + (component.isMonotonicallyIncreasing() ? 8 : 0);
  }

  public static TermStateCodecImpl getCodec(
      TermType termType, IndexOptions indexOptions, boolean hasPayloads) {
    assert indexOptions.ordinal() > IndexOptions.NONE.ordinal();
    // A term can't have skip data (has more than one block's worth of doc),
    // while having a singleton doc at the same time!
    assert !(termType.hasSkipData() && termType.hasSingletonDoc());

    // Can't have payload for index options that is less than POSITIONS
    assert indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()
        || !hasPayloads;

    ArrayList<TermStateCodecComponent> components = new ArrayList<>();
    // handle docs and docFreq
    if (termType.hasSingletonDoc()) {
      components.add(SingletonDocId.INSTANCE);
    } else {
      components.add(DocStartFP.INSTANCE);
      components.add(DocFreq.INSTANCE);
    }
    // handle skip data
    if (termType.hasSkipData()) {
      components.add(SkipOffset.INSTANCE);
    }

    // handle freq
    if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal()) {
      components.add(TotalTermFreq.INSTANCE);
    }
    // handle positions
    if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
      components.add(PositionStartFP.INSTANCE);
      if (hasPayloads) {
        components.add(PayloadStartFP.INSTANCE);
      }
      if (termType.hasLastPositionBlockOffset()) {
        components.add(LastPositionBlockOffset.INSTANCE);
      }
    }
    // handle payload and offsets
    if (indexOptions.ordinal() >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal()) {
      components.add(PayloadStartFP.INSTANCE);
    }

    return new TermStateCodecImpl(components.toArray(TermStateCodecComponent[]::new));
  }

  @Override
  public String toString() {
    return "TermStateCodecImpl{" + "components=" + Arrays.toString(components) + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TermStateCodecImpl that = (TermStateCodecImpl) o;
    return Arrays.equals(components, that.components);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(components);
  }

  @Override
  public byte[] encodeBlockUpTo(IntBlockTermState[] inputs, int uptop, BitPacker bitPacker)
      throws IOException {
    Metadata[] metadataPerComponent = getMetadataPerComponent(inputs, uptop);
    byte[] metadataBytes = serializeMetadata(metadataPerComponent);

    // Encode inputs via the bitpacker
    for (int i = 0; i < uptop; i++) {
      encodeOne(bitPacker, inputs[i], metadataPerComponent);
    }
    bitPacker.flush();

    return metadataBytes;
  }

  private Metadata[] getMetadataPerComponent(IntBlockTermState[] inputs, int upTo) {
    Metadata[] metadataPerComponent = new Metadata[components.length];
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      byte bitWidth = TermStateCodecComponent.getBitWidth(inputs, upTo, component);
      long referenceValue =
          component.isMonotonicallyIncreasing() ? component.getTargetValue(inputs[0]) : 0L;
      metadataPerComponent[i] = new Metadata(bitWidth, referenceValue);
    }
    return metadataPerComponent;
  }

  private byte[] serializeMetadata(Metadata[] metadataPerComponent) {
    byte[] metadataBytes = new byte[this.metadataBytesLength];
    ByteArrayDataOutput dataOut = new ByteArrayDataOutput(metadataBytes);

    for (int i = 0; i < components.length; i++) {
      var metadata = metadataPerComponent[i];
      dataOut.writeByte(metadata.bitWidth);
      if (components[i].isMonotonicallyIncreasing()) {
        dataOut.writeLong(metadata.referenceValue);
      }
    }
    return metadataBytes;
  }

  private void encodeOne(
      BitPacker bitPacker, IntBlockTermState termState, Metadata[] metadataPerComponent)
      throws IOException {
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      var metadata = metadataPerComponent[i];
      long valToEncode = component.getTargetValue(termState) - metadata.referenceValue;
      bitPacker.add(valToEncode, metadata.bitWidth);
    }
  }

  @Override
  public IntBlockTermState decodeWithinBlock(
      BytesRef metadataBytes, BytesRef dataBytes, BitUnpacker bitUnpacker, int index) {
    assert metadataBytes.length == this.metadataBytesLength;

    int startBitIndex = index * getNumBitsPerRecord(metadataBytes);
    return decodeAt(metadataBytes, dataBytes, bitUnpacker, startBitIndex);
  }

  @Override
  public IntBlockTermState decodeAt(
      BytesRef metadataBytes, BytesRef dataBytes, BitUnpacker bitUnpacker, int startBitIndex) {

    IntBlockTermState decoded = new IntBlockTermState();
    decodeAtWithReuse(metadataBytes, dataBytes, bitUnpacker, startBitIndex, decoded);

    return decoded;
  }

  @Override
  public void decodeAtWithReuse(
      BytesRef metadataBytes,
      BytesRef dataBytes,
      BitUnpacker bitUnpacker,
      int startBitIndex,
      IntBlockTermState reuse) {
    assert metadataBytes.length == this.metadataBytesLength;

    reuse.lastPosBlockOffset = -1;
    reuse.skipOffset = -1;
    reuse.singletonDocID = -1;

    int upto = metadataBytes.offset;
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      int bitWidth = metadataBytes.bytes[upto++];
      long val = bitUnpacker.unpack(dataBytes, startBitIndex, bitWidth);
      if (component.isMonotonicallyIncreasing()) {
        val += (long) BitUtil.VH_LE_LONG.get(metadataBytes.bytes, upto);
        upto += 8;
      }
      component.setTargetValue(reuse, val);
      startBitIndex += bitWidth;
    }
  }

  private record Metadata(byte bitWidth, long referenceValue) {}
}
