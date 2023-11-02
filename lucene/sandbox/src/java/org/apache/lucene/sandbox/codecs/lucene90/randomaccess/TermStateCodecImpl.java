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

import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.sandbox.codecs.lucene90.randomaccess.bitpacking.BitPacker;
import org.apache.lucene.sandbox.codecs.lucene90.randomaccess.bitpacking.BitUnpacker;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;

final class TermStateCodecImpl implements TermStateCodec {
  private final TermStateCodecComponent[] components;
  private final int metadataBytesLength;

  private static int getMetadataLength(TermStateCodecComponent component) {
    // 1 byte for bitWidth; optionally 8 byte more for the reference value
    return 1 + (component.isMonotonicallyIncreasing() ? 8 : 0);
  }

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
  public byte[] encode(IntBlockTermState[] inputs, BitPacker bitPacker) {
    Metadata[] metadataPerComponent = getMetadataPerComponent(inputs);
    byte[] metadataBytes = serializeMetadata(metadataPerComponent);

    // Encode inputs via the bitpacker
    for (var termState : inputs) {
      encodeOne(bitPacker, termState, metadataPerComponent);
    }

    return metadataBytes;
  }

  private Metadata[] getMetadataPerComponent(IntBlockTermState[] inputs) {
    Metadata[] metadataPerComponent = new Metadata[components.length];
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      byte bitWidth = TermStateCodecComponent.getBitWidth(inputs, component);
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
      BitPacker bitPacker, IntBlockTermState termState, Metadata[] metadataPerComponent) {
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

    var metadata = deserializedMetadata(metadataBytes);

    int startBitIndex = index * metadata.totalBitsPerTermState;
    return extract(dataBytes, bitUnpacker, startBitIndex, metadata.metadataPerComponent);
  }

  private MetadataAndTotalBitsPerTermState deserializedMetadata(BytesRef metadataBytes) {
    Metadata[] metadataPerComponent = new Metadata[components.length];
    ByteArrayDataInput byteArrayDataInput =
        new ByteArrayDataInput(metadataBytes.bytes, metadataBytes.offset, metadataBytes.length);
    int totalBitsPerTermState = 0;
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      byte bitWidth = byteArrayDataInput.readByte();
      long referenceValue = -1;
      if (component.isMonotonicallyIncreasing()) {
        referenceValue = byteArrayDataInput.readLong();
      }
      metadataPerComponent[i] = new Metadata(bitWidth, referenceValue);

      totalBitsPerTermState += bitWidth;
    }

    return new MetadataAndTotalBitsPerTermState(metadataPerComponent, totalBitsPerTermState);
  }

  private IntBlockTermState extract(
      BytesRef dataBytes,
      BitUnpacker bitUnpacker,
      int startBitIndex,
      Metadata[] metadataPerComponent) {
    IntBlockTermState decoded = new IntBlockTermState();
    for (int i = 0; i < components.length; i++) {
      var component = components[i];
      var metadata = metadataPerComponent[i];
      long val = bitUnpacker.unpack(dataBytes, startBitIndex, metadata.bitWidth);
      if (metadata.referenceValue > 0) {
        val += metadata.referenceValue;
      }
      component.setTargetValue(decoded, val);
      startBitIndex += metadata.bitWidth;
    }
    return decoded;
  }

  private record Metadata(byte bitWidth, long referenceValue) {}

  private record MetadataAndTotalBitsPerTermState(
      Metadata[] metadataPerComponent, int totalBitsPerTermState) {}
}
