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
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.BitUnpackerImpl;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

/**
 * Holds the bit-packed {@link IntBlockTermState} for a given {@link
 * org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermType}
 */
record TermData(TermType termType, ByteSlice metadata, ByteSlice data) {

  IntBlockTermState getTermState(TermStateCodec codec, long ord) throws IOException {
    long blockId = ord / TermDataWriter.NUM_TERMS_PER_BLOCK;
    long metadataStartPos = blockId * (codec.getMetadataBytesLength() + 8);
    long dataStartPos = metadata.getLong(metadataStartPos);
    BytesRef metadataBytesRef =
        new BytesRef(metadata.getBytes(metadataStartPos + 8, codec.getMetadataBytesLength()));

    int numBitsPerRecord = codec.getNumBitsPerRecord(metadataBytesRef);
    int dataBitIndex = numBitsPerRecord * ((int) (ord % TermDataWriter.NUM_TERMS_PER_BLOCK));
    int startBitIndex = dataBitIndex % 8;
    int numBytesToRead = (startBitIndex + numBitsPerRecord) / 8;
    if ((startBitIndex + numBitsPerRecord) % 8 > 0) {
      numBytesToRead += 1;
    }
    BytesRef dataBytesRef =
        new BytesRef(data.getBytes(dataStartPos + dataBitIndex / 8, numBytesToRead));

    return codec.decodeAt(metadataBytesRef, dataBytesRef, BitUnpackerImpl.INSTANCE, startBitIndex);
  }

  static TermData deserializeOnHeap(
      DataInput metaInput, DataInput metadataInput, DataInput dataInput) throws IOException {
    TermType termType = TermType.fromId(metaInput.readByte());
    long metadataSize = metaInput.readVLong();
    long dataSize = metaInput.readVLong();

    if (metadataSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Metadata size it too large to store on heap. Must be less than " + Integer.MAX_VALUE);
    }
    if (dataSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Data size it too large to store on heap.Must be less than " + Integer.MAX_VALUE);
    }

    byte[] metadataBytes = new byte[(int) metadataSize];
    byte[] dataBytes = new byte[(int) dataSize];

    metadataInput.readBytes(metadataBytes, 0, metadataBytes.length);
    dataInput.readBytes(dataBytes, 0, dataBytes.length);

    return new TermData(
        termType, new ByteArrayByteSlice(metadataBytes), new ByteArrayByteSlice(dataBytes));
  }

  static TermData deserializeOffHeap(
      DataInput metaInput, IndexInput metadataInput, IndexInput dataInput) throws IOException {
    TermType termType = TermType.fromId(metaInput.readByte());
    long metadataSize = metaInput.readVLong();
    long dataSize = metaInput.readVLong();

    RandomAccessInput metadata =
        metadataInput.randomAccessSlice(metadataInput.getFilePointer(), metadataSize);
    metadataInput.skipBytes(metadataSize);
    RandomAccessInput data = dataInput.randomAccessSlice(dataInput.getFilePointer(), dataSize);
    dataInput.skipBytes(dataSize);

    return new TermData(
        termType, new RandomAccessInputByteSlice(metadata), new RandomAccessInputByteSlice(data));
  }
}
