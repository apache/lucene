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
import org.apache.lucene.util.BytesRef;

/**
 * Holds the bit-packed {@link IntBlockTermState} for a given {@link
 * org.apache.lucene.sandbox.codecs.lucene99.randomaccess.TermType}
 */
record TermData(ByteSlice metadata, ByteSlice data) {
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

  IntBlockTermState getTermStateWithBuffer(
      TermStateCodec codec, long ord, byte[] metaDataBuffer, byte[] dataBuffer) throws IOException {
    long blockId = ord / TermDataWriter.NUM_TERMS_PER_BLOCK;
    long metadataStartPos = blockId * (codec.getMetadataBytesLength() + 8);
    long dataStartPos = metadata.getLong(metadataStartPos);

    int metadataLength = codec.getMetadataBytesLength();
    metadata.readBytesTo(metaDataBuffer, metadataStartPos + 8, metadataLength);
    BytesRef metadataBytesRef = new BytesRef(metaDataBuffer, 0, metadataLength);

    int numBitsPerRecord = codec.getNumBitsPerRecord(metadataBytesRef);
    int dataBitIndex = numBitsPerRecord * ((int) (ord % TermDataWriter.NUM_TERMS_PER_BLOCK));
    int startBitIndex = dataBitIndex % 8;
    int numBytesToRead = (startBitIndex + numBitsPerRecord) / 8;
    if ((startBitIndex + numBitsPerRecord) % 8 > 0) {
      numBytesToRead += 1;
    }
    data.readBytesTo(dataBuffer, dataStartPos + dataBitIndex / 8, numBytesToRead);
    BytesRef dataBytesRef = new BytesRef(dataBuffer, 0, numBytesToRead);

    return codec.decodeAt(metadataBytesRef, dataBytesRef, BitUnpackerImpl.INSTANCE, startBitIndex);
  }
}
