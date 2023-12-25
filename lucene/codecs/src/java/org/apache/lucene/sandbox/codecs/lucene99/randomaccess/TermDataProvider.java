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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

/** Factory class to produce instances of TermData */
record TermDataProvider(ByteSliceProvider metadataProvider, ByteSliceProvider dataProvider) {
  static TermDataProvider deserializeOnHeap(
      DataInput metaInput, DataInput metadataInput, DataInput dataInput) throws IOException {
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

    return new TermDataProvider(
        () -> new ByteArrayByteSlice(metadataBytes), () -> new ByteArrayByteSlice(dataBytes));
  }

  static TermDataProvider deserializeOffHeap(
      DataInput metaInput, IndexInput metadataInput, IndexInput dataInput) throws IOException {
    final long metadataSize = metaInput.readVLong();
    final long dataSize = metaInput.readVLong();

    final long metadataStart = metadataInput.getFilePointer();
    final long dataStart = dataInput.getFilePointer();

    metadataInput.skipBytes(metadataSize);
    dataInput.skipBytes(dataSize);

    return new TermDataProvider(
        () ->
            new RandomAccessInputByteSlice(
                metadataInput.randomAccessSlice(metadataStart, metadataSize)),
        () -> new RandomAccessInputByteSlice(dataInput.randomAccessSlice(dataStart, dataSize)));
  }
}
