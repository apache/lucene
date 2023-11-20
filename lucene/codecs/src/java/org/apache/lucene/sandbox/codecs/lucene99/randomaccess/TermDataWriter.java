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
import org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking.DataOutputBitPacker;
import org.apache.lucene.store.DataOutput;

/** Writes TermData to two separate {@link DataOutput} one for metadata, another for term data */
final class TermDataWriter {
  static final int NUM_TERMS_PER_BLOCK = 256;

  private final TermStateCodec termStateCodec;

  private final IntBlockTermStateBuffer buffer = new IntBlockTermStateBuffer(NUM_TERMS_PER_BLOCK);

  private final DataOutput metadataOut;
  private final DataOutputBitPacker dataOutputBitPacker;

  private long totalMetaDataBytesWritten;

  TermDataWriter(TermStateCodec termStateCodec, DataOutput metadataOut, DataOutput dataOut) {
    this.termStateCodec = termStateCodec;
    this.metadataOut = metadataOut;
    this.dataOutputBitPacker = new DataOutputBitPacker(dataOut);
  }

  void addTermState(IntBlockTermState termState) throws IOException {
    buffer.add(termState);
    if (buffer.numUsed == NUM_TERMS_PER_BLOCK) {
      writeBlock();
    }
  }

  void finish() throws IOException {
    if (buffer.numUsed > 0) {
      writeBlock();
    }
  }

  long getTotalMetaDataBytesWritten() {
    return totalMetaDataBytesWritten;
  }

  long getTotalDataBytesWritten() {
    return dataOutputBitPacker.getNumBytesWritten();
  }

  private void writeBlock() throws IOException {
    metadataOut.writeLong(dataOutputBitPacker.getNumBytesWritten());
    byte[] metadata =
        termStateCodec.encodeBlockUpTo(buffer.elements, buffer.numUsed, dataOutputBitPacker);
    metadataOut.writeBytes(metadata, metadata.length);
    totalMetaDataBytesWritten += metadata.length + 8;
    buffer.clear();
  }

  /** act like a minial ArrayList, but provide access to the internal array */
  static class IntBlockTermStateBuffer {
    IntBlockTermState[] elements;
    int numUsed;

    IntBlockTermStateBuffer(int capacity) {
      this.elements = new IntBlockTermState[capacity];
    }

    void add(IntBlockTermState termState) {
      elements[numUsed++] = termState;
    }

    void clear() {
      for (int i = 0; i < numUsed; i++) {
        elements[i] = null;
      }
      numUsed = 0;
    }
  }
}
