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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** Utility class to encode/decode postings block. */
final class PostingsUtil {

  /**
   * Read values that have been written using variable-length encoding and group-varint encoding
   * instead of bit-packing.
   */
  static void readVIntBlock(
      IndexInput docIn,
      long[] docBuffer,
      long[] freqBuffer,
      int num,
      boolean indexHasFreq,
      boolean decodeFreq)
      throws IOException {
    docIn.readGroupVInts(docBuffer, num);
    if (indexHasFreq && decodeFreq) {
      for (int i = 0; i < num; ++i) {
        freqBuffer[i] = docBuffer[i] & 0x01;
        docBuffer[i] >>= 1;
        if (freqBuffer[i] == 0) {
          freqBuffer[i] = docIn.readVInt();
        }
      }
    } else if (indexHasFreq) {
      for (int i = 0; i < num; ++i) {
        docBuffer[i] >>= 1;
      }
    }
  }

  /** Write freq buffer with variable-length encoding and doc buffer with group-varint encoding. */
  static void writeVIntBlock(
      IndexOutput docOut, long[] docBuffer, long[] freqBuffer, int num, boolean writeFreqs)
      throws IOException {
    if (writeFreqs) {
      for (int i = 0; i < num; i++) {
        docBuffer[i] = (docBuffer[i] << 1) | (freqBuffer[i] == 1 ? 1 : 0);
      }
    }
    docOut.writeGroupVInts(docBuffer, num);
    if (writeFreqs) {
      for (int i = 0; i < num; i++) {
        final int freq = (int) freqBuffer[i];
        if (freq != 1) {
          docOut.writeVInt(freq);
        }
      }
    }
  }
}
