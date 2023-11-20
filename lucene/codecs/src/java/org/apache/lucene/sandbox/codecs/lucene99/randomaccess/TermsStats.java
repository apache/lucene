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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

/** Data class that holds starts for term stats for a field */
record TermsStats(
    int fieldNumber,
    long size,
    long sumTotalTermFreq,
    long sumDocFreq,
    int docCount,
    BytesRef minTerm,
    BytesRef maxTerm) {

  void serialize(DataOutput output) throws IOException {
    output.writeVInt(fieldNumber);
    output.writeVLong(size);
    output.writeVLong(sumTotalTermFreq);
    output.writeVLong(sumDocFreq);
    output.writeVInt(docCount);
    writeBytesRef(output, minTerm);
    writeBytesRef(output, maxTerm);
  }

  static TermsStats deserialize(DataInput input) throws IOException {
    return new TermsStats(
        input.readVInt(),
        input.readVLong(),
        input.readVLong(),
        input.readVLong(),
        input.readVInt(),
        readBytesRef(input),
        readBytesRef(input));
  }

  static void writeBytesRef(DataOutput output, BytesRef bytes) throws IOException {
    output.writeVInt(bytes.length);
    output.writeBytes(bytes.bytes, bytes.offset, bytes.length);
  }

  static BytesRef readBytesRef(DataInput input) throws IOException {
    int numBytes = input.readVInt();
    if (numBytes < 0) {
      throw new CorruptIndexException("invalid bytes length: " + numBytes, input);
    }

    byte[] bytes = new byte[numBytes];
    input.readBytes(bytes, 0, numBytes);

    return new BytesRef(bytes);
  }
}
