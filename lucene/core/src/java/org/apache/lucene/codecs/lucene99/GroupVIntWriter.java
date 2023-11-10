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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * Encode integers using group-varint. It uses VInt to encode tail values that are not enough for a
 * group
 */
class GroupVIntWriter {
  private int[] buffer = new int[4];
  private byte[] bytes = new byte[16];
  private int byteOffset = 0;
  private int bufferOffset = 0;
  private final IndexOutput output;

  public GroupVIntWriter(IndexOutput output) {
    this.output = output;
  }

  public void add(int v) throws IOException {
    buffer = ArrayUtil.grow(buffer, bufferOffset + 1);
    buffer[bufferOffset++] = v;
  }

  public void reset(int numValues) {
    buffer = ArrayUtil.grow(buffer, numValues);
    bufferOffset = 0;
    byteOffset = 0;
  }

  public void flush() throws IOException {
    if (bufferOffset == 0) {
      return;
    }
    encodeValues(buffer, bufferOffset);
  }

  private int encodeValue(int v) {
    int lastOff = byteOffset;
    while ((v & ~0xFF) != 0) {
      bytes[byteOffset++] = (byte) (v & 0xFF);
      v >>>= 8;
    }
    bytes[byteOffset++] = (byte) v;
    return byteOffset - lastOff;
  }

  private void encodeValues(int[] values, int limit) throws IOException {
    int off = 0;
    byte numGroup = 0;

    // encode each group
    while ((limit - off) >= 4) {
      // the maximum size of one group is 4 ints + 1 byte flag.
      bytes = ArrayUtil.grow(bytes, byteOffset + 17);
      byte flag = 0;
      int flagPos = byteOffset++;
      flag |= (encodeValue(values[off++]) - 1) << 6;
      flag |= (encodeValue(values[off++]) - 1) << 4;
      flag |= (encodeValue(values[off++]) - 1) << 2;
      flag |= (encodeValue(values[off++]) - 1);
      bytes[flagPos] = flag;
      numGroup++;
    }

    if (limit >= 4) {
      output.writeVInt(byteOffset);
      output.writeByte(numGroup);
      output.writeBytes(bytes, byteOffset);
    } else {
      output.writeVInt(0);
    }

    // tail vints
    for (; off < limit; off++) {
      output.writeVInt(values[off]);
    }
  }
}
