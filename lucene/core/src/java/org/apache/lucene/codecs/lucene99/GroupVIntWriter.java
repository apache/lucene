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
import org.apache.lucene.store.DataOutput;

/**
 * Encode integers using group-varint. It uses VInt to encode tail values that are not enough for a
 * group
 */
public class GroupVIntWriter {

  // the maximum size of one group is 4 integers + 1 byte flag.
  private byte[] bytes = new byte[17];
  private int byteOffset = 0;

  public GroupVIntWriter() {}

  private int encodeValue(int v) {
    int lastOff = byteOffset;
    do {
      bytes[byteOffset++] = (byte) (v & 0xFF);
      v >>>= 8;
    } while (v != 0);
    return byteOffset - lastOff;
  }

  public void writeValues(DataOutput out, long[] values, int limit) throws IOException {
    int off = 0;

    // encode each group
    while ((limit - off) >= 4) {
      byte flag = 0;
      byteOffset = 1;
      flag |= (encodeValue((int) values[off++]) - 1) << 6;
      flag |= (encodeValue((int) values[off++]) - 1) << 4;
      flag |= (encodeValue((int) values[off++]) - 1) << 2;
      flag |= (encodeValue((int) values[off++]) - 1);
      bytes[0] = flag;
      out.writeBytes(bytes, byteOffset);
    }

    // tail vints
    for (; off < limit; off++) {
      out.writeVInt((int) values[off]);
    }
  }
}
