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
import org.apache.lucene.store.DataInput;

/** Decode integers using group-varint. */
public class GroupVIntReader {

  public static void readValues(DataInput in, long[] docs, int limit) throws IOException {
    int i;
    for (i = 0; i <= limit - 4; i += 4) {
      final int flag = in.readByte() & 0xFF;

      final int n1Minus1 = flag >> 6;
      final int n2Minus1 = (flag >> 4) & 0x03;
      final int n3Minus1 = (flag >> 2) & 0x03;
      final int n4Minus1 = flag & 0x03;

      docs[i] = readLong(in, n1Minus1);
      docs[i + 1] = readLong(in, n2Minus1);
      docs[i + 2] = readLong(in, n3Minus1);
      docs[i + 3] = readLong(in, n4Minus1);
    }
    for (; i < limit; ++i) {
      docs[i] = in.readVInt();
    }
  }

  private static long readLong(DataInput in, int numBytesMinus1) throws IOException {
    switch (numBytesMinus1) {
      case 0:
        return in.readByte() & 0xFFL;
      case 1:
        return in.readShort() & 0xFFFFL;
      case 2:
        return (in.readShort() & 0xFFFFL) | ((in.readByte() & 0xFFL) << 16);
      default:
        return in.readInt() & 0xFFFFFFFFL;
    }
  }
}
