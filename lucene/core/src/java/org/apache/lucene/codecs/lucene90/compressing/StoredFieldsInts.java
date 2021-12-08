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
package org.apache.lucene.codecs.lucene90.compressing;

import static org.apache.lucene.util.IntCodecHelper.*;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

public class StoredFieldsInts {

  private StoredFieldsInts() {}

  static void writeInts(int[] values, int start, int count, DataOutput out) throws IOException {
    boolean allEqual = true;
    for (int i = 1; i < count; ++i) {
      if (values[start + i] != values[start]) {
        allEqual = false;
        break;
      }
    }
    if (allEqual) {
      out.writeByte((byte) 0);
      out.writeVInt(values[0]);
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(values[start + i]);
      }
      if (max <= 0xff) {
        out.writeByte((byte) 8);
        writeInts8(out, count, values, start);
      } else if (max <= 0xffff) {
        out.writeByte((byte) 16);
        writeInts16(out, count, values, start);
      } else {
        out.writeByte((byte) 32);
        writeInts32(out, count, values, start);
      }
    }
  }

  /** Read {@code count} integers into {@code values}. */
  static void readInts(IndexInput in, int count, long[] values, int offset) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case 0:
        Arrays.fill(values, offset, offset + count, in.readVInt());
        break;
      case 8:
        readInts8(in, count, values, offset);
        break;
      case 16:
        readInts16(in, count, values, offset);
        break;
      case 32:
        readInts32(in, count, values, offset);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }
}
