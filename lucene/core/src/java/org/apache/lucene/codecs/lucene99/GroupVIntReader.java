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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;

/**
 * Decode integers using group-varint. It will fully read the bytes for the block, to avoid repeated
 * expensive bounds checking per readBytes.
 */
public class GroupVIntReader {
  DataInput in;

  // buffer for all groups
  private int offset = 0;
  private byte[] bytes = new byte[16];

  private byte numGroups = 0;
  private byte flag;

  // for testing only
  private byte finalNumGroups;

  // the next int will be read in the single group. in the range [0-3].
  private int posInGroup = 0;

  public GroupVIntReader() {}

  /** Called when decode a new block. */
  public void reset(DataInput indexInput, int num) throws IOException {
    this.in = indexInput;
    offset = 0;
    posInGroup = 0;
    numGroups = 0;

    if (num > 0) {
      int len = in.readVInt();
      if (len > 0) {
        numGroups = in.readByte();
        finalNumGroups = numGroups;
        // + 3 bytes to avoid BitUtil.VH_LE_INT.get out of array bounds when reading the last value
        bytes = ArrayUtil.growNoCopy(bytes, len + 3);
        in.readBytes(bytes, 0, len);
      }
    }
  }

  private void readVInts(long[] docs, int off, int limit) throws IOException {
    for (int i = off; i < limit; i++) {
      docs[i] = in.readVInt();
    }
  }

  /** only readValues or nextInt can be called after reset */
  public void readValues(long[] docs, int limit) throws IOException {
    if (numGroups == 0) {
      readVInts(docs, 0, limit);
      return;
    }
    int groupValues = limit & 0xFFFFFFFC;
    posInGroup = 0;
    for (int i = 0; i < groupValues; i++) {
      posInGroup = i % 4;
      if (posInGroup == 0) {
        flag = bytes[offset++];
      }
      int shift = 6 - (posInGroup << 1);
      int len = ((flag >>> shift) & 3) + 1;
      int mask = 0xFFFFFFFF >>> ((4 - len) << 3);
      docs[i] = (int) BitUtil.VH_LE_INT.get(bytes, offset) & mask;
      offset += len;
    }
    if (groupValues < limit) {
      readVInts(docs, groupValues, limit);
    }
  }

  /** the caller must ensure that the read is not out of bounds */
  public int nextInt() throws IOException {
    if (posInGroup == 0) {
      if (numGroups > 0) {
        flag = bytes[offset++];
        numGroups--;
      } else {
        return in.readVInt();
      }
    }

    // get int from groups buffer
    int shift = 6 - (posInGroup << 1);
    int len = ((flag >>> shift) & 3) + 1;
    int mask = 0xFFFFFFFF >>> ((4 - len) << 3);

    assert offset + 4 <= bytes.length;
    int v = (int) BitUtil.VH_LE_INT.get(bytes, offset) & mask;
    offset += len;
    posInGroup = ++posInGroup % 4;
    assert v >= 0;
    return v;
  }

  // for testing only
  public void rewind() {
    offset = 0;
    posInGroup = 0;
    numGroups = finalNumGroups;
  }
}
