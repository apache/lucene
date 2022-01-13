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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

final class BKDForUtil {

  static final int BLOCK_SIZE = 512;

  private final int[] tmp = new int[384];

  void encode16(int[] ints, DataOutput out) throws IOException {
    for (int i = 0; i < 256; ++i) {
      ints[i] = ints[256 + i] | (ints[i] << 16);
    }
    for (int i = 0; i < 256; i++) {
      out.writeInt(ints[i]);
    }
  }

  void encode32(int off, int[] ints, DataOutput out) throws IOException {
    for (int i = 0; i < 512; i++) {
      out.writeInt(ints[off + i]);
    }
  }

  void encode24(int off, int[] ints, DataOutput out) throws IOException {
    for (int i = 0; i < 384; ++i) {
      tmp[i] = ints[off + i] << 8;
    }
    for (int i = 0; i < 128; i++) {
      final int longIdx = off + i + 384;
      tmp[i] |= (ints[longIdx] >>> 16) & 0xFF;
      tmp[i + 128] |= (ints[longIdx] >>> 8) & 0xFF;
      tmp[i + 256] |= ints[longIdx] & 0xFF;
    }
    for (int i = 0; i < 384; ++i) {
      out.writeInt(tmp[i]);
    }
  }

  void decode16(DataInput in, int[] ints, final int base) throws IOException {
    in.readInts(ints, 0, 256);
    for (int i = 0; i < 256; ++i) {
      int l = ints[i];
      ints[i] = (l >>> 16) + base;
      ints[256 + i] = (l & 0xFFFF) + base;
    }
  }

  void decode24(DataInput in, int[] ints) throws IOException {
    in.readInts(tmp, 0, 384);
    for (int i = 0; i < 384; ++i) {
      ints[i] = tmp[i] >>> 8;
    }
    for (int i = 0; i < 128; i++) {
      ints[i + 384] =
          ((tmp[i] & 0xFF) << 16) | ((tmp[i + 128] & 0xFF) << 8) | (tmp[i + 256] & 0xFF);
    }
  }
}
