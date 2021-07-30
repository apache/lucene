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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

final class DocValuesForUtil {

  static final int BLOCK_SIZE = Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;

  private final ForUtil forUtil = new ForUtil();

  void encode(long[] in, int bitsPerValue, DataOutput out) throws IOException {
    if (bitsPerValue <= 24) { // these bpvs are handled efficiently by ForUtil
      forUtil.encode(in, bitsPerValue, out);
    } else if (bitsPerValue <= 32) {
      collapse32(in);
      for (int i = 0; i < BLOCK_SIZE / 2; ++i) {
        out.writeLong(in[i]);
      }
    } else {
      for (long l : in) {
        out.writeLong(l);
      }
    }
  }

  void decode(int bitsPerValue, DataInput in, long[] out) throws IOException {
    if (bitsPerValue <= 24) {
      forUtil.decode(bitsPerValue, in, out);
    } else if (bitsPerValue <= 32) {
      in.readLongs(out, 0, BLOCK_SIZE / 2);
      expand32(out);
    } else {
      in.readLongs(out, 0, BLOCK_SIZE);
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64 + i];
    }
  }

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }
}
