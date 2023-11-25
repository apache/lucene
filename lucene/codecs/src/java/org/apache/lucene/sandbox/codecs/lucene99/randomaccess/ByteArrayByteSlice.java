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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;

final class ByteArrayByteSlice implements ByteSlice {
  private final byte[] bytes;

  ByteArrayByteSlice(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public long size() {
    return bytes.length;
  }

  @Override
  public void writeAll(DataOutput output) throws IOException {
    output.writeBytes(bytes, bytes.length);
  }

  @Override
  public long getLong(long pos) {
    return (long) BitUtil.VH_LE_LONG.get(bytes, (int) pos);
  }

  @Override
  public byte[] getBytes(long pos, int length) {
    if (length == 0) {
      return new byte[0];
    }
    byte[] result = new byte[length];
    System.arraycopy(bytes, (int) pos, result, 0, length);
    return result;
  }

  @Override
  public void readBytesTo(byte[] destination, long pos, int length) {
    if (length == 0) {
      return;
    }
    System.arraycopy(bytes, (int) pos, destination, 0, length);
  }
}
