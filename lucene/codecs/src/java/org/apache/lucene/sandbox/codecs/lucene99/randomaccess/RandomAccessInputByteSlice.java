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
import org.apache.lucene.store.RandomAccessInput;

final class RandomAccessInputByteSlice implements ByteSlice {
  private final RandomAccessInput randomAccessInput;

  RandomAccessInputByteSlice(RandomAccessInput randomAccessInput) {
    this.randomAccessInput = randomAccessInput;
  }

  @Override
  public long size() {
    return randomAccessInput.length();
  }

  @Override
  public void writeAll(DataOutput output) throws IOException {
    for (long pos = 0; pos < randomAccessInput.length(); pos++) {
      // For buffered inputs and outputs this should be fine.
      output.writeByte(randomAccessInput.readByte(pos));
    }
  }

  @Override
  public long getLong(long pos) throws IOException {
    return randomAccessInput.readLong(pos);
  }

  @Override
  public byte[] getBytes(long pos, int length) throws IOException {
    if (length == 0) {
      return new byte[0];
    }
    byte[] result = new byte[length];
    randomAccessInput.readBytes(pos, result, 0, length);
    return result;
  }

  @Override
  public void readBytesTo(byte[] destination, long pos, int length) throws IOException {
    if (length == 0) {
      return;
    }
    randomAccessInput.readBytes(pos, destination, 0, length);
  }
}
