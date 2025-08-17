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

package org.apache.lucene.store;

import java.io.IOException;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * RandomAccessInput implementation backed by a byte array. <b>WARNING:</b> This class omits most
 * low-level checks, so be sure to test heavily with assertions enabled.
 *
 * @lucene.experimental
 */
public final class ByteArrayRandomAccessInput implements RandomAccessInput {

  private byte[] bytes;

  public ByteArrayRandomAccessInput(byte[] bytes) {
    this.bytes = bytes;
  }

  public ByteArrayRandomAccessInput() {
    this(BytesRef.EMPTY_BYTES);
  }

  /** Reset the array to the provided one so it can be reused */
  public void reset(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public long length() {
    return bytes.length;
  }

  @Override
  public byte readByte(long pos) throws IOException {
    return bytes[(int) pos];
  }

  @Override
  public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
    System.arraycopy(this.bytes, (int) pos, bytes, offset, length);
  }

  @Override
  public short readShort(long pos) throws IOException {
    return (short) BitUtil.VH_LE_SHORT.get(bytes, (int) pos);
  }

  @Override
  public int readInt(long pos) throws IOException {
    return (int) BitUtil.VH_LE_INT.get(bytes, (int) pos);
  }

  @Override
  public long readLong(long pos) throws IOException {
    return (long) BitUtil.VH_LE_LONG.get(bytes, (int) pos);
  }
}
