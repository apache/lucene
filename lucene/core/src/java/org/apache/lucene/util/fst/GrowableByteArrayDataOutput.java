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
package org.apache.lucene.util.fst;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

// Storing a single contiguous byte[] for the current node of the FST we are writing. The byte[]
// will only grow, never shrink.
// Note: This is only safe for usage that is bounded in the number of bytes written. Do not make
// this public! Public users should instead use ByteBuffersDataOutput
final class GrowableByteArrayDataOutput extends DataOutput implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(GrowableByteArrayDataOutput.class);

  private static final int INITIAL_SIZE = 1 << 8;

  // holds an initial size of 256 bytes. this byte array will only grow, but not shrink
  private byte[] bytes = new byte[INITIAL_SIZE];

  private int nextWrite;

  @Override
  public void writeByte(byte b) {
    ensureCapacity(1);
    bytes[nextWrite++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) {
    if (len == 0) {
      return;
    }
    ensureCapacity(len);
    System.arraycopy(b, offset, bytes, nextWrite, len);
    nextWrite += len;
  }

  public int getPosition() {
    return nextWrite;
  }

  public byte[] getBytes() {
    return bytes;
  }

  /** Set the position of the byte[], increasing the capacity if needed */
  public void setPosition(int newLen) {
    assert newLen >= 0;
    if (newLen > nextWrite) {
      ensureCapacity(newLen - nextWrite);
    }
    nextWrite = newLen;
  }

  /**
   * Ensure we can write additional capacityToWrite bytes.
   *
   * @param capacityToWrite the additional bytes to write
   */
  private void ensureCapacity(int capacityToWrite) {
    assert capacityToWrite > 0;
    bytes = ArrayUtil.grow(bytes, nextWrite + capacityToWrite);
  }

  /** Writes all of our bytes to the target {@link DataOutput}. */
  public void writeTo(DataOutput out) throws IOException {
    out.writeBytes(bytes, 0, nextWrite);
  }

  /** Copies bytes from this store to a target byte array. */
  public void writeTo(int srcOffset, byte[] dest, int destOffset, int len) {
    assert srcOffset + len <= nextWrite;
    System.arraycopy(bytes, srcOffset, dest, destOffset, len);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bytes);
  }
}
