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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

// Storing a byte[] for the current node of the FST we are writing. The byte[] will only grow, never
// shrink.
final class GrowableByteArrayDataOutput extends DataOutput implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(GrowableByteArrayDataOutput.class);

  private static final int INITIAL_SIZE = 1 << 8;

  // holds an initial size of 256 bytes. this byte array will only grow, but not shrink
  byte[] bytes = new byte[INITIAL_SIZE];

  private int nextWrite;

  @Override
  public void writeByte(byte b) {
    ensureCapacity(1);
    bytes[nextWrite++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) {
    ensureCapacity(len);
    System.arraycopy(b, offset, bytes, nextWrite, len);
    nextWrite += len;
  }

  /** Skip a number of bytes, increasing capacity if needed */
  public void skipBytes(int len) {
    ensureCapacity(len);
    nextWrite += len;
  }

  /**
   * Ensure we can write additional capacityToWrite bytes.
   *
   * @param capacityToWrite the additional bytes to write
   */
  private void ensureCapacity(int capacityToWrite) {
    bytes = ArrayUtil.grow(bytes, nextWrite + capacityToWrite);
  }

  /** Absolute write byte; you must ensure dest is &lt; max position written so far. */
  public void writeByte(int dest, byte b) {
    assert dest < nextWrite;
    bytes[dest] = b;
  }

  /**
   * Absolute writeBytes without changing the current position. Note: this cannot "grow" the bytes,
   * so you must only call it on already written parts.
   */
  public void writeBytes(int dest, byte[] b, int offset, int len) {
    assert dest + len <= getPosition() : "dest=" + dest + " pos=" + getPosition() + " len=" + len;
    System.arraycopy(b, offset, bytes, dest, len);
  }

  /**
   * Absolute copy bytes self to self, without changing the position. Note: this cannot "grow" the
   * bytes, so must only call it on already written parts.
   */
  public void copyBytes(int src, int dest, int len) {
    assert src < dest;
    writeBytes(dest, bytes, src, len);
  }

  /** Copies bytes from this store to a target byte array. */
  public void copyBytes(int src, byte[] dest, int offset, int len) {
    System.arraycopy(bytes, src, dest, offset, len);
  }

  /** Reverse from srcPos, inclusive, to destPos, inclusive. */
  public void reverse() {
    int src = 0;
    int dest = nextWrite - 1;
    int limit = (dest - src + 1) / 2;
    for (int i = 0; i < limit; i++) {
      byte b = bytes[src + i];
      bytes[src + i] = bytes[dest - i];
      bytes[dest - i] = b;
    }
  }

  public int getPosition() {
    return nextWrite;
  }

  /**
   * Pos must be less than the max position written so far! Ie, you cannot "grow" the file with
   * this!
   */
  public void truncate(int newLen) {
    assert newLen >= 0 && newLen <= getPosition();
    nextWrite = newLen;
  }

  /** Writes all of our bytes to the target {@link DataOutput}. */
  public void writeTo(DataOutput out) throws IOException {
    out.writeBytes(bytes, 0, nextWrite);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bytes);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(pos=" + nextWrite + ")";
  }
}
