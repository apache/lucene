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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link DataOutput} backed by a growable byte[]. The byte[] will only grow, never shrinks.
 *
 * @lucene.experimental
 */
public final class GrowableByteArrayDataOutput extends DataOutput implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(GrowableByteArrayDataOutput.class);

  private byte[] bytes;

  private int nextWrite;

  public GrowableByteArrayDataOutput(int initialSize) {
    bytes = new byte[initialSize];
  }

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

  /**
   * @return the current position of this DataOutput
   */
  public int getPosition() {
    return nextWrite;
  }

  /**
   * @return the internal byte[]
   */
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * Set the position of the byte[], increasing the capacity if needed.
   *
   * @param position the new position
   */
  public void setPosition(int position) {
    assert position >= 0;
    if (position > nextWrite) {
      ensureCapacity(position - nextWrite);
    }
    nextWrite = position;
  }

  /**
   * Ensure we can write additional bytesToWrite bytes.
   *
   * @param bytesToWrite the additional bytes to write
   */
  private void ensureCapacity(int bytesToWrite) {
    bytes = ArrayUtil.grow(bytes, nextWrite + bytesToWrite);
  }

  /**
   * Writes all of our bytes to the target {@link DataOutput}.
   *
   * @param out the DataOutput to write to
   */
  public void writeTo(DataOutput out) throws IOException {
    out.writeBytes(bytes, 0, nextWrite);
  }

  /**
   * Copy a portion of the written bytes to a target byte[]
   *
   * @param srcOffset the offset of the internal byte[] to copy
   * @param dest the target byte[] to write to
   * @param destOffset the offset in the target byte[]
   * @param len the number of bytes to write
   */
  public void writeTo(int srcOffset, byte[] dest, int destOffset, int len) {
    assert srcOffset + len <= nextWrite;
    System.arraycopy(bytes, srcOffset, dest, destOffset, len);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bytes);
  }
}
