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
import java.nio.ByteBuffer;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Provides off-heap storage of finite state machine (FST) bytes in a direct {@link ByteBuffer}.
 *
 * <p>Unlike {@link OnHeapFSTStore}, FST bytes are not counted in JVM heap by {@link
 * #ramBytesUsed()}. Morphological analysis dictionaries use this store to reduce on-heap pressure.
 *
 * @lucene.experimental
 */
public final class DirectBufferFSTStore implements FSTReader {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(DirectBufferFSTStore.class);

  private final ByteBuffer buffer;

  /**
   * Read FST bytes from {@code in} into a direct buffer.
   *
   * @param in input positioned at the first FST byte
   * @param numBytes number of FST bytes to read
   */
  public DirectBufferFSTStore(DataInput in, long numBytes) throws IOException {
    this.buffer = readDirectBuffer(in, numBytes);
  }

  /**
   * Copy FST bytes from an existing {@link FSTReader} into a direct buffer.
   *
   * @param source reader containing FST bytes
   * @param numBytes number of FST bytes to copy
   */
  public DirectBufferFSTStore(FSTReader source, long numBytes) throws IOException {
    if (numBytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("FST too large for direct buffer: " + numBytes);
    }
    ByteBuffer tmp = ByteBuffer.allocateDirect((int) numBytes);
    source.writeTo(
        new DataOutput() {
          @Override
          public void writeByte(byte b) {
            tmp.put(b);
          }

          @Override
          public void writeBytes(byte[] b, int offset, int length) {
            tmp.put(b, offset, length);
          }
        });
    tmp.flip();
    this.buffer = tmp.asReadOnlyBuffer();
  }

  private static ByteBuffer readDirectBuffer(DataInput in, long numBytes) throws IOException {
    if (numBytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("FST too large for direct buffer: " + numBytes);
    }
    int size = (int) numBytes;
    ByteBuffer tmp = ByteBuffer.allocateDirect(size);
    byte[] scratch = new byte[8192];
    int remaining = size;
    while (remaining > 0) {
      int toRead = Math.min(scratch.length, remaining);
      in.readBytes(scratch, 0, toRead);
      tmp.put(scratch, 0, toRead);
      remaining -= toRead;
    }
    tmp.flip();
    return tmp.asReadOnlyBuffer();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED;
  }

  @Override
  public FST.BytesReader getReverseBytesReader() {
    return new ReverseDirectBufferReader(buffer.duplicate());
  }

  @Override
  public void writeTo(DataOutput out) throws IOException {
    ByteBuffer dup = buffer.duplicate();
    dup.rewind();
    byte[] scratch = new byte[8192];
    while (dup.hasRemaining()) {
      int len = Math.min(scratch.length, dup.remaining());
      dup.get(scratch, 0, len);
      out.writeBytes(scratch, 0, len);
    }
  }
}
