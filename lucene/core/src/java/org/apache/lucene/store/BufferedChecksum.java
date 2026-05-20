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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.zip.Checksum;
import org.apache.lucene.util.BitUtil;

/** Wraps another {@link Checksum} with an internal buffer to speed up checksum calculations. */
public class BufferedChecksum implements Checksum {
  private final Checksum in;
  private final byte[] buffer;
  private int upto;

  /** Default buffer size: 1024 */
  public static final int DEFAULT_BUFFERSIZE = 1024;

  /** Create a new BufferedChecksum with {@link #DEFAULT_BUFFERSIZE} */
  public BufferedChecksum(Checksum in) {
    this(in, DEFAULT_BUFFERSIZE);
  }

  /** Create a new BufferedChecksum with the specified bufferSize */
  public BufferedChecksum(Checksum in, int bufferSize) {
    this.in = in;
    this.buffer = new byte[bufferSize];
  }

  @Override
  public void update(int b) {
    if (upto == buffer.length) {
      flush();
    }
    buffer[upto++] = (byte) b;
  }

  @Override
  public void update(byte[] b, int off, int len) {
    if (len >= buffer.length) {
      flush();
      in.update(b, off, len);
    } else {
      if (upto + len > buffer.length) {
        flush();
      }
      System.arraycopy(b, off, buffer, upto, len);
      upto += len;
    }
  }

  void updateShort(short val) {
    if (upto + Short.BYTES > buffer.length) flush();
    BitUtil.VH_LE_SHORT.set(buffer, upto, val);
    upto += Short.BYTES;
  }

  void updateInt(int val) {
    if (upto + Integer.BYTES > buffer.length) flush();
    BitUtil.VH_LE_INT.set(buffer, upto, val);
    upto += Integer.BYTES;
  }

  void updateLong(long val) {
    if (upto + Long.BYTES > buffer.length) flush();
    BitUtil.VH_LE_LONG.set(buffer, upto, val);
    upto += Long.BYTES;
  }

  void updateLongs(long[] vals, int offset, int len) {
    if (upto > 0) {
      int remainingCapacityInLong = Math.min((buffer.length - upto) / Long.BYTES, len);
      for (int i = 0; i < remainingCapacityInLong; i++, offset++, len--) {
        updateLong(vals[offset]);
      }
      if (0 == len) return;
    }

    LongBuffer b = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    final int capacityInLong = buffer.length / Long.BYTES;
    while (len > 0) {
      flush();
      int l = Math.min(capacityInLong, len);
      b.put(0, vals, offset, l);
      upto += l * Long.BYTES;
      offset += l;
      len -= l;
    }
  }

  @Override
  public long getValue() {
    flush();
    return in.getValue();
  }

  @Override
  public void reset() {
    upto = 0;
    in.reset();
  }

  private void flush() {
    if (upto > 0) {
      in.update(buffer, 0, upto);
    }
    upto = 0;
  }
}
