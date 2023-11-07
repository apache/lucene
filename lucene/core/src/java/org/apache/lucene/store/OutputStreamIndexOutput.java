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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import org.apache.lucene.util.BitUtil;

/** Implementation class for buffered {@link IndexOutput} that writes to an {@link OutputStream}. */
public class OutputStreamIndexOutput extends IndexOutput {

  private final CRC32 crc = new CRC32();
  private final XBufferedOutputStream os;

  private long bytesWritten = 0L;
  private boolean flushedOnClose = false;

  /**
   * Creates a new {@link OutputStreamIndexOutput} with the given buffer size.
   *
   * @param bufferSize the buffer size in bytes used to buffer writes internally.
   * @throws IllegalArgumentException if the given buffer size is less than <code>
   *     {@value Long#BYTES}</code>
   */
  public OutputStreamIndexOutput(
      String resourceDescription, String name, OutputStream out, int bufferSize) {
    super(resourceDescription, name);
    if (bufferSize < Long.BYTES) {
      throw new IllegalArgumentException("Buffer size too small, need: " + Long.BYTES);
    }
    this.os = new XBufferedOutputStream(new CheckedOutputStream(out, crc), bufferSize);
  }

  @Override
  public final void writeByte(byte b) throws IOException {
    os.write(b);
    bytesWritten++;
  }

  @Override
  public final void writeBytes(byte[] b, int offset, int length) throws IOException {
    os.write(b, offset, length);
    bytesWritten += length;
  }

  @Override
  public void writeShort(short i) throws IOException {
    os.writeShort(i);
    bytesWritten += Short.BYTES;
  }

  @Override
  public void writeInt(int i) throws IOException {
    os.writeInt(i);
    bytesWritten += Integer.BYTES;
  }

  @Override
  public void writeLong(long i) throws IOException {
    os.writeLong(i);
    bytesWritten += Long.BYTES;
  }

  @Override
  public void close() throws IOException {
    try (final OutputStream o = os) {
      // We want to make sure that os.flush() was running before close:
      // BufferedOutputStream may ignore IOExceptions while flushing on close().
      // We keep this also in Java 8, although it claims to be fixed there,
      // because there are more bugs around this! See:
      // # https://bugs.openjdk.java.net/browse/JDK-7015589
      // # https://bugs.openjdk.java.net/browse/JDK-8054565
      if (!flushedOnClose) {
        flushedOnClose = true; // set this BEFORE calling flush!
        o.flush();
      }
    }
  }

  @Override
  public final long getFilePointer() {
    return bytesWritten;
  }

  @Override
  public final long getChecksum() throws IOException {
    os.flush();
    return crc.getValue();
  }

  /** This subclass is an optimization for writing primitives. Don't use outside of this class! */
  private static final class XBufferedOutputStream extends BufferedOutputStream {

    XBufferedOutputStream(OutputStream out, int size) {
      super(out, size);
    }

    private void flushIfNeeded(int len) throws IOException {
      if (len > buf.length - count) {
        flush();
      }
    }

    void writeShort(short i) throws IOException {
      flushIfNeeded(Short.BYTES);
      BitUtil.VH_LE_SHORT.set(buf, count, i);
      count += Short.BYTES;
    }

    void writeInt(int i) throws IOException {
      flushIfNeeded(Integer.BYTES);
      BitUtil.VH_LE_INT.set(buf, count, i);
      count += Integer.BYTES;
    }

    void writeLong(long i) throws IOException {
      flushIfNeeded(Long.BYTES);
      BitUtil.VH_LE_LONG.set(buf, count, i);
      count += Long.BYTES;
    }
  }
}
