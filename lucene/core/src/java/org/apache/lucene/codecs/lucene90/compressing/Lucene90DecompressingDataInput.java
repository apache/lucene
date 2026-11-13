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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.EOFException;
import java.io.IOException;
import org.apache.lucene.store.DataInput;

/**
 * A {@link DataInput} over decompressed data that can expose partial skip progress.
 *
 * @lucene.internal
 */
public abstract class Lucene90DecompressingDataInput extends DataInput {

  /**
   * Skip up to {@code numBytes} decompressed bytes.
   *
   * @return the number of bytes that were actually skipped
   */
  public abstract long skipBytesUpTo(long numBytes) throws IOException;

  /**
   * Read up to {@code len} decompressed bytes.
   *
   * @return the number of bytes that were actually read
   */
  public int readBytesUpTo(byte[] b, int offset, int len) throws IOException {
    int read = 0;
    while (read < len) {
      try {
        b[offset + read] = readByte();
        ++read;
      } catch (EOFException _) {
        return read;
      }
    }
    return read;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    int read = 0;
    while (read < len) {
      final int actualRead = readBytesUpTo(b, offset + read, len - read);
      if (actualRead == 0) {
        throw new EOFException();
      }
      read += actualRead;
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    final long skipped = skipBytesUpTo(numBytes);
    if (skipped < numBytes) {
      throw new EOFException();
    }
  }
}
