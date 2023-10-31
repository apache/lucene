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

import org.apache.lucene.util.ByteBlockPool;

import java.io.IOException;

/** Reads in reverse from a ByteBlockPool. */
final class ByteBlockPoolReverseBytesReader extends FST.BytesReader {

  private final ByteBlockPool buf;
  private final long relativePos;
  private long pos;

  public ByteBlockPoolReverseBytesReader(ByteBlockPool buf, long relativePos) {
    this.buf = buf;
    this.relativePos = relativePos;
  }

  @Override
  public byte readByte() throws IOException {
    return buf.readByte(pos--);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    // first read the bytes as-is
    buf.readBytes(pos, b, offset, len);
    pos -= len;

    // then revert the bytes
    int limit = len / 2;
    for (int i = 0; i < limit; i++) {
      byte tmp = b[offset + i];
      b[offset + i] = b[offset + len - i - 1];
      b[offset + len - i - 1] = tmp;
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    pos -= numBytes;
  }

  @Override
  public long getPosition() {
    return pos + relativePos;
  }

  @Override
  public void setPosition(long pos) {
    this.pos = pos - relativePos;
  }

  @Override
  public boolean reversed() {
    return true;
  }
}
