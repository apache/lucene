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
import org.apache.lucene.util.ByteBlockPool;

/** Reads in reverse from a ByteBlockPool. */
final class ByteBlockPoolReverseBytesReader extends FST.BytesReader {

  private final ByteBlockPool buf;
  // the difference between the FST node address and the hash table copied node address
  private long posDelta;
  private long pos;

  public ByteBlockPoolReverseBytesReader(ByteBlockPool buf) {
    this.buf = buf;
  }

  @Override
  public byte readByte() {
    return buf.readByte(pos--);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    for (int i = 0; i < len; i++) {
      b[offset + i] = buf.readByte(pos--);
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    pos -= numBytes;
  }

  @Override
  public long getPosition() {
    return pos + posDelta;
  }

  @Override
  public void setPosition(long pos) {
    this.pos = pos - posDelta;
  }

  public void setPosDelta(long posDelta) {
    this.posDelta = posDelta;
  }
}
