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
import org.apache.lucene.util.RandomAccessInputRef;

/**
 * DataInput backed by a {@link RandomAccessInput}. <b>WARNING:</b> This class omits all low-level
 * checks.
 *
 * @lucene.experimental
 */
public final class RandomAccessInputDataInput extends DataInput {

  private RandomAccessInput input;
  private long length;
  private long offset;

  public RandomAccessInputDataInput() {}

  /** Sets the current position for this {@link DataInput}. */
  public long getPosition() {
    return offset;
  }

  /** Sets the current position for this {@link DataInput}. */
  public void setPosition(long pos) {
    this.offset = pos;
  }

  /** Resets the input to a new {@link RandomAccessInput} at position 0. */
  public void reset(RandomAccessInputRef input) {
    this.input = input.bytes;
    this.offset = input.offset;
    this.length = input.length;
  }

  /** The total number of bytes on this {@link DataInput}. */
  public long length() {
    return length;
  }

  @Override
  public void skipBytes(long count) {
    offset += count;
  }

  @Override
  public short readShort() throws IOException {
    try {
      return input.readShort(offset);
    } finally {
      offset += Short.BYTES;
    }
  }

  @Override
  public int readInt() throws IOException {
    try {
      return input.readInt(offset);
    } finally {
      offset += Integer.BYTES;
    }
  }

  @Override
  public long readLong() throws IOException {
    try {
      return input.readLong(offset);
    } finally {
      offset += Long.BYTES;
    }
  }

  @Override
  public byte readByte() throws IOException {
    return input.readByte(offset++);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    input.readBytes(this.offset, b, offset, len);
    this.offset += len;
  }
}
