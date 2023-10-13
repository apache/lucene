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
package org.apache.lucene.backward_codecs.store;

import java.io.IOException;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/** A {@link IndexInput} wrapper that changes the endianness of the provided index input. */
final class EndiannessReverserIndexInput extends FilterIndexInput {

  EndiannessReverserIndexInput(IndexInput in) {
    super("Endianness reverser Index Input wrapper", in);
  }

  @Override
  public short readShort() throws IOException {
    return Short.reverseBytes(in.readShort());
  }

  @Override
  public int readInt() throws IOException {
    return Integer.reverseBytes(in.readInt());
  }

  @Override
  public long readLong() throws IOException {
    return Long.reverseBytes(in.readLong());
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    in.readLongs(dst, offset, length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = Long.reverseBytes(dst[offset + i]);
    }
  }

  @Override
  public void readInts(int[] dst, int offset, int length) throws IOException {
    in.readInts(dst, offset, length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = Integer.reverseBytes(dst[offset + i]);
    }
  }

  @Override
  public void readFloats(float[] dst, int offset, int length) throws IOException {
    in.readFloats(dst, offset, length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] =
          Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(dst[offset + i])));
    }
  }

  @Override
  public IndexInput clone() {
    return new EndiannessReverserIndexInput(in.clone());
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return new EndiannessReverserIndexInput(in.slice(sliceDescription, offset, length));
  }

  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    return new EndiannessReverserRandomAccessInput(in.randomAccessSlice(offset, length));
  }

  /**
   * A {@link RandomAccessInput} wrapper that changes the endianness of the provided index input.
   *
   * @lucene.internal
   */
  public static class EndiannessReverserRandomAccessInput implements RandomAccessInput {

    private final RandomAccessInput in;

    public EndiannessReverserRandomAccessInput(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long length() {
      return in.length();
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return in.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return Short.reverseBytes(in.readShort(pos));
    }

    @Override
    public int readInt(long pos) throws IOException {
      return Integer.reverseBytes(in.readInt(pos));
    }

    @Override
    public long readLong(long pos) throws IOException {
      return Long.reverseBytes(in.readLong(pos));
    }
  }
}
