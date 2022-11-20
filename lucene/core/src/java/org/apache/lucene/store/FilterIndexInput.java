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
import java.util.Map;
import java.util.Set;

/**
 * IndexInput implementation that delegates calls to another directory. This class can be used to
 * add limitations on top of an existing {@link IndexInput} implementation or to add additional
 * sanity checks for tests. However, if you plan to write your own {@link IndexInput}
 * implementation, you should consider extending directly {@link IndexInput} or {@link DataInput}
 * rather than try to reuse functionality of existing {@link IndexInput}s by extending this class.
 *
 * @lucene.internal
 */
public class FilterIndexInput extends IndexInput {

  public static IndexInput unwrap(IndexInput in) {
    while (in instanceof FilterIndexInput) {
      in = ((FilterIndexInput) in).in;
    }
    return in;
  }

  protected final IndexInput in;

  public FilterIndexInput(String resourceDescription, IndexInput in) {
    super(resourceDescription);
    this.in = in;
  }

  public IndexInput getDelegate() {
    return in;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long getFilePointer() {
    return in.getFilePointer();
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos);
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    in.skipBytes(numBytes);
  }

  @Override
  public long length() {
    return in.length();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return in.slice(sliceDescription, offset, length);
  }

  @Override
  protected String getFullSliceDescription(String sliceDescription) {
    return in.getFullSliceDescription(sliceDescription);
  }

  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    return in.randomAccessSlice(offset, length);
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    in.readBytes(b, offset, len);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public short readShort() throws IOException {
    return in.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return in.readInt();
  }

  @Override
  public int readVInt() throws IOException {
    return in.readVInt();
  }

  @Override
  public int readZInt() throws IOException {
    return in.readZInt();
  }

  @Override
  public long readLong() throws IOException {
    return in.readLong();
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    in.readLongs(dst, offset, length);
  }

  @Override
  public void readInts(int[] dst, int offset, int length) throws IOException {
    in.readInts(dst, offset, length);
  }

  @Override
  public void readFloats(float[] floats, int offset, int len) throws IOException {
    in.readFloats(floats, offset, len);
  }

  @Override
  public long readVLong() throws IOException {
    return in.readVLong();
  }

  @Override
  public long readZLong() throws IOException {
    return in.readZLong();
  }

  @Override
  public String readString() throws IOException {
    return in.readString();
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    return in.readMapOfStrings();
  }

  @Override
  public Set<String> readSetOfStrings() throws IOException {
    return in.readSetOfStrings();
  }
}
