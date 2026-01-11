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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.lucene.internal.tests.TestSecrets;

/**
 * IndexInput implementation that delegates calls to another IndexInput. This class can be used to
 * add limitations on top of an existing {@link IndexInput} implementation or to add additional
 * sanity checks for tests. However, if you plan to write your own {@link IndexInput}
 * implementation, you should consider extending directly {@link IndexInput} or {@link DataInput}
 * rather than try to reuse functionality of existing {@link IndexInput}s by extending this class.
 *
 * @lucene.internal
 */
public class FilterIndexInput extends IndexInput {

  static final CopyOnWriteArrayList<Class<?>> TEST_FILTER_INPUTS = new CopyOnWriteArrayList<>();

  static {
    TestSecrets.setFilterInputIndexAccess(TEST_FILTER_INPUTS::add);
  }

  /**
   * Unwraps all FilterIndexInputs until the first non-FilterIndexInput IndexInput instance and
   * returns it
   */
  public static IndexInput unwrap(IndexInput in) {
    while (in instanceof FilterIndexInput) {
      in = ((FilterIndexInput) in).in;
    }
    return in;
  }

  /**
   * Unwraps all test FilterIndexInputs until the first non-test FilterIndexInput IndexInput
   * instance and returns it
   */
  public static IndexInput unwrapOnlyTest(IndexInput in) {
    while (in instanceof FilterIndexInput && TEST_FILTER_INPUTS.contains(in.getClass())) {
      in = ((FilterIndexInput) in).in;
    }
    return in;
  }

  protected final IndexInput in;

  /** Creates a FilterIndexInput with a resource description and wrapped delegate IndexInput */
  public FilterIndexInput(String resourceDescription, IndexInput in) {
    super(resourceDescription);
    this.in = in;
  }

  /** Gets the delegate that was passed in on creation */
  public IndexInput getDelegate() {
    return in;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public IndexInput clone() {
    throw new RuntimeException(
        "Clone not implemented by the filter class: " + getClass().getName());
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
  public long length() {
    return in.length();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return in.slice(sliceDescription, offset, length);
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
  public Set<String> readSetOfStrings() throws IOException {
    return in.readSetOfStrings();
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    return in.readMapOfStrings();
  }

  @Override
  public String readString() throws IOException {
    return in.readString();
  }

  @Override
  public long readZLong() throws IOException {
    return in.readZLong();
  }

  @Override
  public long readVLong() throws IOException {
    return in.readVLong();
  }

  @Override
  public void readFloats(float[] floats, int offset, int len) throws IOException {
    in.readFloats(floats, offset, len);
  }

  @Override
  public void readInts(int[] dst, int offset, int length) throws IOException {
    in.readInts(dst, offset, length);
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    in.readLongs(dst, offset, length);
  }

  @Override
  public long readLong() throws IOException {
    return in.readLong();
  }

  @Override
  public int readZInt() throws IOException {
    return in.readZInt();
  }

  @Override
  public int readVInt() throws IOException {
    return in.readVInt();
  }

  @Override
  public int readInt() throws IOException {
    return in.readInt();
  }

  @Override
  public short readShort() throws IOException {
    return in.readShort();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public void prefetch(long offset, long length) throws IOException {
    in.prefetch(offset, length);
  }

  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    return in.randomAccessSlice(offset, length);
  }

  @Override
  public void updateIOContext(IOContext context) throws IOException {
    in.updateIOContext(context);
  }

  @Override
  public Optional<Boolean> isLoaded() {
    return in.isLoaded();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length, IOContext context)
      throws IOException {
    return in.slice(sliceDescription, offset, length, context);
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    in.skipBytes(numBytes);
  }
}
