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
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.lucene.internal.tests.TestSecrets;

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
}
