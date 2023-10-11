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

/**
 * IndexOutput implementation that delegates calls to another directory. This class can be used to
 * add limitations on top of an existing {@link IndexOutput} implementation such as {@link
 * ByteBuffersIndexOutput} or to add additional sanity checks for tests. However, if you plan to
 * write your own {@link IndexOutput} implementation, you should consider extending directly {@link
 * IndexOutput} or {@link DataOutput} rather than try to reuse functionality of existing {@link
 * IndexOutput}s by extending this class.
 *
 * @lucene.internal
 */
public class FilterIndexOutput extends IndexOutput {

  /**
   * Unwraps all FilterIndexOutputs until the first non-FilterIndexOutput IndexOutput instance and
   * returns it
   */
  public static IndexOutput unwrap(IndexOutput out) {
    while (out instanceof FilterIndexOutput) {
      out = ((FilterIndexOutput) out).out;
    }
    return out;
  }

  protected final IndexOutput out;

  /**
   * Creates a FilterIndexOutput with a resource description, name, and wrapped delegate IndexOutput
   */
  protected FilterIndexOutput(String resourceDescription, String name, IndexOutput out) {
    super(resourceDescription, name);
    this.out = out;
  }

  /** Gets the delegate that was passed in on creation */
  public final IndexOutput getDelegate() {
    return out;
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public long getFilePointer() {
    return out.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return out.getChecksum();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    out.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    out.writeBytes(b, offset, length);
  }
}
