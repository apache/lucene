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
package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.IndexInput;

/**
 * InputStream semantics for reading from an IndexInput
 */
public class SegmentInputStream extends InputStream {

  /** */
  private final IndexInput indexInput;

  public final long initialFilePointerPosition;
  public final long limit;
  public long pos = 0;

  // TODO: This input stream needs to be modified to enable buffering.
  public SegmentInputStream(IndexInput indexInput, long limit, long initialFilePointerPosition)
      throws IOException {
    super();
    this.indexInput = indexInput;
    this.initialFilePointerPosition = initialFilePointerPosition;
    this.limit = limit;

    this.indexInput.seek(initialFilePointerPosition);
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b, int off, int len) {
    try {
      long avail = limit - pos;
      if (pos >= limit) {
        return -1;
      }
      if (len > avail) {
        len = (int) avail;
      }
      if (len <= 0) {
        return 0;
      }
      indexInput.readBytes(b, off, len);
      pos += len;
      return len;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() throws IOException {
    indexInput.seek(initialFilePointerPosition);
    pos = 0;
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // Do nothing for now.
  }

  @Override
  public int available() {
    throw new UnsupportedOperationException();
  }
}
