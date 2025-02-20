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
import java.io.OutputStream;
import org.apache.lucene.store.IndexOutput;

/** OutputStream for writing into an IndexOutput */
final class IndexOutputOutputStream extends OutputStream {

  static final int DEFAULT_BUFFER_SIZE = 8192;

  final IndexOutput out;
  final int bufferSize;
  final byte[] buffer;
  int idx;

  IndexOutputOutputStream(IndexOutput out) {
    this(out, DEFAULT_BUFFER_SIZE);
  }

  IndexOutputOutputStream(IndexOutput out, int bufferSize) {
    this.out = out;
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
  }

  @Override
  public void write(int b) throws IOException {
    buffer[idx] = (byte) b;
    idx++;
    if (idx == bufferSize) {
      flush();
    }
  }

  @Override
  public void write(byte[] b, int offset, int length) throws IOException {
    if (idx != 0) {
      flush();
    }
    out.writeBytes(b, offset, length);
  }

  @Override
  public void flush() throws IOException {
    out.writeBytes(buffer, 0, idx);
    idx = 0;
  }

  @Override
  public void close() throws IOException {
    this.flush();
  }
}
