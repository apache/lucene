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

/** InputStream for reading from an IndexInput. */
final class IndexInputInputStream extends InputStream {

  final IndexInput in;
  long pos = 0;
  final long limit;

  IndexInputInputStream(IndexInput in) {
    this.in = in;
    this.limit = in.length();
  }

  @Override
  public int read() throws IOException {
    if (pos >= limit) {
      return -1;
    }
    pos++;
    return in.readByte();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len <= 0) {
      return 0;
    }
    if (pos >= limit) {
      return -1;
    }
    long avail = limit - pos;
    if (len > avail) {
      len = (int) avail;
    }
    in.readBytes(b, off, len);
    pos += len;
    return len;
  }
}
