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
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IndexOutput;

/** A {@link IndexOutput} wrapper that changes the endianness of the provided index output. */
final class EndiannessReverserIndexOutput extends FilterIndexOutput {

  EndiannessReverserIndexOutput(IndexOutput out) {
    super("Endianness reverser Index Output wrapper", out.getName(), out);
  }

  @Override
  public String toString() {
    return out.getName();
  }

  @Override
  public void writeInt(int i) throws IOException {
    out.writeInt(Integer.reverseBytes(i));
  }

  @Override
  public void writeShort(short i) throws IOException {
    out.writeShort(Short.reverseBytes(i));
  }

  @Override
  public void writeLong(long i) throws IOException {
    out.writeLong(Long.reverseBytes(i));
  }
}
