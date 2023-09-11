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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;

/** A dummy DataOutput class that just counts how many bytes have been written. */
public class ByteCountDataOutput extends DataOutput {
  private Long byteCount;

  /** Default constructor */
  ByteCountDataOutput() {
    this.byteCount = 0L;
  }

  /**
   * Constructor Syntactic sugar for ByteCountDataOutput out; out.writeVInt(val);
   *
   * @param val the int value for which bytes to be written will be counted
   */
  ByteCountDataOutput(int val) {
    this();
    try {
      writeVInt(val);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructor Syntactic sugar for ByteCountDataOutput out; out.writeVLong(val);
   *
   * @param val the long value for which bytes to be written will be counted
   */
  ByteCountDataOutput(long val) {
    this();
    try {
      writeVLong(val);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void reset() {
    this.byteCount = 0L;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    byteCount++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    byteCount += length;
  }

  Long getByteCount() {
    return byteCount;
  }
}
