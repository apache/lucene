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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking;

/**
 * A {@link BitPacker} implementation that requires user to know the size of the resulting byte
 * array upfront, in order to avoid allocation and copying for dynamically growing the array.
 */
public final class FixedSizeByteArrayBitPacker extends BitPackerImplBase {
  private final byte[] bytes;
  private int numBytesUsed;

  public FixedSizeByteArrayBitPacker(int capacity) {
    this.bytes = new byte[capacity];
  }

  @Override
  void writeByte(byte b) {
    assert numBytesUsed < bytes.length;
    bytes[numBytesUsed++] = b;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
