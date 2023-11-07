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
 * Implementation of {@link BitPacker}. The behavior the is abstracted out here is how to write a
 * byte. This is useful as we can wire the byte-writing to byte[], stream or IndexInput, etc.
 */
abstract class BitPackerImplBase implements BitPacker {
  private long totalNumBytesWritten;
  private byte buffer;
  private int bufferNumBitsUsed;

  abstract void writeByte(byte b);

  /** {@inheritDoc}. value could be larger than 2^numBits - 1 but the higher bits won't be used. */
  @Override
  public void add(long value, int numBits) {
    assert numBits < 64;
    // clear bits higher than `numBits`
    value &= (1L << numBits) - 1;

    while (numBits > 0) {
      int bufferNumBitsRemaining = 8 - bufferNumBitsUsed;
      if (numBits < bufferNumBitsRemaining) {
        buffer |= (byte) (value << bufferNumBitsUsed);
        bufferNumBitsUsed += numBits;
        break;
      } else {
        long mask = (1L << bufferNumBitsRemaining) - 1;
        buffer |= (byte) ((value & mask) << bufferNumBitsUsed);
        numBits -= bufferNumBitsRemaining;
        value >>>= bufferNumBitsRemaining;
        writeByte(buffer);
        totalNumBytesWritten += 1;
        buffer = 0;
        bufferNumBitsUsed = 0;
      }
    }
  }

  @Override
  public void flush() {
    if (bufferNumBitsUsed > 0) {
      writeByte(buffer);
      bufferNumBitsUsed = 0;
    }
  }
}
