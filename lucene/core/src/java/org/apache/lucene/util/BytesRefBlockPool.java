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

package org.apache.lucene.util;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.Arrays;

/**
 * Represents a logical list of ByteRef backed by a {@link ByteBlockPool}. It uses up to two bytes
 * to record the length of the BytesRef followed by the actual bytes. They can be read using the
 * start position returned when they are appended.
 *
 * <p>The {@link BytesRef} is written so it never crosses the {@link ByteBlockPool#BYTE_BLOCK_SIZE}
 * boundary. The limit of the largest {@link BytesRef} is therefore {@link
 * ByteBlockPool#BYTE_BLOCK_SIZE}-2 bytes.
 *
 * @lucene.internal
 */
public class BytesRefBlockPool implements Accountable {

  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(BytesRefBlockPool.class);

  private final ByteBlockPool byteBlockPool;

  public BytesRefBlockPool() {
    this.byteBlockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
  }

  public BytesRefBlockPool(ByteBlockPool byteBlockPool) {
    this.byteBlockPool = byteBlockPool;
  }

  /** Reset this buffer to the empty state. */
  void reset() {
    byteBlockPool.reset(false, false); // we don't need to 0-fill the buffers
  }

  /**
   * Populates the given BytesRef with the term starting at <i>start</i>.
   *
   * @see #fillBytesRef(BytesRef, int)
   */
  public void fillBytesRef(BytesRef term, int start) {
    final byte[] bytes = term.bytes = byteBlockPool.getBuffer(start >> BYTE_BLOCK_SHIFT);
    int pos = start & BYTE_BLOCK_MASK;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      term.length = bytes[pos];
      term.offset = pos + 1;
    } else {
      // length is 2 bytes
      term.length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      term.offset = pos + 2;
    }
    assert term.length >= 0;
  }

  /**
   * Add a term returning the start position on the underlying {@link ByteBlockPool}. THis can be
   * used to read back the value using {@link #fillBytesRef(BytesRef, int)}.
   *
   * @see #fillBytesRef(BytesRef, int)
   */
  public int addBytesRef(BytesRef bytes) {
    final int length = bytes.length;
    final int len2 = 2 + bytes.length;
    if (len2 + byteBlockPool.byteUpto > BYTE_BLOCK_SIZE) {
      if (len2 > BYTE_BLOCK_SIZE) {
        throw new BytesRefHash.MaxBytesLengthExceededException(
            "bytes can be at most " + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
      }
      byteBlockPool.nextBuffer();
    }
    final byte[] buffer = byteBlockPool.buffer;
    final int bufferUpto = byteBlockPool.byteUpto;
    final int textStart = bufferUpto + byteBlockPool.byteOffset;

    // We first encode the length, followed by the
    // bytes. Length is encoded as vInt, but will consume
    // 1 or 2 bytes at most (we reject too-long terms,
    // above).
    if (length < 128) {
      // 1 byte to store length
      buffer[bufferUpto] = (byte) length;
      byteBlockPool.byteUpto += length + 1;
      assert length >= 0 : "Length must be positive: " + length;
      System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1, length);
    } else {
      // 2 byte to store length
      BitUtil.VH_BE_SHORT.set(buffer, bufferUpto, (short) (length | 0x8000));
      byteBlockPool.byteUpto += length + 2;
      System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2, length);
    }
    return textStart;
  }

  /**
   * Computes the hash of the BytesRef at the given start. This is equivalent of doing:
   *
   * <pre>
   *     BytesRef bytes = new BytesRef();
   *     fillTerm(bytes, start);
   *     BytesRefHash.doHash(bytes.bytes, bytes.pos, bytes.len);
   *  </pre>
   *
   * It just saves the work of filling the BytesRef.
   */
  int hash(int start) {
    final int offset = start & BYTE_BLOCK_MASK;
    final byte[] bytes = byteBlockPool.getBuffer(start >> BYTE_BLOCK_SHIFT);
    final int len;
    int pos;
    if ((bytes[offset] & 0x80) == 0) {
      // length is 1 byte
      len = bytes[offset];
      pos = offset + 1;
    } else {
      len = ((short) BitUtil.VH_BE_SHORT.get(bytes, offset)) & 0x7FFF;
      pos = offset + 2;
    }
    return BytesRefHash.doHash(bytes, pos, len);
  }

  /**
   * Computes the equality between the BytesRef at the start position with the provided BytesRef.
   * This is equivalent of doing:
   *
   * <pre>
   *     BytesRef bytes = new BytesRef();
   *     fillTerm(bytes, start);
   *     Arrays.equals(bytes.bytes, bytes.offset, bytes.offset + length, b.bytes, b.offset, b.offset + b.length);
   *  </pre>
   *
   * It just saves the work of filling the BytesRef.
   */
  boolean equals(int start, BytesRef b) {
    final byte[] bytes = byteBlockPool.getBuffer(start >> BYTE_BLOCK_SHIFT);
    int pos = start & BYTE_BLOCK_MASK;
    final int length;
    final int offset;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      length = bytes[pos];
      offset = pos + 1;
    } else {
      // length is 2 bytes
      length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      offset = pos + 2;
    }
    return Arrays.equals(bytes, offset, offset + length, b.bytes, b.offset, b.offset + b.length);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES + byteBlockPool.ramBytesUsed();
  }
}
