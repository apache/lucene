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
package org.apache.lucene.util.fst;

import static org.apache.lucene.store.ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP;
import static org.apache.lucene.store.ByteBuffersDataOutput.NO_REUSE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;

/**
 * An adapter class to use {@link ByteBuffersDataOutput} as a {@link FSTReader}. It allows the FST
 * to be readable immediately after writing
 */
final class ReadWriteDataOutput extends DataOutput implements FSTReader {

  private final ByteBuffersDataOutput dataOutput;
  private final int blockBits;
  private final int blockSize;
  private final int blockMask;
  private List<ByteBuffer> byteBuffers;
  // whether this DataOutput is already frozen
  private boolean frozen;

  public ReadWriteDataOutput(int blockBits) {
    this.dataOutput =
        new ByteBuffersDataOutput(blockBits, blockBits, ALLOCATE_BB_ON_HEAP, NO_REUSE);
    this.blockBits = blockBits;
    this.blockSize = 1 << blockBits;
    this.blockMask = blockSize - 1;
  }

  @Override
  public void writeByte(byte b) {
    assert frozen == false;
    dataOutput.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) {
    assert frozen == false;
    dataOutput.writeBytes(b, offset, length);
  }

  @Override
  public long ramBytesUsed() {
    return dataOutput.ramBytesUsed();
  }

  public void freeze() {
    frozen = true;
    // this operation is costly, so we want to compute it once and cache
    this.byteBuffers = dataOutput.toWriteableBufferList();
    // ensure the ByteBuffer internal array is accessible. The call to toWriteableBufferList() above
    // would ensure that it is accessible.
    assert byteBuffers.stream().allMatch(ByteBuffer::hasArray);
  }

  @Override
  public FST.BytesReader getReverseBytesReader() {
    assert byteBuffers != null; // freeze() must be called first
    if (byteBuffers.size() == 1) {
      // use a faster implementation for single-block case
      return new ReverseBytesReader(byteBuffers.get(0).array());
    }
    return new FST.BytesReader() {
      private byte[] current = byteBuffers.get(0).array();
      private int nextBuffer = -1;
      private int nextRead;

      @Override
      public byte readByte() {
        if (nextRead == -1) {
          current = byteBuffers.get(nextBuffer--).array();
          nextRead = blockSize - 1;
        }
        return current[nextRead--];
      }

      @Override
      public void skipBytes(long count) {
        setPosition(getPosition() - count);
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) {
        for (int i = 0; i < len; i++) {
          b[offset + i] = readByte();
        }
      }

      @Override
      public long getPosition() {
        return ((long) nextBuffer + 1) * blockSize + nextRead;
      }

      @Override
      public void setPosition(long pos) {
        int bufferIndex = (int) (pos >> blockBits);
        if (nextBuffer != bufferIndex - 1) {
          nextBuffer = bufferIndex - 1;
          current = byteBuffers.get(bufferIndex).array();
        }
        nextRead = (int) (pos & blockMask);
        assert getPosition() == pos : "pos=" + pos + " getPos()=" + getPosition();
      }
    };
  }

  @Override
  public void writeTo(DataOutput out) throws IOException {
    dataOutput.copyTo(out);
  }
}
