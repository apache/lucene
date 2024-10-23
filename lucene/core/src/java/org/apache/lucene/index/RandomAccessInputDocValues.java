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

package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * A per-document binary value.
 *
 * @lucene.experimental
 */
public abstract class RandomAccessInputDocValues extends DocValuesIterator {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected RandomAccessInputDocValues() {}

  /**
   * Returns the binary value as a {@link RandomAccessInput} for the current document ID. The bytes
   * start at position 0 up to {@link RandomAccessInput#length()}. It is illegal to call this method
   * after {@link #advanceExact(int)} returned {@code false}.
   *
   * @return the binary value as a {@link RandomAccessInput}
   */
  public abstract RandomAccessInput randomAccessInputValue() throws IOException;

  /** Wraps a {@link BinaryDocValues} with a {@link RandomAccessInputDocValues} instance. */
  public static RandomAccessInputDocValues fromBinaryDocValues(BinaryDocValues binaryDocValues) {
    return new RandomAccessInputDocValues() {
      final ByteArrayRandomAccessInput value = new ByteArrayRandomAccessInput();

      @Override
      public RandomAccessInput randomAccessInputValue() throws IOException {
        value.reset(binaryDocValues.binaryValue());
        return value;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return binaryDocValues.advanceExact(target);
      }

      @Override
      public int docID() {
        return binaryDocValues.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return binaryDocValues.nextDoc();
      }

      @Override
      public int advance(int target) throws IOException {
        return binaryDocValues.advance(target);
      }

      @Override
      public long cost() {
        return binaryDocValues.cost();
      }
    };
  }

  private static class ByteArrayRandomAccessInput implements RandomAccessInput {

    private BytesRef bytesRef;

    ByteArrayRandomAccessInput() {}

    @Override
    public long length() {
      return bytesRef.length;
    }

    void reset(BytesRef bytesRef) {
      this.bytesRef = bytesRef;
    }

    @Override
    public byte readByte(long pos) {
      return bytesRef.bytes[bytesRef.offset + (int) pos];
    }

    @Override
    public short readShort(long pos) {
      return (short) BitUtil.VH_LE_SHORT.get(bytesRef.bytes, bytesRef.offset + (int) pos);
    }

    @Override
    public int readInt(long pos) {
      return (int) BitUtil.VH_LE_INT.get(bytesRef.bytes, bytesRef.offset + (int) pos);
    }

    @Override
    public long readLong(long pos) {
      return (long) BitUtil.VH_LE_LONG.get(bytesRef.bytes, bytesRef.offset + (int) pos);
    }
  }
}
