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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

/**
 * A per-document binary value.
 *
 * @lucene.experimental
 */
public abstract class DataInputDocValues extends DocValuesIterator {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected DataInputDocValues() {}

  /**
   * Returns the binary value wrapped as a {@link DataInputDocValue} for the current document ID. It
   * is illegal to call this method after {@link #advanceExact(int)} returned {@code false}.
   *
   * @return the binary value wrapped as a {@link DataInputDocValue}
   */
  public abstract DataInputDocValue dataInputValue() throws IOException;

  /**
   * A {@link DataInput} view over a binary doc value which is positional aware.
   *
   * @lucene.experimental
   */
  public abstract static class DataInputDocValue extends DataInput {

    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    public DataInputDocValue() {}

    /** Sets the position in this stream. */
    public abstract void setPosition(int pos) throws IOException;

    /** Returns the current position in this stream. */
    public abstract int getPosition() throws IOException;
  }

  /** Wraps a {@link BinaryDocValues} with a {@link DataInputDocValues} instance. */
  public static DataInputDocValues fromBinaryDocValues(BinaryDocValues binaryDocValues) {
    final ByteArrayDataInput dataInput = new ByteArrayDataInput();
    final DataInputDocValue value = new DataInputDocValue() {
      @Override
      public void setPosition(int pos) {
        dataInput.setPosition(pos);
      }

      @Override
      public int getPosition() {
        return dataInput.getPosition();
      }

      @Override
      public byte readByte() {
        return dataInput.readByte();
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) {
         dataInput.readBytes(b, offset, len);
      }

      @Override
      public void skipBytes(long numBytes) {
        dataInput.skipBytes(numBytes);
      }
    };
    return new DataInputDocValues() {
      @Override
      public DataInputDocValue dataInputValue() throws IOException {
        BytesRef bytes = binaryDocValues.binaryValue();
        dataInput.reset(bytes.bytes, bytes.offset, bytes.length);
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
}
