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

import java.io.IOException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;

/**
 * An adapter class to use {@link ByteBuffersDataOutput} as a {@link FSTReader}. It allows the FST
 * to be readable immediately after writing
 */
final class ReadWriteDataOutput extends DataOutput implements FSTReader {

  private final ByteBuffersDataOutput dataOutput;
  // the DataInput to read from once we finish writing
  private ByteBuffersDataInput dataInput;
  // whether this DataOutput is already frozen
  private boolean frozen;

  public ReadWriteDataOutput(ByteBuffersDataOutput dataOutput) {
    this.dataOutput = dataOutput;
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
    // this operation are costly, so we want to compute it once and cache
    dataInput = dataOutput.toDataInput();
  }

  @Override
  public FST.BytesReader getReverseBytesReader() {
    assert dataInput != null; // freeze() must be called first
    return new ReverseRandomAccessReader(dataInput);
  }

  @Override
  public void writeTo(DataOutput out) throws IOException {
    dataOutput.copyTo(out);
  }
}
