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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.IndexOutput;

/** An {@link IndexOutput} that wraps another instance and tracks the number of bytes written */
public class ByteTrackingIndexOutput extends IndexOutput {

  private final IndexOutput output;
  private final AtomicLong byteTracker;
  private boolean closed = false;

  protected ByteTrackingIndexOutput(IndexOutput output, AtomicLong byteTracker) {
    super(
        "Byte tracking wrapper for: " + output.getName(),
        "ByteTrackingIndexOutput{" + output.getName() + "}");
    this.output = output;
    this.byteTracker = byteTracker;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    output.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    output.writeBytes(b, offset, length);
  }

  @Override
  public void writeShort(short i) throws IOException {
    output.writeShort(i);
  }

  @Override
  public void writeInt(int i) throws IOException {
    output.writeInt(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    output.writeLong(i);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      output.close();
      return;
    }
    byteTracker.addAndGet(output.getFilePointer());
    closed = true;
    output.close();
  }

  @Override
  public long getFilePointer() {
    return output.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return output.getChecksum();
  }

  public String getWrappedName() {
    return output.getName();
  }

  public String getWrappedToString() {
    return output.toString();
  }
}
