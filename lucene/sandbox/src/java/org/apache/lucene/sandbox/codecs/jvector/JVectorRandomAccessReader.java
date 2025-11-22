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

package org.apache.lucene.sandbox.codecs.jvector;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import org.apache.lucene.store.IndexInput;

/// Implements JVector reader capabilities over a Lucene IndexInput
public class JVectorRandomAccessReader implements RandomAccessReader {
  private final byte[] internalBuffer = new byte[Long.BYTES];
  private final IndexInput indexInputDelegate;

  public JVectorRandomAccessReader(IndexInput indexInputDelegate) {
    this.indexInputDelegate = indexInputDelegate;
  }

  @Override
  public void seek(long offset) throws IOException {
    indexInputDelegate.seek(offset);
  }

  @Override
  public long getPosition() throws IOException {
    return indexInputDelegate.getFilePointer();
  }

  @Override
  public int readInt() throws IOException {
    return indexInputDelegate.readInt();
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(indexInputDelegate.readInt());
  }

  @Override
  public long readLong() throws IOException {
    return indexInputDelegate.readLong();
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    indexInputDelegate.readBytes(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(ByteBuffer buffer) throws IOException {
    // validate that the requested bytes actually exist ----
    long remainingInFile = indexInputDelegate.length() - indexInputDelegate.getFilePointer();
    if (buffer.remaining() > remainingInFile) {
      throw new EOFException(
          "Requested " + buffer.remaining() + " bytes but only " + remainingInFile + " available");
    }

    // Heap buffers with a backing array can be filled in one call ----
    if (buffer.hasArray()) {
      int off = buffer.arrayOffset() + buffer.position();
      int len = buffer.remaining();
      indexInputDelegate.readBytes(buffer.array(), off, len);
      buffer.position(buffer.limit()); // advance fully
      return;
    }

    // Direct / non-array buffers: copy in reasonable chunks ----
    while (buffer.hasRemaining()) {
      final int bytesToRead = Math.min(buffer.remaining(), Long.BYTES);
      indexInputDelegate.readBytes(this.internalBuffer, 0, bytesToRead);
      buffer.put(this.internalBuffer, 0, bytesToRead);
    }
  }

  @Override
  public void readFully(long[] vector) throws IOException {
    for (int i = 0; i < vector.length; i++) {
      vector[i] = readLong();
    }
  }

  @Override
  public void read(int[] ints, int offset, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      ints[offset + i] = readInt();
    }
  }

  @Override
  public void read(float[] floats, int offset, int count) throws IOException {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(Float.BYTES * count);
    indexInputDelegate.readBytes(byteBuffer.array(), offset, Float.BYTES * count);
    FloatBuffer buffer = byteBuffer.asFloatBuffer();
    buffer.get(floats, offset, count);
  }

  @Override
  public void close() throws IOException {
    // no need to really close the index input delegate since it is a clone
  }

  @Override
  public long length() throws IOException {
    return indexInputDelegate.length();
  }

  /**
   * Supplies readers which are actually slices of the original IndexInput. We will vend out slices
   * in order for us to easily find the footer of the jVector graph index. This is useful because
   * our logic that reads the graph that the footer is always at {@link IndexInput#length()} of the
   * slice. Which is how {@link
   * io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex#load(ReaderSupplier, long)} is working
   * behind the scenes. The header offset, on the other hand, is flexible because we can provide it
   * as a parameter to {@link
   * io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex#load(ReaderSupplier, long)}
   */
  public static class Supplier implements ReaderSupplier {
    private final IndexInput input;

    public Supplier(IndexInput input) {
      this.input = input;
    }

    @Override
    public synchronized RandomAccessReader get() throws IOException {
      return new JVectorRandomAccessReader(input.clone());
    }

    @Override
    public void close() throws IOException {
      // Cloned inputs do not need to be closed
    }
  }
}
