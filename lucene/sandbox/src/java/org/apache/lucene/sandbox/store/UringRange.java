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
package org.apache.lucene.sandbox.store;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

/**
 * The handle {@link org.apache.lucene.store.IndexInput#prefetchRange} returns: an input over one
 * buffer that an io_uring read is landing in, or has already landed in.
 *
 * <p>Reads block until the bytes have arrived, so a caller can obtain many of these before reading
 * any.
 *
 * <p>Must be read and closed by the thread that created it, and must be closed even if never read.
 * Not thread safe.
 *
 * <p>Accessors are unaligned because the buffer starts at an arbitrary file offset.
 */
final class UringRange extends IndexInput {

  /** {@link #result} before a completion has been consumed. Not a valid byte count or errno. */
  static final int PENDING = Integer.MIN_VALUE;

  private final IOUringEngine engine;
  private final IOUringEngine.Lease lease;
  private final int slot;

  final int fd;
  final long offset;
  final int length;

  /** The buffer this read lands in. Owned by the handle from construction. */
  final MemorySegment buffer;

  /** Set once an SQE has been built and handed to the kernel. */
  boolean submitted;

  /** Bytes read, or a negative errno, or {@link #PENDING}. */
  int result = PENDING;

  private long pos;
  private boolean closed;

  UringRange(
      String description,
      IOUringEngine engine,
      IOUringEngine.Lease lease,
      int slot,
      int fd,
      long offset,
      int length,
      MemorySegment buffer) {
    super(description);
    this.engine = engine;
    this.lease = lease;
    this.slot = slot;
    this.fd = fd;
    this.offset = offset;
    this.length = length;
    this.buffer = buffer;
  }

  /** Blocks until the bytes are available. A failed or short read throws. */
  private void ensureReady() throws IOException {
    if (closed) {
      throw new AlreadyClosedException("already closed: " + this);
    }
    if (result == length) {
      return;
    }
    if (result == PENDING) {
      engine.ensureReady(lease, this);
    }
    if (result < 0) {
      throw new IOException(
          "io_uring read failed with errno=" + (-result) + " at offset " + offset + ": " + this);
    }
    if (result < length) {
      throw new IOException(
          "io_uring short read at offset "
              + offset
              + ": got "
              + result
              + " of "
              + length
              + " bytes: "
              + this);
    }
  }

  private long checkedPos(int bytes) throws IOException {
    long p = pos;
    if (p + bytes > length) {
      throw new EOFException("read past end of " + this);
    }
    pos = p + bytes;
    return p;
  }

  @Override
  public byte readByte() throws IOException {
    ensureReady();
    return buffer.get(JAVA_BYTE, checkedPos(Byte.BYTES));
  }

  @Override
  public short readShort() throws IOException {
    ensureReady();
    return buffer.get(JAVA_SHORT_UNALIGNED, checkedPos(Short.BYTES));
  }

  @Override
  public int readInt() throws IOException {
    ensureReady();
    return buffer.get(JAVA_INT_UNALIGNED, checkedPos(Integer.BYTES));
  }

  @Override
  public long readLong() throws IOException {
    ensureReady();
    return buffer.get(JAVA_LONG_UNALIGNED, checkedPos(Long.BYTES));
  }

  @Override
  public void readBytes(byte[] b, int off, int len) throws IOException {
    ensureReady();
    MemorySegment.copy(buffer, JAVA_BYTE, checkedPos(len), b, off, len);
  }

  @Override
  public void readFloats(float[] dst, int off, int len) throws IOException {
    ensureReady();
    MemorySegment.copy(buffer, JAVA_FLOAT_UNALIGNED, checkedPos(len * Float.BYTES), dst, off, len);
  }

  @Override
  public void readLongs(long[] dst, int off, int len) throws IOException {
    ensureReady();
    MemorySegment.copy(buffer, JAVA_LONG_UNALIGNED, checkedPos(len * Long.BYTES), dst, off, len);
  }

  @Override
  public void readInts(int[] dst, int off, int len) throws IOException {
    ensureReady();
    MemorySegment.copy(buffer, JAVA_INT_UNALIGNED, checkedPos(len * Integer.BYTES), dst, off, len);
  }

  @Override
  public long getFilePointer() {
    return pos;
  }

  @Override
  public void seek(long p) throws IOException {
    if (p < 0 || p > length) {
      throw new EOFException("seek to " + p + " outside " + this);
    }
    pos = p;
  }

  @Override
  public long length() {
    return length;
  }

  /**
   * A view of part of this range, awaiting the read first. The result shares this range's buffer
   * but is not part of the slot or lease lifecycle: closing it does nothing, and closing this range
   * does not invalidate it.
   */
  @Override
  public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength)
      throws IOException {
    if (sliceOffset < 0 || sliceLength < 0 || sliceLength > length - sliceOffset) {
      throw new EOFException(
          "slice("
              + sliceOffset
              + ","
              + sliceLength
              + ") is outside "
              + this
              + " of length "
              + length);
    }
    ensureReady();
    return detached(
        getFullSliceDescription(sliceDescription),
        buffer.asSlice(sliceOffset, sliceLength),
        (int) sliceLength);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Awaits the read before returning, because only the thread holding the ring lease can
   * complete a pending range. The clone is detached, as for {@link #slice}.
   */
  @Override
  public IndexInput clone() {
    try {
      ensureReady();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    UringRange c = detached(toString(), buffer, length);
    c.pos = pos;
    return c;
  }

  /** A range over bytes that have already arrived, owning no slot and no lease. */
  private static UringRange detached(String description, MemorySegment segment, int length) {
    UringRange r = new UringRange(description, null, null, -1, -1, 0L, length, segment);
    r.submitted = true;
    r.result = length;
    return r;
  }

  /**
   * Releases the slot if the read never went out, and the lease once the thread holds no more
   * handles. Does not block: a handle closed with its read in flight leaves the ring dirty, and the
   * next thread to want a ring cleans it.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (engine == null) {
      // A clone or slice does not own the slot or lease.
      return;
    }
    engine.handleClosed(lease, this, slot);
  }
}
