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
package org.apache.lucene.store;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

/** Base implementation class for buffered {@link IndexInput}. */
public abstract class BufferedIndexInput extends IndexInput implements RandomAccessInput {

  private static final ByteBuffer EMPTY_BYTEBUFFER =
      ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

  /** Default buffer size set to {@value #BUFFER_SIZE}. */
  public static final int BUFFER_SIZE = 1024;

  /** Minimum buffer size allowed */
  public static final int MIN_BUFFER_SIZE = 8;

  // The normal read buffer size defaults to 1024, but
  // increasing this during merging seems to yield
  // performance gains.  However we don't want to increase
  // it too much because there are quite a few
  // BufferedIndexInputs created during merging.  See
  // LUCENE-888 for details.
  /** A buffer size for merges set to {@value #MERGE_BUFFER_SIZE}. */
  public static final int MERGE_BUFFER_SIZE = 4096;

  private final int bufferSize;

  // Despite the two buffer references below, BufferedIndexInput only tracks a single buffer. Either
  // prefetch() has been called last and `buffer` is set to EMPTY_BYTEBUFFER while `prefetchBuffer`
  // tracks the actual buffer, or prefetchBuffer is set to EMPTY_BYTEBUFFER and `buffer` tracks the
  // actual buffer. This approach helps only check if `buffer.hasRemaining()` to know whether to
  // trigger a refill(), and refill() will check if there is a pending prefetch() before actually
  // reading bytes.
  private ByteBuffer buffer = EMPTY_BYTEBUFFER; // initialized lazily
  private ByteBuffer prefetchBuffer = EMPTY_BYTEBUFFER;
  private FutureTask<Void> pendingPrefetch; // only non-null if there is a pending prefetch()

  private long bufferStart = 0; // position in file of buffer

  @Override
  public final byte readByte() throws IOException {
    if (buffer.hasRemaining() == false) {
      refill();
    }
    return buffer.get();
  }

  public BufferedIndexInput(String resourceDesc) {
    this(resourceDesc, BUFFER_SIZE);
  }

  public BufferedIndexInput(String resourceDesc, IOContext context) {
    this(resourceDesc, bufferSize(context));
  }

  /** Inits BufferedIndexInput with a specific bufferSize */
  public BufferedIndexInput(String resourceDesc, int bufferSize) {
    super(resourceDesc);
    checkBufferSize(bufferSize);
    this.bufferSize = bufferSize;
  }

  /** Returns buffer size */
  public final int getBufferSize() {
    return bufferSize;
  }

  private void checkBufferSize(int bufferSize) {
    if (bufferSize < MIN_BUFFER_SIZE)
      throw new IllegalArgumentException(
          "bufferSize must be at least MIN_BUFFER_SIZE (got " + bufferSize + ")");
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    readBytes(b, offset, len, true);
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    // We need to finish pending prefetch operations to use data from the prefetch() instead of
    // reading directly bytes into the user's buffer.
    // Other readXXX methods don't need to do this since they always call refill() when they don't
    // have enough data, which in-turn calls finishPendingPrefetch(). But readBytes() may read bytes
    // into the user's buffer without refilling the internal buffer.
    finishPendingPrefetch();

    int available = buffer.remaining();
    if (len <= available) {
      // the buffer contains enough data to satisfy this request
      if (len > 0) // to allow b to be null if len is 0...
      buffer.get(b, offset, len);
    } else {
      // the buffer does not have enough data. First serve all we've got.
      if (available > 0) {
        buffer.get(b, offset, available);
        offset += available;
        len -= available;
      }
      // and now, read the remaining 'len' bytes:
      if (useBuffer && len < bufferSize) {
        // If the amount left to read is small enough, and
        // we are allowed to use our buffer, do it in the usual
        // buffered way: fill the buffer and copy from it:
        refill();
        if (buffer.remaining() < len) {
          // Throw an exception when refill() could not read len bytes:
          buffer.get(b, offset, buffer.remaining());
          throw new EOFException("read past EOF: " + this);
        } else {
          buffer.get(b, offset, len);
        }
      } else {
        // The amount left to read is larger than the buffer
        // or we've been asked to not use our buffer -
        // there's no performance reason not to read it all
        // at once. Note that unlike the previous code of
        // this function, there is no need to do a seek
        // here, because there's no need to reread what we
        // had in the buffer.
        long after = bufferStart + buffer.position() + len;
        if (after > length()) throw new EOFException("read past EOF: " + this);
        readInternal(ByteBuffer.wrap(b, offset, len));
        bufferStart = after;
        buffer.limit(0); // trigger refill() on read
      }
    }
  }

  @Override
  public final short readShort() throws IOException {
    if (Short.BYTES <= buffer.remaining()) {
      return buffer.getShort();
    } else {
      return super.readShort();
    }
  }

  @Override
  public final int readInt() throws IOException {
    if (Integer.BYTES <= buffer.remaining()) {
      return buffer.getInt();
    } else {
      return super.readInt();
    }
  }

  @Override
  protected void readGroupVInt(long[] dst, int offset) throws IOException {
    final int len =
        GroupVIntUtil.readGroupVInt(
            this, buffer.remaining(), p -> buffer.getInt((int) p), buffer.position(), dst, offset);
    if (len > 0) {
      buffer.position(buffer.position() + len);
    }
  }

  @Override
  public final long readLong() throws IOException {
    if (Long.BYTES <= buffer.remaining()) {
      return buffer.getLong();
    } else {
      return super.readLong();
    }
  }

  @Override
  public void readFloats(float[] dst, int offset, int len) throws IOException {
    int remainingDst = len;
    while (remainingDst > 0) {
      int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
      buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
      buffer.position(buffer.position() + Float.BYTES * cnt);
      remainingDst -= cnt;
      if (remainingDst > 0) {
        if (buffer.hasRemaining()) {
          dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
          --remainingDst;
        } else {
          refill();
        }
      }
    }
  }

  @Override
  public void readLongs(long[] dst, int offset, int len) throws IOException {
    int remainingDst = len;
    while (remainingDst > 0) {
      int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
      buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
      buffer.position(buffer.position() + Long.BYTES * cnt);
      remainingDst -= cnt;
      if (remainingDst > 0) {
        if (buffer.hasRemaining()) {
          dst[offset + len - remainingDst] = readLong();
          --remainingDst;
        } else {
          refill();
        }
      }
    }
  }

  @Override
  public void readInts(int[] dst, int offset, int len) throws IOException {
    int remainingDst = len;
    while (remainingDst > 0) {
      int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
      buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
      buffer.position(buffer.position() + Integer.BYTES * cnt);
      remainingDst -= cnt;
      if (remainingDst > 0) {
        if (buffer.hasRemaining()) {
          dst[offset + len - remainingDst] = readInt();
          --remainingDst;
        } else {
          refill();
        }
      }
    }
  }

  // Computes an offset into the current buffer from an absolute position to read
  // `width` bytes from.  If the buffer does not contain the position, then we
  // readjust the bufferStart and refill.
  private long resolvePositionInBuffer(long pos, int width) throws IOException {
    long index = pos - bufferStart;
    if (index >= 0 && index <= buffer.limit() - width) {
      return index;
    }
    if (index < 0) {
      // if we're moving backwards, then try and fill up the previous page rather than
      // starting again at the current pos, to avoid successive backwards reads reloading
      // the same data over and over again.  We also check that we can read `width`
      // bytes without going over the end of the buffer
      bufferStart = Math.max(bufferStart - bufferSize, pos + width - bufferSize);
      bufferStart = Math.max(bufferStart, 0);
      bufferStart = Math.min(bufferStart, pos);
    } else {
      // we're moving forwards, reset the buffer to start at pos
      bufferStart = pos;
    }
    buffer.limit(0); // trigger refill() on read
    seekInternal(bufferStart);
    refill();
    return pos - bufferStart;
  }

  @Override
  public final byte readByte(long pos) throws IOException {
    long index = resolvePositionInBuffer(pos, Byte.BYTES);
    return buffer.get((int) index);
  }

  @Override
  public void readBytes(long pos, byte[] bytes, int offset, int len) throws IOException {
    if (len <= bufferSize) {
      // the buffer is big enough to satisfy this request
      if (len > 0) { // to allow b to be null if len is 0...
        long index = resolvePositionInBuffer(pos, len);
        buffer.get((int) index, bytes, offset, len);
      }
    } else {
      while (len > bufferSize) {
        long index = resolvePositionInBuffer(pos, bufferSize);
        buffer.get((int) index, bytes, offset, bufferSize);
        len -= bufferSize;
        offset += bufferSize;
        pos += bufferSize;
      }
      long index = resolvePositionInBuffer(pos, len);
      buffer.get((int) index, bytes, offset, len);
    }
  }

  @Override
  public final short readShort(long pos) throws IOException {
    long index = resolvePositionInBuffer(pos, Short.BYTES);
    return buffer.getShort((int) index);
  }

  @Override
  public final int readInt(long pos) throws IOException {
    long index = resolvePositionInBuffer(pos, Integer.BYTES);
    return buffer.getInt((int) index);
  }

  @Override
  public final long readLong(long pos) throws IOException {
    long index = resolvePositionInBuffer(pos, Long.BYTES);
    return buffer.getLong((int) index);
  }

  private void maybeInitBuffer() throws IOException {
    assert pendingPrefetch == null;
    assert prefetchBuffer == EMPTY_BYTEBUFFER;

    if (buffer == EMPTY_BYTEBUFFER) {
      buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN).limit(0);
      seekInternal(bufferStart);
    }
  }

  private void refill() throws IOException {
    assert buffer.hasRemaining() == false;

    // Wait for pending prefetching to finish.
    if (finishPendingPrefetch()) {
      return;
    }

    long start = bufferStart + buffer.position();
    long end = start + bufferSize;
    if (end > length()) // don't read past EOF
    end = length();
    int newLength = (int) (end - start);
    if (newLength <= 0) throw new EOFException("read past EOF: " + this);

    // allocate buffer lazily
    maybeInitBuffer();
    buffer.position(0);
    buffer.limit(newLength);
    bufferStart = start;
    readInternal(buffer);
    // Make sure sub classes don't mess up with the buffer.
    assert buffer.order() == ByteOrder.LITTLE_ENDIAN : buffer.order();
    assert buffer.remaining() == 0 : "should have thrown EOFException";
    assert buffer.position() == newLength;
    buffer.flip();
  }

  private boolean finishPendingPrefetch() throws IOException {
    if (pendingPrefetch != null) {
      try {
        pendingPrefetch.get();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        throw IOUtils.rethrowAlways(e.getCause());
      } finally {
        // Always clear pendingPrefetch and swap buffers, regardless of success/failure so that
        // future read() operations work on the correct buffer.
        pendingPrefetch = null;
        prefetchBuffer.flip();
        buffer = prefetchBuffer;
        prefetchBuffer = EMPTY_BYTEBUFFER;
      }

      return buffer.hasRemaining();
    }
    return false;
  }

  @Override
  public void prefetch() throws IOException {
    final long pos = getFilePointer();
    final long length = length();
    if (pos >= length) {
      throw new EOFException("read past EOF: " + this);
    }

    // Make sure to never have two concurrent prefetch() calls trying to push bytes to the same
    // buffer.
    if (pendingPrefetch != null) {
      // prefetch() got called twice without reading bytes in-between?
      // nocommit should we fail instead?
      return;
    }

    if (buffer.hasRemaining()) {
      // the seek() that preceded prefetch() moved within the buffer, so we still have valid bytes
      // TODO: should we still prefetch more bytes in this case if there are very few bytes left?
      return;
    } else {
      // The buffer may not have been initialized yet, e.g. if prefetch() was called immediately
      // after calling clone() then seek().
      maybeInitBuffer();
    }

    assert buffer.capacity() > 0;
    assert prefetchBuffer == EMPTY_BYTEBUFFER;

    bufferStart = pos;
    final ByteBuffer prefetchBuffer = buffer;
    prefetchBuffer.position(0);
    final int limit = (int) Math.min(length - bufferStart, prefetchBuffer.capacity());
    assert limit > 0;
    prefetchBuffer.limit(limit);

    FutureTask<Void> pendingPrefetch =
        new FutureTask<>(
            () -> {
              readInternal(prefetchBuffer);
              return null;
            });
    Thread.startVirtualThread(pendingPrefetch::run);

    // We could schedule a background read successfully, now update state
    this.pendingPrefetch = pendingPrefetch;
    this.prefetchBuffer = prefetchBuffer;
    this.buffer = EMPTY_BYTEBUFFER; // trigger refill on next read()
  }

  /**
   * Expert: implements buffer refill. Reads bytes from the current position in the input.
   *
   * @param b the buffer to read bytes into
   */
  protected abstract void readInternal(ByteBuffer b) throws IOException;

  @Override
  public final long getFilePointer() {
    return bufferStart + buffer.position();
  }

  @Override
  public final void seek(long pos) throws IOException {
    // If there is a pending prefetch(), wait for it to finish before moving the file pointer.
    finishPendingPrefetch();
    assert prefetchBuffer == EMPTY_BYTEBUFFER;

    if (pos >= bufferStart && pos < (bufferStart + buffer.limit()))
      buffer.position((int) (pos - bufferStart)); // seek within buffer
    else {
      bufferStart = pos;
      buffer.limit(0); // trigger refill() on read
      prefetchBuffer.limit(0);
      seekInternal(pos);
    }
  }

  /**
   * Expert: implements seek. Sets current position in this file, where the next {@link
   * #readInternal(ByteBuffer)} will occur.
   *
   * @see #readInternal(ByteBuffer)
   */
  protected abstract void seekInternal(long pos) throws IOException;

  @Override
  public BufferedIndexInput clone() {
    BufferedIndexInput clone = (BufferedIndexInput) super.clone();

    clone.buffer = EMPTY_BYTEBUFFER;
    clone.prefetchBuffer = EMPTY_BYTEBUFFER;
    clone.pendingPrefetch = null;
    clone.bufferStart = getFilePointer();

    return clone;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return wrap(sliceDescription, this, offset, length);
  }

  /** Returns default buffer sizes for the given {@link IOContext} */
  public static int bufferSize(IOContext context) {
    switch (context.context()) {
      case MERGE:
        return MERGE_BUFFER_SIZE;
      case DEFAULT:
      case FLUSH:
      default:
        return BUFFER_SIZE;
    }
  }

  /**
   * Wraps a portion of another IndexInput with buffering.
   *
   * <p><b>Please note:</b> This is in most cases ineffective, because it may double buffer!
   */
  public static BufferedIndexInput wrap(
      String sliceDescription, IndexInput other, long offset, long length) {
    return new SlicedIndexInput(sliceDescription, other, offset, length);
  }

  /** Implementation of an IndexInput that reads from a portion of a file. */
  private static final class SlicedIndexInput extends BufferedIndexInput {
    IndexInput base;
    long fileOffset;
    long length;

    SlicedIndexInput(String sliceDescription, IndexInput base, long offset, long length) {
      super(
          (sliceDescription == null)
              ? base.toString()
              : (base.toString() + " [slice=" + sliceDescription + "]"),
          BufferedIndexInput.BUFFER_SIZE);
      if (offset < 0 || length < 0 || offset + length > base.length()) {
        throw new IllegalArgumentException(
            "slice() " + sliceDescription + " out of bounds: " + base);
      }
      this.base = base.clone();
      this.fileOffset = offset;
      this.length = length;
    }

    @Override
    public SlicedIndexInput clone() {
      SlicedIndexInput clone = (SlicedIndexInput) super.clone();
      clone.base = base.clone();
      clone.fileOffset = fileOffset;
      clone.length = length;
      return clone;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
      long start = getFilePointer();
      if (start + b.remaining() > length) {
        throw new EOFException("read past EOF: " + this);
      }
      base.seek(fileOffset + start);
      base.readBytes(b.array(), b.position(), b.remaining());
      b.position(b.position() + b.remaining());
    }

    @Override
    protected void seekInternal(long pos) {}

    @Override
    public void close() throws IOException {
      base.close();
    }

    @Override
    public long length() {
      return length;
    }
  }
}
