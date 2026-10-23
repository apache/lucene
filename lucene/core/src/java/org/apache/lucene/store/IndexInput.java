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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.codecs.CompoundFormat;

/**
 * Abstract base class for input from a file in a {@link Directory}. A random-access input stream.
 * Used for all Lucene index input operations.
 *
 * <p>{@code IndexInput} may only be used from one thread, because it is not thread safe (it keeps
 * internal state like file position). To allow multithreaded use, every {@code IndexInput} instance
 * must be cloned before it is used in another thread. Subclasses must therefore implement {@link
 * #clone()}, returning a new {@code IndexInput} which operates on the same underlying resource, but
 * positioned independently.
 *
 * <p><b>Warning:</b> Lucene never closes cloned {@code IndexInput}s, it will only call {@link
 * #close()} on the original object.
 *
 * <p>If you access the cloned IndexInput after closing the original object, any <code>readXXX
 * </code> methods will throw {@link AlreadyClosedException}.
 *
 * @see Directory
 */
public abstract class IndexInput extends DataInput implements Closeable {

  private final String resourceDescription;

  /**
   * resourceDescription should be a non-null, opaque string describing this resource; it's returned
   * from {@link #toString}.
   */
  protected IndexInput(String resourceDescription) {
    if (resourceDescription == null) {
      throw new IllegalArgumentException("resourceDescription must not be null");
    }
    this.resourceDescription = resourceDescription;
  }

  /** Closes the stream to further operations. */
  @Override
  public abstract void close() throws IOException;

  /**
   * Returns the current position in this file, where the next read will occur.
   *
   * @see #seek(long)
   */
  public abstract long getFilePointer();

  /**
   * Sets current position in this file, where the next read will occur. If this is beyond the end
   * of the file then this will throw {@code EOFException} and then the stream is in an undetermined
   * state.
   *
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * {@inheritDoc}
   *
   * <p>Behavior is functionally equivalent to seeking to <code>getFilePointer() + numBytes</code>.
   *
   * @see #getFilePointer()
   * @see #seek(long)
   */
  @Override
  public void skipBytes(long numBytes) throws IOException {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    final long skipTo = getFilePointer() + numBytes;
    seek(skipTo);
  }

  /** The number of bytes in the file. */
  public abstract long length();

  @Override
  public String toString() {
    return resourceDescription;
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Warning:</b> Lucene never closes cloned {@code IndexInput}s, it will only call {@link
   * #close()} on the original object.
   *
   * <p>If you access the cloned IndexInput after closing the original object, any <code>readXXX
   * </code> methods will throw {@link AlreadyClosedException}.
   *
   * <p>This method is NOT thread safe, so if the current {@code IndexInput} is being used by one
   * thread while {@code clone} is called by another, disaster could strike.
   */
  @Override
  public IndexInput clone() {
    return (IndexInput) super.clone();
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is sought to the beginning.
   */
  public abstract IndexInput slice(String sliceDescription, long offset, long length)
      throws IOException;

  /**
   * Create a slice with a new {@link IOContext}. This is typically used by {@link CompoundFormat}
   * implementations to modify the {@link IOContext} for specific files within the compound file.
   *
   * <p><b>NOTE</b>: only certain {@link IOContext} may be usable here, depending on how this
   * instance was opened.
   *
   * <p>The default implementation delegates to {@link #slice(String, long, long)} and ignores the
   * {@link IOContext}.
   */
  public IndexInput slice(String sliceDescription, long offset, long length, IOContext context)
      throws IOException {
    return slice(sliceDescription, offset, length);
  }

  /**
   * Subclasses call this to get the String for resourceDescription of a slice of this {@code
   * IndexInput}.
   */
  protected String getFullSliceDescription(String sliceDescription) {
    if (sliceDescription == null) {
      // Clones pass null sliceDescription:
      return toString();
    } else {
      return toString() + " [slice=" + sliceDescription + "]";
    }
  }

  /**
   * Creates a random-access slice of this index input, with the given offset and length.
   *
   * <p>The default implementation calls {@link #slice}, and it doesn't support random access, it
   * implements absolute reads as seek+read.
   */
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    final IndexInput slice = slice("randomaccess", offset, length);
    if (slice instanceof RandomAccessInput rai) {
      // slice() already supports random access
      return rai;
    } else {
      // return default impl
      return new RandomAccessInput() {
        @Override
        public long length() {
          assert length == slice.length();
          return slice.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
          slice.seek(pos);
          return slice.readByte();
        }

        @Override
        public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
          slice.seek(pos);
          slice.readBytes(bytes, offset, length);
        }

        @Override
        public short readShort(long pos) throws IOException {
          slice.seek(pos);
          return slice.readShort();
        }

        @Override
        public int readInt(long pos) throws IOException {
          slice.seek(pos);
          return slice.readInt();
        }

        @Override
        public long readLong(long pos) throws IOException {
          slice.seek(pos);
          return slice.readLong();
        }

        @Override
        public boolean prefetch(long offset, long length) throws IOException {
          return slice.prefetch(offset, length);
        }

        @Override
        public String toString() {
          return "RandomAccessInput(" + IndexInput.this.toString() + ")";
        }
      };
    }
  }

  /**
   * Optional method: Give a hint to this input that some bytes will be read in the near future.
   * IndexInput implementations may take advantage of this hint to start fetching pages of data
   * immediately from storage.
   *
   * <p>The default implementation is a no-op.
   *
   * @param offset start offset
   * @param length the number of bytes to prefetch
   * @return true if prefetch actually prefetched something, hence user can benefit from deferring
   *     reading the prefetched memory block.
   */
  public boolean prefetch(long offset, long length) throws IOException {
    return false;
  }

  /**
   * Optional method: prefetch {@code [offset, offset+length)} and, if this implementation can serve
   * those bytes from a buffer of its own, return an {@link IndexInput} over exactly them.
   *
   * <p>Where {@link #prefetch} only hints, this returns the input through which the prefetched
   * bytes are to be read. An implementation backed by asynchronous I/O may return before the read
   * has completed, in which case the first read on the returned input blocks until the bytes are
   * available. Callers can therefore request many ranges before reading any of them, which lets an
   * implementation issue them as a batch.
   *
   * <p>A non-null returned input is positioned at its beginning and has a {@link #length()} of
   * {@code length}. It must be read and closed by the thread that called this method, and closed
   * even if it is never read. Closing may block until an outstanding read into its buffer has
   * completed.
   *
   * <p>A {@code null} return means this implementation has no buffer of its own to hand out, and
   * the caller should read the bytes as it normally would. It is a property of the implementation,
   * not of the call: an implementation that returns an input must do so on every call or throw. In
   * particular it must not return {@code null} when it runs out of buffers, descriptors or queue
   * capacity, because the caller cannot tell that apart from "unsupported". Such conditions throw
   * {@link IOException}.
   *
   * <p>The default implementation checks the range and returns {@code null}. It does not call
   * {@link #prefetch}; a caller that gets {@code null} is expected to call {@link #prefetch} for
   * the range it wants, which may be wider than this one. An implementation that only advises the
   * operating system, such as a memory-mapped one, should keep the default.
   *
   * @param rangeDescription description of the returned input, as for {@link #slice}
   * @param offset start offset
   * @param length the number of bytes to prefetch
   * @return an input over the prefetched bytes, or {@code null} if this implementation has no
   *     buffer to hand out
   * @throws EOFException if {@code [offset, offset+length)} is not wholly within this input
   * @throws IOException if the prefetch cannot be started, including when the implementation is out
   *     of the resources it needs to buffer the range
   */
  public IndexInput prefetchRange(String rangeDescription, long offset, long length)
      throws IOException {
    checkRange(offset, length);
    return null;
  }

  /**
   * Verifies that {@code [offset, offset+length)} lies within this input, for {@link
   * #prefetchRange} implementations. A zero-length range at the end is permitted, as for {@link
   * #slice}.
   */
  protected final void checkRange(long offset, long length) throws IOException {
    final long fileLength = length();
    // Compared against the bytes remaining, since offset + length could overflow.
    if (offset < 0 || length < 0 || length > fileLength - offset) {
      throw new EOFException(
          "prefetch range out of bounds: offset="
              + offset
              + ",length="
              + length
              + ",fileLength="
              + fileLength
              + ": "
              + this);
    }
  }

  /**
   * Optional method: Updates the {@code IOContext} to specify a new read access pattern. IndexInput
   * implementations may take advantage of this hint to optimize reads from storage.
   *
   * <p>The default implementation is a no-op.
   */
  public void updateIOContext(IOContext context) throws IOException {}

  /**
   * Returns a hint whether all the contents of this input are resident in physical memory. It's a
   * hint because the operating system may have paged out some of the data by the time this method
   * returns. If the optional is true, then it's likely that the contents of this input are resident
   * in physical memory. A value of false does not imply that the contents are not resident in
   * physical memory. An empty optional is returned if it is not possible to determine.
   *
   * <p>This runs in linear time with the {@link #length()} of this input / page size.
   *
   * <p>The default implementation returns an empty optional.
   */
  public Optional<Boolean> isLoaded() {
    return Optional.empty();
  }
}
