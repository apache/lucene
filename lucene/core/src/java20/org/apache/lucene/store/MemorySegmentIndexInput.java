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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.GroupVIntUtil;

/**
 * Base IndexInput implementation that uses an array of MemorySegments to represent a file.
 *
 * <p>For efficiency, this class requires that the segment size are a power-of-two (<code>
 * chunkSizePower</code>).
 */
@SuppressWarnings("preview")
abstract class MemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
  static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  static final ValueLayout.OfShort LAYOUT_LE_SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  static final ValueLayout.OfInt LAYOUT_LE_INT =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  static final ValueLayout.OfLong LAYOUT_LE_LONG =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  static final ValueLayout.OfFloat LAYOUT_LE_FLOAT =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  final long length;
  final long chunkSizeMask;
  final int chunkSizePower;
  final Arena arena;
  final MemorySegment[] segments;

  int curSegmentIndex = -1;
  MemorySegment
      curSegment; // redundant for speed: segments[curSegmentIndex], also marker if closed!
  long curPosition; // relative to curSegment, not globally

  public static MemorySegmentIndexInput newInstance(
      String resourceDescription,
      Arena arena,
      MemorySegment[] segments,
      long length,
      int chunkSizePower) {
    assert Arrays.stream(segments).map(MemorySegment::scope).allMatch(arena.scope()::equals);
    if (segments.length == 1) {
      return new SingleSegmentImpl(resourceDescription, arena, segments[0], length, chunkSizePower);
    } else {
      return new MultiSegmentImpl(resourceDescription, arena, segments, 0, length, chunkSizePower);
    }
  }

  private MemorySegmentIndexInput(
      String resourceDescription,
      Arena arena,
      MemorySegment[] segments,
      long length,
      int chunkSizePower) {
    super(resourceDescription);
    this.arena = arena;
    this.segments = segments;
    this.length = length;
    this.chunkSizePower = chunkSizePower;
    this.chunkSizeMask = (1L << chunkSizePower) - 1L;
    this.curSegment = segments[0];
  }

  void ensureOpen() {
    if (curSegment == null) {
      throw alreadyClosed(null);
    }
  }

  // the unused parameter is just to silence javac about unused variables
  RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos)
      throws IOException {
    if (pos < 0L) {
      return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
    } else {
      throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
    }
  }

  AlreadyClosedException alreadyClosed(RuntimeException e) {
    // we use NPE to signal if this input is closed (to not have checks everywhere). If NPE happens,
    // we check the "is closed" condition explicitly by checking that our "curSegment" is null.
    // Care must be taken to not leak the NPE to code outside MemorySegmentIndexInput!
    if (this.curSegment == null) {
      return new AlreadyClosedException("Already closed: " + this);
    }
    // ISE can be thrown by MemorySegment and contains "closed" in message:
    if (e instanceof IllegalStateException
        && e.getMessage() != null
        && e.getMessage().contains("closed")) {
      return new AlreadyClosedException("Already closed: " + this, e);
    }
    // otherwise rethrow unmodified NPE/ISE (as it possibly a bug with passing a null parameter to
    // the IndexInput method):
    throw e;
  }

  @Override
  public final byte readByte() throws IOException {
    try {
      final byte v = curSegment.get(LAYOUT_BYTE, curPosition);
      curPosition++;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      do {
        curSegmentIndex++;
        if (curSegmentIndex >= segments.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curSegment = segments[curSegmentIndex];
        curPosition = 0L;
      } while (curSegment.byteSize() == 0L);
      final byte v = curSegment.get(LAYOUT_BYTE, curPosition);
      curPosition++;
      return v;
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    try {
      MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
      curPosition += len;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      readBytesBoundary(b, offset, len);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  private void readBytesBoundary(byte[] b, int offset, int len) throws IOException {
    try {
      long curAvail = curSegment.byteSize() - curPosition;
      while (len > curAvail) {
        MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, (int) curAvail);
        len -= curAvail;
        offset += curAvail;
        curSegmentIndex++;
        if (curSegmentIndex >= segments.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curSegment = segments[curSegmentIndex];
        curPosition = 0L;
        curAvail = curSegment.byteSize();
      }
      MemorySegment.copy(curSegment, LAYOUT_BYTE, curPosition, b, offset, len);
      curPosition += len;
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public void readInts(int[] dst, int offset, int length) throws IOException {
    try {
      MemorySegment.copy(curSegment, LAYOUT_LE_INT, curPosition, dst, offset, length);
      curPosition += Integer.BYTES * (long) length;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException iobe) {
      super.readInts(dst, offset, length);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    try {
      MemorySegment.copy(curSegment, LAYOUT_LE_LONG, curPosition, dst, offset, length);
      curPosition += Long.BYTES * (long) length;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException iobe) {
      super.readLongs(dst, offset, length);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public void readFloats(float[] dst, int offset, int length) throws IOException {
    try {
      MemorySegment.copy(curSegment, LAYOUT_LE_FLOAT, curPosition, dst, offset, length);
      curPosition += Float.BYTES * (long) length;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException iobe) {
      super.readFloats(dst, offset, length);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public final short readShort() throws IOException {
    try {
      final short v = curSegment.get(LAYOUT_LE_SHORT, curPosition);
      curPosition += Short.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public final int readInt() throws IOException {
    try {
      final int v = curSegment.get(LAYOUT_LE_INT, curPosition);
      curPosition += Integer.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public final int readVInt() throws IOException {
    // this can make JVM less confused (see LUCENE-10366)
    return super.readVInt();
  }

  @Override
  public final long readVLong() throws IOException {
    // this can make JVM less confused (see LUCENE-10366)
    return super.readVLong();
  }

  @Override
  public final long readLong() throws IOException {
    try {
      final long v = curSegment.get(LAYOUT_LE_LONG, curPosition);
      curPosition += Long.BYTES;
      return v;
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException e) {
      return super.readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return (((long) curSegmentIndex) << chunkSizePower) + curPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    // we use >> here to preserve negative, so we will catch AIOOBE,
    // in case pos + offset overflows.
    final int si = (int) (pos >> chunkSizePower);
    try {
      if (si != curSegmentIndex) {
        final MemorySegment seg = segments[si];
        // write values, on exception all is unchanged
        this.curSegmentIndex = si;
        this.curSegment = seg;
      }
      this.curPosition = Objects.checkIndex(pos & chunkSizeMask, curSegment.byteSize() + 1);
    } catch (IndexOutOfBoundsException e) {
      throw handlePositionalIOOBE(e, "seek", pos);
    }
  }

  @Override
  public byte readByte(long pos) throws IOException {
    try {
      final int si = (int) (pos >> chunkSizePower);
      return segments[si].get(LAYOUT_BYTE, pos & chunkSizeMask);
    } catch (IndexOutOfBoundsException ioobe) {
      throw handlePositionalIOOBE(ioobe, "read", pos);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  protected void readGroupVInt(long[] dst, int offset) throws IOException {
    try {
      final int len =
          GroupVIntUtil.readGroupVInt(
              this,
              curSegment.byteSize() - curPosition,
              p -> curSegment.get(LAYOUT_LE_INT, p),
              curPosition,
              dst,
              offset);
      curPosition += len;
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  // used only by random access methods to handle reads across boundaries
  private void setPos(long pos, int si) throws IOException {
    try {
      final MemorySegment seg = segments[si];
      // write values, on exception above all is unchanged
      this.curPosition = pos & chunkSizeMask;
      this.curSegmentIndex = si;
      this.curSegment = seg;
    } catch (IndexOutOfBoundsException ioobe) {
      throw handlePositionalIOOBE(ioobe, "read", pos);
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public short readShort(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return segments[si].get(LAYOUT_LE_SHORT, pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public int readInt(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return segments[si].get(LAYOUT_LE_INT, pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public long readLong(long pos) throws IOException {
    final int si = (int) (pos >> chunkSizePower);
    try {
      return segments[si].get(LAYOUT_LE_LONG, pos & chunkSizeMask);
    } catch (
        @SuppressWarnings("unused")
        IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, si);
      return readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw alreadyClosed(e);
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public final MemorySegmentIndexInput clone() {
    final MemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
    try {
      clone.seek(getFilePointer());
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }

    return clone;
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is seeked to the beginning.
   */
  @Override
  public final MemorySegmentIndexInput slice(String sliceDescription, long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + ",length="
              + length
              + ",fileLength="
              + this.length
              + ": "
              + this);
    }

    return buildSlice(sliceDescription, offset, length);
  }

  /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */
  MemorySegmentIndexInput buildSlice(String sliceDescription, long offset, long length) {
    ensureOpen();

    final long sliceEnd = offset + length;
    final int startIndex = (int) (offset >>> chunkSizePower);
    final int endIndex = (int) (sliceEnd >>> chunkSizePower);

    // we always allocate one more slice, the last one may be a 0 byte one after truncating with
    // asSlice():
    final MemorySegment slices[] = ArrayUtil.copyOfSubArray(segments, startIndex, endIndex + 1);

    // set the last segment's limit for the sliced view.
    slices[slices.length - 1] = slices[slices.length - 1].asSlice(0L, sliceEnd & chunkSizeMask);

    offset = offset & chunkSizeMask;

    final String newResourceDescription = getFullSliceDescription(sliceDescription);
    if (slices.length == 1) {
      return new SingleSegmentImpl(
          newResourceDescription,
          null, // clones don't have an Arena, as they can't close)
          slices[0].asSlice(offset, length),
          length,
          chunkSizePower);
    } else {
      return new MultiSegmentImpl(
          newResourceDescription,
          null, // clones don't have an Arena, as they can't close)
          slices,
          offset,
          length,
          chunkSizePower);
    }
  }

  @Override
  public final void close() throws IOException {
    if (curSegment == null) {
      return;
    }

    // the master IndexInput has an Arena and is able
    // to release all resources (unmap segments) - a
    // side effect is that other threads still using clones
    // will throw IllegalStateException
    if (arena != null) {
      while (arena.scope().isAlive()) {
        try {
          arena.close();
          break;
        } catch (
            @SuppressWarnings("unused")
            IllegalStateException e) {
          Thread.onSpinWait();
        }
      }
    }

    // make sure all accesses to this IndexInput instance throw NPE:
    curSegment = null;
    Arrays.fill(segments, null);
  }

  /** Optimization of MemorySegmentIndexInput for when there is only one segment. */
  static final class SingleSegmentImpl extends MemorySegmentIndexInput {

    SingleSegmentImpl(
        String resourceDescription,
        Arena arena,
        MemorySegment segment,
        long length,
        int chunkSizePower) {
      super(resourceDescription, arena, new MemorySegment[] {segment}, length, chunkSizePower);
      this.curSegmentIndex = 0;
    }

    @Override
    public void seek(long pos) throws IOException {
      ensureOpen();
      try {
        curPosition = Objects.checkIndex(pos, length + 1);
      } catch (IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE(e, "seek", pos);
      }
    }

    @Override
    public long getFilePointer() {
      ensureOpen();
      return curPosition;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      try {
        return curSegment.get(LAYOUT_BYTE, pos);
      } catch (IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE(e, "read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw alreadyClosed(e);
      }
    }

    @Override
    public short readShort(long pos) throws IOException {
      try {
        return curSegment.get(LAYOUT_LE_SHORT, pos);
      } catch (IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE(e, "read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw alreadyClosed(e);
      }
    }

    @Override
    public int readInt(long pos) throws IOException {
      try {
        return curSegment.get(LAYOUT_LE_INT, pos);
      } catch (IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE(e, "read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw alreadyClosed(e);
      }
    }

    @Override
    public long readLong(long pos) throws IOException {
      try {
        return curSegment.get(LAYOUT_LE_LONG, pos);
      } catch (IndexOutOfBoundsException e) {
        throw handlePositionalIOOBE(e, "read", pos);
      } catch (NullPointerException | IllegalStateException e) {
        throw alreadyClosed(e);
      }
    }
  }

  /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
  static final class MultiSegmentImpl extends MemorySegmentIndexInput {
    private final long offset;

    MultiSegmentImpl(
        String resourceDescription,
        Arena arena,
        MemorySegment[] segments,
        long offset,
        long length,
        int chunkSizePower) {
      super(resourceDescription, arena, segments, length, chunkSizePower);
      this.offset = offset;
      try {
        seek(0L);
      } catch (IOException ioe) {
        throw new AssertionError(ioe);
      }
      assert curSegment != null && curSegmentIndex >= 0;
    }

    @Override
    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos)
        throws IOException {
      return super.handlePositionalIOOBE(unused, action, pos - offset);
    }

    @Override
    public void seek(long pos) throws IOException {
      assert pos >= 0L : "negative position";
      super.seek(pos + offset);
    }

    @Override
    public long getFilePointer() {
      return super.getFilePointer() - offset;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return super.readByte(pos + offset);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return super.readShort(pos + offset);
    }

    @Override
    public int readInt(long pos) throws IOException {
      return super.readInt(pos + offset);
    }

    @Override
    public long readLong(long pos) throws IOException {
      return super.readLong(pos + offset);
    }

    @Override
    MemorySegmentIndexInput buildSlice(String sliceDescription, long ofs, long length) {
      return super.buildSlice(sliceDescription, this.offset + ofs, length);
    }
  }
}
